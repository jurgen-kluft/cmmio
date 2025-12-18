#include "ccore/c_allocator.h"

#include "cmmio/c_mmio.h"
#include "cmmio/c_mmmq.h"

#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>

namespace ncore
{
    namespace nmmmq
    {

// ====== Public constants ======
#define MMQ_ALIGN        8u
#define MMQ_FLAG_PENDING (1u << 0)
#define MMQ_FLAG_READY   (1u << 1)
#define MMQ_FLAG_ABORTED (1u << 2)

#define MMQ_MAGIC_INDEX   0x1CEB00FDEADBEEFULL
#define MMQ_MAGIC_DATA    0xDA7A5E90D0D0F0DULL
#define MMQ_MAGIC_CONTROL 0xC017301D00DFACEULL

        // ====== Queue layouts (8-byte aligned) ======

        // index.mm (read-only for consumers)
        struct index_entry_t
        {
            u64 seq;       // sequence number
            u32 off8;      // offset >> 3 (max 32 GiB per segment)
            u32 len;       // length in bytes of data
            u32 flags;     // bit0=PENDING, bit1=READY, bit2=ABORTED (optional)
            u32 reserved;  // padding to 8-byte alignment
        };

        struct index_header_t
        {
            u64 magic;        // MMQ_MAGIC_INDEX
            u32 version;      // 1
            u32 align;        // 8
            u64 next_seq;     // producer-only
            u64 entry_count;  // mirror of next_seq (optional)
            // followed by index_entry_t entries[] (append-only)
        };

        // data.mm (read-only for consumers)
        struct data_header_t
        {
            u64 magic;      // MMQ_MAGIC_DATA
            u32 version;    // 1
            u32 align;      // 8
            u32 reserved;   // padding to 8-byte alignment
            u64 write_pos;  // producer-only, bytes
            u64 file_size;  // mapped payload bytes
            // followed by u8 payload[file_size]
        };

        // control.mm (shared read–write for producer & consumers)
        struct consumer_slot_t
        {
            u64  last_seq;        // consumer progress
            u64  last_update_ns;  // optional heartbeat
            u32  active;          // 1=in use
            char name[64];        // consumer id
        };

        // control.mm header
        struct control_header_t
        {
            u64  magic;                  // MMQ_MAGIC_CONTROL
            u32  version;                // 1
            u32  align;                  // 8
            u32  max_consumers;          // maximum number of consumer slots
            char new_entries_sem[64];    // e.g. "/X_new"
            char registry_lock_sem[64];  // binary semaphore name (acts as mutex)
            u64  notify_seq;             // incremented per publish
            // followed by consumer_slot_t[max_consumers]
        };

        // ====== Producer and Consumer ======
        struct producer_t
        {
            // mapped bases
            void* index_base;
            void* data_base;
            void* control_base;

            // typed views
            index_header_t*   ih;
            data_header_t*    dh;
            control_header_t* ch;
        };

        struct consumer_t
        {
            // mapped bases
            void const* index_base;
            void const* data_base;
            void*       control_base;

            // typed views
            const index_header_t* ih;
            const data_header_t*  dh;
            control_header_t*     ch;
        };

        // ====== Handle structure ======
        struct handle_t
        {
            alloc_t* m_allocator;

            // memory mapped file objects
            nmmio::mappedfile_t* m_index;
            nmmio::mappedfile_t* m_data;
            nmmio::mappedfile_t* m_control;

            int_t index_size;
            int_t data_size;
            int_t control_size;

            bool is_producer;

            union
            {
                producer_t producer;
                consumer_t consumer;
            };

            // semaphores (opaque pointers)
            void* new_sem;
            void* reg_sem;
        };

        // ====== Local helpers ======

        // Named semaphore helpers (macOS semantics: leading '/', O_CREAT|O_EXCL atomic)
        // docs:  [1](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/sem_open.2.html)
        static sem_t* sem_create_exclusive(const char* name, i32 initial)
        {
            sem_t* s = sem_open(name, O_CREAT | O_EXCL, 0666, initial);
            if (s == SEM_FAILED)
            {
                if (errno == EEXIST)
                {
                    s = sem_open(name, 0);
                }
                else
                {
                    return NULL;
                }
            }
            return s;
        }
        static sem_t* sem_open_existing(const char* name)
        {
            sem_t* s = sem_open(name, 0);
            return (s == SEM_FAILED) ? NULL : s;
        }

        // Typed view helpers
        static inline index_entry_t*       get_producer_entries(index_header_t* ih) { return (index_entry_t*)((u8*)ih + sizeof(index_header_t)); }
        static inline const index_entry_t* get_consumer_entries(const index_header_t* ih) { return (const index_entry_t*)((u8*)ih + sizeof(index_header_t)); }
        static inline u8*                  get_producer_payload(data_header_t* dh) { return (u8*)dh + sizeof(data_header_t); }
        static inline const u8*            get_consumer_payload(const data_header_t* dh) { return (const u8*)dh + sizeof(data_header_t); }
        static inline consumer_slot_t*     get_slots(control_header_t* ch) { return (consumer_slot_t*)((u8*)ch + sizeof(control_header_t)); }

        // ====== Construct/Destruct handle ======
        handle_t* create_handle(alloc_t* allocator)
        {
            handle_t* h    = g_allocate_and_clear<handle_t>(allocator);
            h->m_allocator = allocator;
            nmmio::allocate(allocator, h->m_index);
            nmmio::allocate(allocator, h->m_data);
            nmmio::allocate(allocator, h->m_control);
            return h;
        }

        void destroy_handle(handle_t*& h)
        {
            if (h)
            {
                close_handle(h);
                alloc_t* allocator = h->m_allocator;
                allocator->deallocate(h);
                h = nullptr;
            }
        }

        // ====== Producer init ======
        i32 init_producer(handle_t* h, const config_t& config, const char* index_path, const char* data_path, const char* control_path, const char* new_sem_name, const char* reg_sem_name)
        {
            // first check if the files already exist, if they do not exist we create them.
            // if they do exist, we open them and assume they are valid.
            const bool index_exists = nmmio::exists(h->m_index, index_path);
            if (index_exists)
            {
                if (!nmmio::open_rw(h->m_index, index_path))
                    return -1;

                h->producer.index_base = nmmio::address_rw(h->m_index);
                h->index_size          = nmmio::size(h->m_index);
                h->producer.ih         = (index_header_t*)h->producer.index_base;
            }
            else
            {
                if (!nmmio::create_rw(h->m_index, index_path, config.index_initial_bytes))
                    return -1;

                h->producer.index_base = nmmio::address_rw(h->m_index);
                h->index_size          = nmmio::size(h->m_index);

                h->producer.ih = (index_header_t*)h->producer.index_base;
                memset(h->producer.ih, 0, sizeof(index_header_t));
                h->producer.ih->magic       = MMQ_MAGIC_INDEX;
                h->producer.ih->version     = 1;
                h->producer.ih->align       = MMQ_ALIGN;
                h->producer.ih->next_seq    = 0;
                h->producer.ih->entry_count = 0;
            }

            const bool data_exists = nmmio::exists(h->m_data, data_path);
            if (data_exists)
            {
                if (!nmmio::open_rw(h->m_data, data_path))
                    return -1;

                h->producer.data_base = nmmio::address_rw(h->m_data);
                h->data_size          = nmmio::size(h->m_data);
                h->producer.dh        = (data_header_t*)h->producer.data_base;
            }
            else
            {
                if (!nmmio::create_rw(h->m_data, data_path, config.data_initial_bytes))
                    return -1;

                h->producer.data_base = nmmio::address_rw(h->m_data);
                h->data_size          = nmmio::size(h->m_data);

                h->producer.dh = (data_header_t*)h->producer.data_base;
                memset(h->producer.dh, 0, sizeof(data_header_t));
                h->producer.dh->magic     = MMQ_MAGIC_DATA;
                h->producer.dh->version   = 1;
                h->producer.dh->align     = MMQ_ALIGN;
                h->producer.dh->write_pos = 0;
                h->producer.dh->file_size = h->data_size - sizeof(data_header_t);
            }

            const bool control_exists = nmmio::exists(h->m_control, control_path);
            if (control_exists)
            {
                if (!nmmio::open_rw(h->m_control, control_path))
                    return -1;
            }
            else
            {
                if (!nmmio::create_rw(h->m_control, control_path, config.control_bytes))
                    return -1;
            }

            h->is_producer           = true;
            h->producer.control_base = nmmio::address_rw(h->m_control);
            h->control_size          = nmmio::size(h->m_control);

            // initialize the full control.mm area
            h->producer.ch = (control_header_t*)h->producer.control_base;
            memset(h->producer.ch, 0, h->control_size);
            h->producer.ch->magic         = MMQ_MAGIC_CONTROL;
            h->producer.ch->version       = 1;
            h->producer.ch->align         = MMQ_ALIGN;
            h->producer.ch->max_consumers = config.max_consumers;
            h->producer.ch->notify_seq    = 0;

            // store semaphore names
            strncpy(h->producer.ch->new_entries_sem, new_sem_name, sizeof(h->producer.ch->new_entries_sem) - 1);
            strncpy(h->producer.ch->registry_lock_sem, reg_sem_name, sizeof(h->producer.ch->registry_lock_sem) - 1);

            // create/open semaphores
            h->new_sem = (void*)sem_create_exclusive(h->producer.ch->new_entries_sem, 0);    // counting semaphore
            h->reg_sem = (void*)sem_create_exclusive(h->producer.ch->registry_lock_sem, 1);  // acts as mutex

            if (!h->new_sem || !h->reg_sem)
                return -1;

            return 0;
        }

        // ====== Consumer attach ======
        i32 attach_consumer(handle_t* h, const char* index_path, const char* data_path, const char* control_path)
        {
            if (!nmmio::open_ro(h->m_index, index_path))
                return -1;
            if (!nmmio::open_ro(h->m_data, data_path))
                return -1;
            if (!nmmio::open_rw(h->m_control, control_path))
                return -1;

            h->consumer.index_base   = nmmio::address_ro(h->m_index);
            h->consumer.data_base    = nmmio::address_ro(h->m_data);
            h->consumer.control_base = nmmio::address_rw(h->m_control);

            h->index_size   = nmmio::size(h->m_index);
            h->data_size    = nmmio::size(h->m_data);
            h->control_size = nmmio::size(h->m_control);

            h->consumer.ih = (index_header_t*)h->consumer.index_base;
            h->consumer.dh = (data_header_t*)h->consumer.data_base;
            h->consumer.ch = (control_header_t*)h->consumer.control_base;

            // sanity
            if (h->consumer.ch->magic != MMQ_MAGIC_INDEX || h->consumer.ih->version != 1 || h->consumer.ih->align != MMQ_ALIGN)
                return -1;
            if (h->consumer.dh->magic != MMQ_MAGIC_DATA || h->consumer.dh->version != 1 || h->consumer.dh->align != MMQ_ALIGN)
                return -1;
            if (h->consumer.ch->magic != MMQ_MAGIC_CONTROL || h->consumer.ch->version != 1 || h->consumer.ch->align != MMQ_ALIGN)
                return -1;

            // open semaphores by names in control.mm
            h->new_sem = (void*)sem_open_existing(h->consumer.ch->new_entries_sem);
            h->reg_sem = (void*)sem_open_existing(h->consumer.ch->registry_lock_sem);
            if (!h->new_sem || !h->reg_sem)
                return -1;

            h->is_producer = false;

            return 0;
        }

        // ====== Consumer registration ======
        i32 register_consumer(handle_t* h, const char* name, u64 start_seq)
        {
            sem_t* lock = (sem_t*)h->reg_sem;
            if (sem_wait(lock) != 0)
                return -1;  // lock registry

            i32              slot = -1;
            u32              i, maxc = h->consumer.ch->max_consumers;
            consumer_slot_t* s = get_slots(h->consumer.ch);

            // reuse slot if same name
            for (i = 0; i < maxc; ++i)
            {
                if (s[i].active && strncmp(s[i].name, name, sizeof(s[i].name)) == 0)
                {
                    slot = (i32)i;
                    break;
                }
            }
            if (slot < 0)
            {
                for (i = 0; i < maxc; ++i)
                {
                    if (!s[i].active)
                    {
                        s[i].active   = 1;
                        s[i].last_seq = start_seq;
                        strncpy(s[i].name, name, sizeof(s[i].name) - 1);
                        slot = (i32)i;
                        break;
                    }
                }
            }

            sem_post(lock);  // unlock
            return slot;     // -1 if full
        }

        static inline u64 align_up_u64(u64 x, u64 a) { return (x + (a - 1)) & ~(a - 1); }

        // ====== Producer publish ======
        i32 publish(handle_t* h, const void* msg, u32 len)
        {
            producer_t* p = &h->producer;

            // Append data (grow if needed)
            u64 pos  = align_up_u64(p->dh->write_pos, MMQ_ALIGN);
            u64 span = align_up_u64((u64)len, MMQ_ALIGN);
            u64 end  = pos + span;

            if (end > p->dh->file_size)
            {
                // Grow 10% of current file size
                // e.g. 10MB -> 11MB, 100MB -> 110MB
                int_t new_size = (int_t)(h->data_size * 11 / 10);
                if (!nmmio::extend_size(h->m_data, new_size))
                    return -1;
                h->data_size     = nmmio::size(h->m_data);
                p->data_base     = nmmio::address_rw(h->m_data);
                p->dh            = (data_header_t*)p->data_base;
                p->dh->file_size = h->data_size - sizeof(data_header_t);
            }

            u8* payload = get_producer_payload(p->dh);
            memcpy(payload + pos, msg, len);
            if (span > len)
                memset(payload + pos + len, 0, (int_t)(span - len));
            p->dh->write_pos = end;

            // Ensure index has room
            u64   seq              = p->ih->next_seq;
            int_t need_index_bytes = sizeof(index_header_t) + (int_t)((seq + 1) * sizeof(index_entry_t));
            if (need_index_bytes > h->index_size)
            {
                // grow index in chunks (e.g., +64k entries)
                int_t grow_entries = 64 * 1024;
                int_t goal_entries = (int_t)(seq + grow_entries);
                int_t new_size     = sizeof(index_header_t) + goal_entries * sizeof(index_entry_t);

                if (!nmmio::extend_size(h->m_index, new_size))
                    return -1;

                p->ih = (index_header_t*)p->index_base;
            }

            // Index entry: PENDING -> READY (single producer, no lock needed)
            index_entry_t* e = &get_producer_entries(p->ih)[seq];
            e->seq           = seq;
            e->off8          = (u32)(pos >> 3);
            e->len           = len;
            e->flags         = MMQ_FLAG_PENDING;

            p->ih->next_seq    = seq + 1;
            p->ih->entry_count = p->ih->next_seq;

            e->flags = MMQ_FLAG_READY;

            // Notify consumers
            p->ch->notify_seq++;
            sem_t* ns = (sem_t*)h->new_sem;
            sem_post(ns);
            return 0;
        }

        // ====== Consumer drain ======
        bool consumer_drain(handle_t* h, i32 slot_index, u8 const* msg_data, u32& msg_len)
        {
            consumer_slot_t* self = &get_slots(h->consumer.ch)[slot_index];
            const u64        nseq = h->consumer.ih->next_seq;
            while (self->last_seq < nseq)
            {
                const index_entry_t* e = &get_consumer_entries(h->consumer.ih)[self->last_seq];
                if ((e->flags & MMQ_FLAG_READY) == 0)
                {  // skip non-ready
                    self->last_seq++;
                    continue;
                }
                self->last_seq++;

                const u64 off = ((u64)e->off8) << 3;
                msg_data      = get_consumer_payload(h->consumer.dh) + off;
                msg_len       = e->len;
                return true;
            }
            return false;
        }

        // ====== Waits ======
        i32 wait_for_new(handle_t* h)
        {
            sem_t* ns = (sem_t*)h->new_sem;
            return sem_wait(ns);
        }

        i32 wait_for_new_timeout(handle_t* h, u32 timeout_us)
        {
            // Emulate: loop trywait + nanosleep
            sem_t*    ns        = (sem_t*)h->new_sem;
            const u32 slice_us  = 500;
            u32       waited_us = 0;
            while (waited_us < timeout_us)
            {
                if (sem_trywait(ns) == 0)
                    return 0;
                struct timespec ts;
                ts.tv_sec  = 0;
                ts.tv_nsec = slice_us * 1000;
                nanosleep(&ts, NULL);
                waited_us += slice_us;
            }
            errno = ETIMEDOUT;
            return -1;
        }

        // ====== Teardown ======
        void close_handle(handle_t* h)
        {
            nmmio::close(h->m_index);
            nmmio::close(h->m_data);
            nmmio::close(h->m_control);

            nmmio::deallocate(h->m_allocator, h->m_index);
            nmmio::deallocate(h->m_allocator, h->m_data);
            nmmio::deallocate(h->m_allocator, h->m_control);

            h->index_size   = 0;
            h->data_size    = 0;
            h->control_size = 0;

            h->producer.index_base   = nullptr;
            h->producer.data_base    = nullptr;
            h->producer.control_base = nullptr;

            if (h->new_sem)
            {
                sem_close((sem_t*)h->new_sem);
                h->new_sem = NULL;
            }
            if (h->reg_sem)
            {
                sem_close((sem_t*)h->reg_sem);
                h->reg_sem = NULL;
            }

            // Optional: producer may sem_unlink(h->ch->new_entries_sem); sem_unlink(h->ch->registry_lock_sem);
        }

    }  // namespace nmmmq
}  // namespace ncore
