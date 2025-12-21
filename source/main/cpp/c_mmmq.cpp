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
#define MMQ_ALIGN 8u

#define MMQ_MAGIC_INDEX   0x1CEB00FDEADBEEFULL
#define MMQ_MAGIC_DATA    0xDA7A5E90D0D0F0DULL
#define MMQ_MAGIC_CONTROL 0xC017301D00DFACEULL

        // ====== Layouts ======
        typedef u64 seq_t;

        // index.mm (read-only for consumers)
        // entry = 24 bytes
        struct index_entry_t
        {
            seq_t m_seq;    // sequence number
            u64   m_off8;   // offset in data.mm (aligned to 8 bytes)
            u32   m_len;    // length in bytes of data
            u32   m_flags;  // bit0=PENDING, bit1=READY, bit2=ABORTED (optional)
        };

        // header = 32 bytes
        struct index_header_t
        {
            u64   m_magic;        // MMQ_MAGIC_INDEX
            u32   m_version;      // 1
            u32   m_align;        // 8
            seq_t m_next_seq;     // producer-only
            seq_t m_entry_count;  // mirror of next_seq (optional)
            // followed by index_entry_t entries[] (append-only)
        };

        // data.mm (read-only for consumers)
        // header = 32 bytes
        struct data_header_t
        {
            u64 m_magic;      // MMQ_MAGIC_DATA
            u32 m_version;    // 1
            u32 m_align;      // 8
            u64 m_write_pos;  // producer-only, bytes
            u64 m_file_size;  // mapped payload bytes
            // followed by u8 payload[file_size]
        };

        // control.mm (shared read–write for producer & consumers)
        // slot = 64 bytes
        struct consumer_slot_t
        {
            u64   m_last_update_ns;  // optional heartbeat
            seq_t m_last_seq;        // consumer progress
            u32   m_active;          // 1=in use
            char  m_name[64 - 20];   // consumer id
        };

        // control.mm header
        // header = 128 bytes
        struct control_header_t
        {
            u64   m_magic;                       // MMQ_MAGIC_CONTROL
            u16   m_version;                     // 1
            u16   m_align;                       // 8
            i16   m_max_consumers;               // maximum number of consumer slots
            u16   m_reserved0;                   // padding
            seq_t m_notify_seq;                  // incremented per publish
            char  m_new_entries_sem[64 - 12];    // e.g. "/X_new"
            char  m_registry_lock_sem[64 - 12];  // binary semaphore name (acts as mutex)
            // followed by consumer_slot_t[max_consumers]
        };

        // ====== Producer and Consumer ======

        // producer bookkeeping = 48 bytes
        struct producer_t
        {
            // mapped bases
            void* m_index_base;
            void* m_data_base;
            void* m_control_base;

            // typed views
            index_header_t*   m_ih;
            data_header_t*    m_dh;
            control_header_t* m_ch;
        };

        // consumer bookkeeping = 48 bytes
        struct consumer_t
        {
            // mapped bases
            void const* m_index_base;
            void const* m_data_base;
            void*       m_control_base;

            // typed views
            const index_header_t* m_ih;
            const data_header_t*  m_dh;
            control_header_t*     m_ch;
        };

        // ====== Handle structure ======
        // handle = 64 + 64 = 128 bytes
        struct handle_t
        {
            alloc_t* m_allocator;

            // memory mapped file objects
            nmmio::mappedfile_t* m_index;
            nmmio::mappedfile_t* m_data;
            nmmio::mappedfile_t* m_control;

            int_t m_index_size;
            int_t m_data_size;
            int_t m_control_size;

            int_t m_is_producer;

            // semaphores (opaque pointers)
            void* m_new_sem;
            void* m_reg_sem;

            union
            {
                producer_t m_producer;
                consumer_t m_consumer;
            };
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

        enum EErrorCodes
        {
            MMQ_ERR_OK                  = 0,
            MMQ_ERR_INDEX_OPEN_RW       = -1,
            MMQ_ERR_DATA_OPEN_RW        = -2,
            MMQ_ERR_CONTROL_OPEN_RW     = -3,
            MMQ_ERR_INDEX_SANITY        = -4,
            MMQ_ERR_DATA_SANITY         = -5,
            MMQ_ERR_CONTROL_SANITY      = -6,
            MMQ_ERR_SEMAPHORE_OPEN      = -7,
            MMQ_ERR_REGISTRY_LOCK       = -8,
            MMQ_ERR_CONSUMER_SLOTS_FULL = -9,
            MMQ_ERR_INDEX_EXTEND        = -10,
            MMQ_ERR_DATA_EXTEND         = -11,
            MMQ_ERR_NO_MSG_AVAILABLE    = -12,
            MMQ_ERR_TIMEDOUT            = -13,
        };

        // ====== Producer init ======
        i32 init_producer(handle_t* h, const config_t& config, const char* index_path, const char* data_path, const char* control_path, const char* new_sem_name, const char* reg_sem_name)
        {
            STATIC_ASSERTS(sizeof(control_header_t) == (64*2), "control_header_t size must be multiple of 64");

            // first check if the files already exist, if they do not exist we create them.
            // if they do exist, we open them and assume they are valid.
            const bool index_exists = nmmio::exists(h->m_index, index_path);
            if (index_exists)
            {
                if (!nmmio::open_rw(h->m_index, index_path))
                    return MMQ_ERR_INDEX_OPEN_RW;

                h->m_producer.m_index_base = nmmio::address_rw(h->m_index);
                h->m_index_size            = nmmio::size(h->m_index);
                h->m_producer.m_ih         = (index_header_t*)h->m_producer.m_index_base;
            }
            else
            {
                if (!nmmio::create_rw(h->m_index, index_path, config.index_initial_bytes))
                    return MMQ_ERR_INDEX_OPEN_RW;

                h->m_producer.m_index_base = nmmio::address_rw(h->m_index);
                h->m_index_size            = nmmio::size(h->m_index);

                h->m_producer.m_ih = (index_header_t*)h->m_producer.m_index_base;
                memset(h->m_producer.m_ih, 0, sizeof(index_header_t));
                h->m_producer.m_ih->m_magic       = MMQ_MAGIC_INDEX;
                h->m_producer.m_ih->m_version     = 1;
                h->m_producer.m_ih->m_align       = MMQ_ALIGN;
                h->m_producer.m_ih->m_next_seq    = 0;
                h->m_producer.m_ih->m_entry_count = 0;
            }

            const bool data_exists = nmmio::exists(h->m_data, data_path);
            if (data_exists)
            {
                if (!nmmio::open_rw(h->m_data, data_path))
                    return MMQ_ERR_DATA_OPEN_RW;

                h->m_producer.m_data_base = nmmio::address_rw(h->m_data);
                h->m_data_size            = nmmio::size(h->m_data);
                h->m_producer.m_dh        = (data_header_t*)h->m_producer.m_data_base;
            }
            else
            {
                if (!nmmio::create_rw(h->m_data, data_path, config.data_initial_bytes))
                    return MMQ_ERR_DATA_OPEN_RW;

                h->m_producer.m_data_base = nmmio::address_rw(h->m_data);
                h->m_data_size            = nmmio::size(h->m_data);

                h->m_producer.m_dh = (data_header_t*)h->m_producer.m_data_base;
                memset(h->m_producer.m_dh, 0, sizeof(data_header_t));
                h->m_producer.m_dh->m_magic     = MMQ_MAGIC_DATA;
                h->m_producer.m_dh->m_version   = 1;
                h->m_producer.m_dh->m_align     = MMQ_ALIGN;
                h->m_producer.m_dh->m_write_pos = 0;
                h->m_producer.m_dh->m_file_size = h->m_data_size - sizeof(data_header_t);
            }

            const bool control_exists = nmmio::exists(h->m_control, control_path);
            if (control_exists)
            {
                if (!nmmio::open_rw(h->m_control, control_path))
                    return MMQ_ERR_CONTROL_OPEN_RW;
            }
            else
            {
                int_t control_bytes = sizeof(control_header_t) + (sizeof(consumer_slot_t) * config.max_consumers);
                control_bytes       = (control_bytes + (1024 - 1)) & ~(1024 - 1);  // align up to 1 KiB

                if (!nmmio::create_rw(h->m_control, control_path, control_bytes))
                    return MMQ_ERR_CONTROL_OPEN_RW;
            }

            h->m_is_producer             = true;
            h->m_producer.m_control_base = nmmio::address_rw(h->m_control);
            h->m_control_size            = nmmio::size(h->m_control);

            // initialize the full control.mm area
            h->m_producer.m_ch = (control_header_t*)h->m_producer.m_control_base;
            memset(h->m_producer.m_ch, 0, h->m_control_size);
            h->m_producer.m_ch->m_magic         = MMQ_MAGIC_CONTROL;
            h->m_producer.m_ch->m_version       = 1;
            h->m_producer.m_ch->m_align         = MMQ_ALIGN;
            h->m_producer.m_ch->m_max_consumers = config.max_consumers;
            h->m_producer.m_ch->m_notify_seq    = 0;

            // store semaphore names
            strncpy(h->m_producer.m_ch->m_new_entries_sem, new_sem_name, sizeof(h->m_producer.m_ch->m_new_entries_sem) - 1);
            strncpy(h->m_producer.m_ch->m_registry_lock_sem, reg_sem_name, sizeof(h->m_producer.m_ch->m_registry_lock_sem) - 1);

            // create/open semaphores
            h->m_new_sem = (void*)sem_create_exclusive(h->m_producer.m_ch->m_new_entries_sem, 0);    // counting semaphore
            h->m_reg_sem = (void*)sem_create_exclusive(h->m_producer.m_ch->m_registry_lock_sem, 1);  // acts as mutex
            if (!h->m_new_sem || !h->m_reg_sem)
                return MMQ_ERR_SEMAPHORE_OPEN;

            return MMQ_ERR_OK;
        }

        // ====== Consumer attach ======
        i32 attach_consumer(handle_t* h, const char* index_path, const char* data_path, const char* control_path)
        {
            if (!nmmio::open_ro(h->m_index, index_path))
                return MMQ_ERR_INDEX_OPEN_RW;
            if (!nmmio::open_ro(h->m_data, data_path))
                return MMQ_ERR_DATA_OPEN_RW;
            if (!nmmio::open_rw(h->m_control, control_path))
                return MMQ_ERR_CONTROL_OPEN_RW;

            h->m_consumer.m_index_base   = nmmio::address_ro(h->m_index);
            h->m_consumer.m_data_base    = nmmio::address_ro(h->m_data);
            h->m_consumer.m_control_base = nmmio::address_rw(h->m_control);

            h->m_index_size   = nmmio::size(h->m_index);
            h->m_data_size    = nmmio::size(h->m_data);
            h->m_control_size = nmmio::size(h->m_control);

            h->m_consumer.m_ih = (index_header_t*)h->m_consumer.m_index_base;
            h->m_consumer.m_dh = (data_header_t*)h->m_consumer.m_data_base;
            h->m_consumer.m_ch = (control_header_t*)h->m_consumer.m_control_base;

            // sanity
            if (h->m_consumer.m_ih->m_magic != MMQ_MAGIC_INDEX || h->m_consumer.m_ih->m_version != 1 || h->m_consumer.m_ih->m_align != MMQ_ALIGN)
                return MMQ_ERR_INDEX_OPEN_RW;
            if (h->m_consumer.m_dh->m_magic != MMQ_MAGIC_DATA || h->m_consumer.m_dh->m_version != 1 || h->m_consumer.m_dh->m_align != MMQ_ALIGN)
                return MMQ_ERR_DATA_OPEN_RW;
            if (h->m_consumer.m_ch->m_magic != MMQ_MAGIC_CONTROL || h->m_consumer.m_ch->m_version != 1 || h->m_consumer.m_ch->m_align != MMQ_ALIGN)
                return MMQ_ERR_CONTROL_OPEN_RW;

            // open semaphores by names in control.mm
            h->m_new_sem = (void*)sem_open_existing(h->m_consumer.m_ch->m_new_entries_sem);
            h->m_reg_sem = (void*)sem_open_existing(h->m_consumer.m_ch->m_registry_lock_sem);
            if (!h->m_new_sem || !h->m_reg_sem)
                return MMQ_ERR_SEMAPHORE_OPEN;

            h->m_is_producer = false;
            return MMQ_ERR_OK;
        }

        // ====== Consumer registration ======
        i32 register_consumer(handle_t* h, const char* name, u32 start_seq, i32& slot)
        {
            sem_t* lock = (sem_t*)h->m_reg_sem;
            if (sem_wait(lock) != 0)
                return MMQ_ERR_REGISTRY_LOCK;

            i32              maxc = h->m_consumer.m_ch->m_max_consumers;
            consumer_slot_t* s    = get_slots(h->m_consumer.m_ch);

            // reuse slot if same name
            i32 inactive = maxc;
            slot         = -1;
            for (i32 i = 0; i < maxc; ++i)
            {
                if (s[i].m_active)
                {
                    if (strncmp(s[i].m_name, name, sizeof(s[i].m_name)) == 0)
                    {
                        slot = (i32)i;
                        break;
                    }
                }
                else if (i < inactive)
                {
                    inactive = i;
                }
            }
            if (slot < 0)
            {
                for (i32 i = 0; i < maxc; ++i)
                {
                    if (!s[i].m_active)
                    {
                        s[i].m_active   = 1;
                        s[i].m_last_seq = start_seq;
                        strncpy(s[i].m_name, name, sizeof(s[i].m_name) - 1);
                        slot = (i32)i;
                        break;
                    }
                }
            }

            sem_post(lock);  // unlock
            return (slot < 0) ? MMQ_ERR_CONSUMER_SLOTS_FULL : MMQ_ERR_OK;
        }

        static inline u64 align_up_u64(u64 x, u64 a) { return (x + (a - 1)) & ~(a - 1); }

        // ====== Producer publish ======
        i32 publish(handle_t* h, const void* msg, u32 len)
        {
            producer_t* p = &h->m_producer;

            // Append data (grow if needed)
            u64 pos  = align_up_u64(p->m_dh->m_write_pos, MMQ_ALIGN);
            u64 span = align_up_u64((u64)len, MMQ_ALIGN);
            u64 end  = pos + span;

            if (end > p->m_dh->m_file_size)
            {
                // Grow 10% of current file size
                // e.g. 10MB -> 11MB, 100MB -> 110MB
                int_t new_size = (int_t)(h->m_data_size * 11 / 10);
                if (!nmmio::extend_size(h->m_data, new_size))
                    return -1;
                h->m_data_size       = nmmio::size(h->m_data);
                p->m_data_base       = nmmio::address_rw(h->m_data);
                p->m_dh              = (data_header_t*)p->m_data_base;
                p->m_dh->m_file_size = h->m_data_size - sizeof(data_header_t);
            }

            u8* payload = get_producer_payload(p->m_dh);
            memcpy(payload + pos, msg, len);
            if (span > len)
                memset(payload + pos + len, 0, (int_t)(span - len));
            p->m_dh->m_write_pos = end;

            // Ensure index has room
            const seq_t seq              = p->m_ih->m_next_seq;
            const int_t need_index_bytes = sizeof(index_header_t) + ((int_t)(seq + 1) * sizeof(index_entry_t));
            if (need_index_bytes > h->m_index_size)
            {
                // grow index in chunks (e.g., +64k entries)
                int_t grow_entries = 64 * 1024;
                int_t goal_entries = (int_t)(seq + grow_entries);
                int_t new_size     = sizeof(index_header_t) + goal_entries * sizeof(index_entry_t);

                if (!nmmio::extend_size(h->m_index, new_size))
                    return MMQ_ERR_INDEX_EXTEND;

                p->m_ih = (index_header_t*)p->m_index_base;
            }

            // Index entry: PENDING -> READY (single producer, no lock needed)
            index_entry_t* e = &get_producer_entries(p->m_ih)[seq];
            e->m_seq         = seq;
            e->m_off8        = (u32)(pos >> 3);
            e->m_len         = len;

            p->m_ih->m_next_seq    = seq + 1;
            p->m_ih->m_entry_count = p->m_ih->m_next_seq;

            // Notify consumers
            p->m_ch->m_notify_seq++;
            sem_t* ns = (sem_t*)h->m_new_sem;
            sem_post(ns);

            return MMQ_ERR_OK;
        }

        // ====== Consumer drain ======
        bool consumer_drain(handle_t* h, i32 slot_index, u8 const*& msg_data, u32& msg_len)
        {
            consumer_slot_t* self = &get_slots(h->m_consumer.m_ch)[slot_index];
            const seq_t      nseq = h->m_consumer.m_ih->m_next_seq;
            if (self->m_last_seq < nseq)
            {
                const index_entry_t* e   = &get_consumer_entries(h->m_consumer.m_ih)[self->m_last_seq];
                const u64            off = ((u64)e->m_off8) << 3;
                msg_data                 = get_consumer_payload(h->m_consumer.m_dh) + off;
                msg_len                  = e->m_len;

                self->m_last_seq++;
                return true;
            }
            msg_data = nullptr;
            msg_len  = 0;
            return false;
        }

        // ====== Waits ======
        bool wait_for_new(handle_t* h)
        {
            sem_t*    ns     = (sem_t*)h->m_new_sem;
            const i32 result = sem_wait(ns);
            return (result == 0);
        }

        bool wait_for_new_timeout(handle_t* h, u32 timeout_us)
        {
            // Emulate: loop trywait + nanosleep
            sem_t*    ns        = (sem_t*)h->m_new_sem;
            const u32 slice_us  = 500;
            u32       waited_us = 0;
            while (waited_us < timeout_us)
            {
                if (sem_trywait(ns) == 0)
                    return true;
                struct timespec ts;
                ts.tv_sec  = 0;
                ts.tv_nsec = slice_us * 1000;
                nanosleep(&ts, NULL);
                waited_us += slice_us;
            }
            errno = ETIMEDOUT;
            return false;
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

            h->m_index_size   = 0;
            h->m_data_size    = 0;
            h->m_control_size = 0;

            h->m_producer.m_index_base   = nullptr;
            h->m_producer.m_data_base    = nullptr;
            h->m_producer.m_control_base = nullptr;

            if (h->m_new_sem)
            {
                sem_close((sem_t*)h->m_new_sem);
                h->m_new_sem = NULL;
            }
            if (h->m_reg_sem)
            {
                sem_close((sem_t*)h->m_reg_sem);
                h->m_reg_sem = NULL;
            }

            // Optional: producer may sem_unlink(h->ch->new_entries_sem); sem_unlink(h->ch->registry_lock_sem);
        }

        const char* error_str(i32 result)
        {
            switch (result)
            {
                case MMQ_ERR_OK: return "Ok";
                case MMQ_ERR_INDEX_OPEN_RW: return "Failed to open/create index.mm read–write";
                case MMQ_ERR_DATA_OPEN_RW: return "Failed to open/create data.mm read–write";
                case MMQ_ERR_CONTROL_OPEN_RW: return "Failed to open/create control.mm read–write";
                case MMQ_ERR_INDEX_SANITY: return "index.mm sanity check failed";
                case MMQ_ERR_DATA_SANITY: return "data.mm sanity check failed";
                case MMQ_ERR_CONTROL_SANITY: return "control.mm sanity check failed";
                case MMQ_ERR_SEMAPHORE_OPEN: return "Failed to create/open named semaphore";
                case MMQ_ERR_REGISTRY_LOCK: return "Failed to lock consumer registry semaphore";
                case MMQ_ERR_CONSUMER_SLOTS_FULL: return "No free consumer slots available";
                case MMQ_ERR_INDEX_EXTEND: return "Failed to extend index.mm size";
                case MMQ_ERR_DATA_EXTEND: return "Failed to extend data.mm size";
                case MMQ_ERR_NO_MSG_AVAILABLE: return "No message available to consume";
                case MMQ_ERR_TIMEDOUT: return "Timed out waiting for new message";
                default: return "Unknown error code";
            }
        }

    }  // namespace nmmmq
}  // namespace ncore
