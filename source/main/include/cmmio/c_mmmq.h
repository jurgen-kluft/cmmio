#ifndef __CMMIO_MMMQ_H__
#define __CMMIO_MMMQ_H__
#include "ccore/c_target.h"
#ifdef USE_PRAGMA_ONCE
#    pragma once
#endif

namespace ncore
{
    // Summary:
    // Single Producer / Multiple Consumer Message Queue using memory-mapped files and semaphores
    // Producer appends messages to data.mm and index.mm, then notifies consumers via control.mm semaphore.
    // Consumers register in control.mm, then drains available messages from index.mm and data.mm.
    // No dynamic memory allocation or copying is performed during publish or consume operations.
    // Designed for high-throughput, low-latency IPC on same machine.

    namespace nmmmq
    {
        struct handle_t;

        struct config_t
        {
            config_t(uint_t index_bytes, uint_t data_bytes, u16 max_consumers)
                : index_initial_bytes(index_bytes)
                , data_initial_bytes(data_bytes)
                , max_consumers(max_consumers)
            {
            }
            uint_t index_initial_bytes;
            uint_t data_initial_bytes;
            u16    max_consumers;
        };

        // Create handle
        handle_t* create_handle(alloc_t* allocator);
        void      destroy_handle(handle_t*& h);

        // Producer opens mmaps (RW) and semaphores (new_sem, reg_sem).
        i32 init_producer(handle_t* h, const config_t& config, const char* index_path, const char* data_path, const char* control_path, const char* new_sem_name, const char* reg_sem_name);

        // Producer publishes one message (append-only data + index two-phase commit).
        i32 publish(handle_t* h, const void* msg, u32 len);

        // Consumer attaches: index/data (RO), control (RW); opens named semaphores.
        i32 attach_consumer(handle_t* h, const char* index_path, const char* data_path, const char* control_path);

        // Consumer registers (protected by registry_lock semaphore), returns slot index >= 0 or -1 when full.
        // @name: maximum length is 44 chars (including null terminator)
        i32 register_consumer(handle_t* h, const char* name, u32 start_seq);

        // Consumer drains all available READY entries, keep calling until none left.
        // Function will return false when no more messages are available, true otherwise.
        // No memory allocation or copy is performed, the user has to process the message immediately
        // or copy it elsewhere, there is no guarantee the message will remain valid after any
        // call to this API.
        bool consumer_drain(handle_t* h, i32 slot_index, u8 const*& msg_data, u32& msg_len);

        // Blocking wait for new entries (sem_wait).
        i32 wait_for_new(handle_t* h);

        // Emulated timed wait (macOS lacks sem_timedwait): trywait + sleeps for timeout_us.
        i32 wait_for_new_timeout(handle_t* h, u32 timeout_us);

        // Close/unmap files and close semaphores. (Producer may also sem_unlink by names if desired.)
        void close_handle(handle_t* h);

    }  // namespace nmmmq
}  // namespace ncore

#endif  // __CMMIO_MMMQ_H__
