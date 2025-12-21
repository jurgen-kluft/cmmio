#include "ccore/c_allocator.h"
#include "ccore/c_memory.h"

#include "cmmio/c_mmmq.h"

#include <unistd.h>
#include <cerrno>
#include <string.h>
#include <stdio.h>
#include <cstdlib>

class malloc_based_alloc_t : public ncore::alloc_t
{
public:
    void* v_allocate(ncore::u32 size, ncore::u32 alignment) { return malloc(size); }
    void  v_deallocate(void* ptr) { free(ptr); }
};

namespace ncore
{

    static malloc_based_alloc_t s_malloc_based_alloc;
    static alloc_t*             s_allocator = &s_malloc_based_alloc;

    static i32 producer()
    {
        nmmmq::handle_t* h = nmmmq::create_handle(s_allocator);

        const char* index_path   = "index.mm";
        const char* data_path    = "data.mm";
        const char* control_path = "control.mm";
        const char* new_sem_name = "mmq_new_entries_sem";
        const char* reg_sem_name = "mmq_registry_lock_sem";

        nmmmq::config_t config(1 * cMB, 10 * cMB, 16);

        printf("initializing producer with index_path=%s, data_path=%s, control_path=%s\n", index_path, data_path, control_path);

        i32 result = nmmmq::init_producer(h, config, index_path, data_path, control_path, new_sem_name, reg_sem_name);
        if (result < 0)
        {
            printf("producer: init failed (err = %s)\n", nmmmq::error_str(result));
            return 1;
        }

        const i32 num_seconds = 60;

        printf("producing messages for %d seconds...\n", num_seconds);

        char msg[128];
        for (int i = 0; i < (num_seconds * 20); ++i)
        {
            int n = snprintf(msg, sizeof(msg), "msg %d (pid=%d)", i, getpid());
            if (n < 0)
                n = 0;

            if (n >= (int)sizeof(msg))
                n = (int)sizeof(msg) - 1;

            i32 result = nmmmq::publish(h, msg, (u32)(n + 1));
            if (result < 0)
            {
                printf("producer: publish failed (err = %s)\n", nmmmq::error_str(result));
                nmmmq::close_handle(h);
                return 1;
            }

            // keep on the same line in the console and print the message count
            printf("\rproduced %d messages...", i + 1);
            fflush(stdout);

            usleep(50 * 1000);
        }

        printf("done producing messages.\n");

        nmmmq::close_handle(h);
        return 0;
    }

    static int consumer(const char* consumer_name, u32 start_seq = 0)
    {
        nmmmq::handle_t* h = nmmmq::create_handle(s_allocator);

        const char* index_path   = "index.mm";
        const char* data_path    = "data.mm";
        const char* control_path = "control.mm";

        printf("attaching consumer '%s' with start_seq=%u to index_path=%s, data_path=%s, control_path=%s\n", consumer_name, start_seq, index_path, data_path, control_path);

        i32 result = nmmmq::attach_consumer(h, index_path, data_path, control_path);
        if (result != 0)
        {
            printf("consumer: attach failed (err = %s)\n", nmmmq::error_str(result));
            return 1;
        }

        printf("registering consumer '%s' with start_seq=%u\n", consumer_name, start_seq);

        i32 slot;
        result = nmmmq::register_consumer(h, consumer_name, start_seq, slot);
        if (result<0)
        {
            printf("consumer: register failed (err = %s)\n", nmmmq::error_str(result));
            nmmmq::close_handle(h);
            return 1;
        }

        printf("starting to consume messages...\n");
        for (;;)
        {
            const u8* msg_data;
            u32       msg_len;
            if (!nmmmq::consumer_drain(h, slot, msg_data, msg_len))
            {
                if (!nmmmq::wait_for_new(h))
                {
                    printf("consumer: wait failed (errno=%d)\n", errno);
                    break;
                }
            }
            else
            {
                if (msg_data != nullptr)
                    printf("consumer '%s' got message: %.*s\n", consumer_name, msg_len, msg_data);

                usleep(80 * 1000);
            }
        }

        return 0;
    }

    int AppMain(int argc, const char** argv)
    {
        if (argc >= 2 && strcmp(argv[1], "producer") == 0)
        {
            producer();
        }
        else if (argc >= 2 && strcmp(argv[1], "consumer") == 0)
        {
            const char* consumer_name = (argc >= 3) ? argv[2] : "consumer1";
            u32         start_seq     = (argc >= 4) ? (u32)atoi(argv[3]) : 0;
            consumer(consumer_name, start_seq);
        }
        else
        {
            printf("Usage: %s [producer|consumer]\n", argv[0]);
            return -1;
        }
        return 0;
    }

}  // namespace ncore
