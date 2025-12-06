#include "ccore/c_allocator.h"

#include "cmmio/c_mmio.h"

#include "cunittest/cunittest.h"

using namespace ncore;

// The following API needs to be tested:
//      bool        open_rw(mappedfile_t* mf, const char* path);
//      bool        open_ro(mappedfile_t* mf, const char* path);
//      bool        is_writeable(mappedfile_t* mf);
//      void*       address_rw(mappedfile_t* mf);
//      const void* address_ro(mappedfile_t* mf);
//      size_t      size(mappedfile_t* mf);
//      void        sync(mappedfile_t* mf);
//      void        sync(mappedfile_t* mf, size_t offset, size_t bytes);

UNITTEST_SUITE_BEGIN(mmio)
{
    UNITTEST_FIXTURE(basic)
    {
        UNITTEST_FIXTURE_SETUP() {}
        UNITTEST_FIXTURE_TEARDOWN() {}

        UNITTEST_ALLOCATOR;

        UNITTEST_TEST(create)
        {
            nmmio::mappedfile_t* mf = nullptr;
            nmmio::allocate(Allocator, mf);
            nmmio::deallocate(Allocator, mf);
        }

        UNITTEST_TEST(open_nonexistent_file)
        {
            nmmio::mappedfile_t* mf = nullptr;
            nmmio::allocate(Allocator, mf);

            bool result = nmmio::open_ro(mf, "this_file_does_not_exist.txt");
            CHECK_FALSE(result);

            nmmio::deallocate(Allocator, mf);
        }

        UNITTEST_TEST(open_existing_file)
        {
            nmmio::mappedfile_t* mf = nullptr;
            nmmio::allocate(Allocator, mf);

            bool result = nmmio::open_ro(mf, "data/test.bin");
            CHECK_TRUE(result);

            nmmio::close(mf);
            nmmio::deallocate(Allocator, mf);
        }

    }
}
UNITTEST_SUITE_END
