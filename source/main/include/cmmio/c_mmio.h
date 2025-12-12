#ifndef __CMMIO_MMIO_PUBLIC_H__
#define __CMMIO_MMIO_PUBLIC_H__
#include "ccore/c_target.h"
#ifdef USE_PRAGMA_ONCE
#    pragma once
#endif

#include "ccore/c_stream.h"

namespace ncore
{
    class alloc_t;

    namespace nmmio
    {
        struct mappedfile_t;
        void allocate(alloc_t* allocator, mappedfile_t*& out_mf);
        void deallocate(alloc_t* allocator, mappedfile_t* mf);

        bool        exists(mappedfile_t* mf, const char* path);
        bool        open_rw(mappedfile_t* mf, const char* path);
        bool        open_ro(mappedfile_t* mf, const char* path);
        bool        create_rw(mappedfile_t* mf, const char* path, u64 size);
        bool        create_ro(mappedfile_t* mf, const char* path, u64 size);
        bool        close(mappedfile_t* mf);
        bool        is_writeable(mappedfile_t* mf);
        bool        extend_size(mappedfile_t* mf, u64 new_size);
        void*       address_rw(mappedfile_t* mf);
        const void* address_ro(mappedfile_t* mf);
        u64         size(mappedfile_t* mf);
        void        sync(mappedfile_t* mf);
        void        sync(mappedfile_t* mf, u64 offset, u64 bytes);

    }  // namespace nmmio
}  // namespace ncore

#endif  ///< __CMMIO_MMIO_PUBLIC_H__
