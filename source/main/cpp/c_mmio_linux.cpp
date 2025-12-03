#include "ccore/c_target.h"

#ifdef TARGET_LINUX
#    include "ccore/c_memory.h"
#    include "ccore/c_vmem.h"
#    include "ccore/c_allocator.h"

#    include <assert.h>
#    include <errno.h>
#    include <exception>
#    include <fcntl.h>
#    include <limits>
#    include <optional>
#    include <string.h>
#    include <string>
#    include <sys/mman.h>
#    include <sys/stat.h>
#    include <sys/types.h>
#    include <unistd.h>

namespace ncore
{
    namespace nmmio
    {
        class filedescr_t
        {
        public:
            int m_fd;

            bool open(const char* path, int flags, int mode = 0666 /* octal permissions */)
            {
                close();
                m_fd = ::open(path, flags, mode);
                return valid();
            }

            bool valid() const { return m_fd != -1; }

            void close()
            {
                if (valid())
                {
                    ::close(m_fd);
                    m_fd = -1;
                }
            }

            size_t size() const
            {
                if (!valid())
                    return 0;

                struct stat s;
                if (fstat(m_fd, &s) == -1)
                    return 0;
                return static_cast<size_t>(s.st_size);
            }

            bool truncate(size_t size) { return valid() ? !(ftruncate(m_fd, size) == -1) : false; }
        };

        class memorymap_t
        {
        public:
            size_t      m_size       = std::numeric_limits<size_t>::max();
            void*       m_rw_address = nullptr;
            const void* m_ro_address = nullptr;
            bool        m_fixed      = false;

            bool is_writeable() const { return m_rw_address != nullptr; }

            bool map_rw(void* addr, size_t length, int flags, int fd, off_t offset)
            {
                m_size       = length;
                m_rw_address = mmap(const_cast<void*>(addr), length, PROT_READ | PROT_WRITE, flags, fd, offset);
                m_ro_address = m_rw_address;
#    if __APPLE__
                m_fixed = ((flags & (MAP_FIXED)) != 0);
#    else
                m_fixed = ((flags & (MAP_FIXED | MAP_FIXED_NOREPLACE)) != 0);
#    endif
                return !(m_rw_address == MAP_FAILED);
            }

            bool map_ro(const void* addr, size_t length, int flags, int fd, off_t offset)
            {
                m_size       = length;
                m_rw_address = nullptr;
                m_ro_address = mmap(const_cast<void*>(addr), length, PROT_READ, flags, fd, offset);

#    if __APPLE__
                m_fixed = ((flags & (MAP_FIXED)) != 0);
#    else
                m_fixed = ((flags & (MAP_FIXED | MAP_FIXED_NOREPLACE)) != 0);
#    endif
                return !(m_ro_address == MAP_FAILED);
            }

            void close() { unmap(); }

            void*       address_rw() const { return m_rw_address; }
            void*       address_rw(size_t offset) const { return m_rw_address != nullptr ? static_cast<void*>(static_cast<char*>(m_rw_address) + offset) : nullptr; }
            const void* address_ro() const { return m_ro_address; }

            size_t size() const { return m_size; }

            bool sync(size_t offset, size_t size) const
            {
                if (!is_writeable())
                    return false;

                assert(offset + size <= m_size);
                size_t alignedOffset = offset & ~(v_alloc_get_page_size() - 1);
                size_t alignedSize   = size + offset - alignedOffset;
                void*  offsetAddress = static_cast<void*>(static_cast<std::byte*>(const_cast<void*>(m_rw_address)) + alignedOffset);
                if (msync(offsetAddress, alignedSize, MS_SYNC | MS_INVALIDATE) == -1)
                    return false;
                return true;
            }

            bool sync() const
            {
                if (!is_writeable())
                    return false;

                // ENOMEM "Cannot allocate memory" here likely means something remapped
                // the range before this object went out of scope. I haven't found a
                // good way to avoid this other than the user being careful to delete
                // the object before remapping.
                if (msync(const_cast<void*>(m_rw_address), m_size, MS_SYNC | MS_INVALIDATE) == -1)
                {
                    return false;
                }
                return true;
            }

            bool unmap()
            {
                if (m_ro_address != MAP_FAILED && m_ro_address != nullptr)
                {
                    // Perhaps controversial to do unconditionally, but safer/less surprising?
                    if (is_writeable())
                        sync();

                    // If the mapping was created with a specific address and MAP_FIXED,
                    // restore the original mapping to PROT_NONE to keep the range
                    // reserved. Otherwise, unmap.
                    if (m_fixed)
                    {
                        if (mmap(const_cast<void*>(m_ro_address), m_size, PROT_NONE, MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS, -1, 0) == MAP_FAILED)
                            return false;
                    }
                    else if (munmap(const_cast<void*>(m_ro_address), m_size) == -1)
                    {
                        return false;
                    }

                    m_rw_address = nullptr;
                    m_ro_address = nullptr;
                    return true;
                }
                return false;
            }
        };

        struct mappedfile_t
        {
            int         m_protection = PROT_READ | PROT_WRITE;
            filedescr_t m_file;
            memorymap_t m_mapped;
        };

        bool open_rw(mappedfile_t* mf, const char* path)
        {
            if (mf->m_file.open(path, O_RDWR))
            {
                return mf->m_mapped.map_rw(nullptr, mf->m_file.size(), MAP_SHARED, mf->m_file.m_fd, 0);
            }
            return false;
        }

        bool open_ro(mappedfile_t* mf, const char* path)
        {
            if (mf->m_file.open(path, O_RDONLY))
            {
                return mf->m_mapped.map_ro(nullptr, mf->m_file.size(), MAP_SHARED, mf->m_file.m_fd, 0);
            }
            return false;
        }

        bool close(mappedfile_t* mf)
        {
            bool unmapped = mf->m_mapped.unmap();
            mf->m_file.close();
            return unmapped;
        }

        void*       data_rw(mappedfile_t* mf) { return mf->m_mapped.address_rw(); }
        const void* data_ro(mappedfile_t* mf) { return mf->m_mapped.address_ro(); }
        size_t      size(mappedfile_t* mf) { return mf->m_mapped.size(); }
        void        sync(mappedfile_t* mf) { mf->m_mapped.sync(); }
        void        sync(mappedfile_t* mf, size_t offset, size_t size) { mf->m_mapped.sync(offset, size); }

        void allocate(alloc_t* allocator, mappedfile_t*& out_mf)
        {
            // construct a mappedfile_t using the provided allocator
            out_mf = g_construct<mappedfile_t>(allocator);
        }

        void deallocate(alloc_t* allocator, mappedfile_t* mf)
        {
            if (mf)
            {
                close(mf);
                allocator->deallocate(mf);
            }
        }

    }  // namespace nmmio
}  // namespace ncore

#endif