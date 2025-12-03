#include "cmmio/c_mmio.h"
#include "ccore/c_allocator.h"
#include "ccore/c_memory.h"
#include "ccore/c_vmem.h"

#ifdef TARGET_PC
#    include <assert.h>
#    include <windows.h>
#    include <subauth.h>  // must come after windows.h
#endif

#if defined(TARGET_MAC) || defined(TARGET_LINUX)
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
#endif

#    ifdef TARGET_PC

namespace ncore
{
    namespace nmmio
    {
#        define INVALID_HANDLE_VALUE (HANDLE)(LONG_PTR) - 1

        struct handle_t
        {
            handle_t()
                : m_handle(INVALID_HANDLE_VALUE)
            {
            }

            handle_t(HANDLE handle)
                : m_handle(handle)
            {
            }

            bool   valid() const { return m_handle == INVALID_HANDLE_VALUE; }
            HANDLE native() const { return m_handle; }

            void close()
            {
                if (valid())
                {
                    CloseHandle(m_handle);
                    m_handle = INVALID_HANDLE_VALUE;
                }
            }

            HANDLE m_handle;
        };

        struct filehandle_t
        {
            handle_t m_handle;

            bool open(const char* path)
            {
                m_handle = handle_t(CreateFileW(path, .....));
                return m_handle.is_valid();
            }

            void close() { m_handle.close(); }

            void setPointer(ptrdiff_t distance, DWORD moveMethod = FILE_BEGIN) { SetFilePointerEx(m_handle.native(), LARGE_INTEGER{.QuadPart = distance}, nullptr, moveMethod); }
            void setEndOfFile() { SetEndOfFile(m_handle.native()); }

            size_t size()
            {
                LARGE_INTEGER result;
                if (!GetFileSizeEx(m_handle.native(), &result))
                    throw LastError();
                return result.QuadPart;
            }

            bool flush() const { return FlushFileBuffers(m_handle.native()); }
        };

        struct filemapping_handle_t
        {
            handle_t m_handle;

            bool open(const filehandle_t& file, LPSECURITY_ATTRIBUTES fileMappingAttributes, DWORD protect, size_t maximumSize, LPCWSTR name = nullptr)
            {
                // Mapping backed by filesystem file
                return create(static_cast<HANDLE>(file), fileMappingAttributes, protect, maximumSize, name);
            }

            bool open(LPSECURITY_ATTRIBUTES fileMappingAttributes, DWORD protect, size_t maximumSize, LPCWSTR name = nullptr)
            {
                // Mapping backed by system paging file
                return create(INVALID_HANDLE_VALUE, fileMappingAttributes, protect, maximumSize, name);
            }

            void close() { m_handle.close(); }

        private:
            bool create(HANDLE fileHandle, LPSECURITY_ATTRIBUTES fileMappingAttributes, DWORD protect, size_t maximumSize, LPCWSTR name = nullptr)
            {
                m_handle = handle_t(CreateFileMappingW(fileHandle, fileMappingAttributes, protect, (maximumSize >> 32) & 0xffffffff, maximumSize & 0xffffffff, name));
            }
        };

        struct filemapping_view_t
        {
            bool open_rw(const filemapping_handle_t& fileMapping, DWORD desiredAccess, size_t fileOffset = 0, size_t bytesToMap = 0 /* 0 means to the end */, void* baseAddress = nullptr)
            {
                m_address_rw = MapViewOfFileEx(fileMapping.m_handle, desiredAccess, (fileOffset >> 32) & 0xffffffff, fileOffset & 0xffffffff, bytesToMap, baseAddress);
                m_address_ro = m_address_rw;
                return m_address_rw != nullptr;
            }

            bool open_ro(const filemapping_handle_t& fileMapping, DWORD desiredAccess, size_t fileOffset = 0, size_t bytesToMap = 0 /* 0 means to the end */, void* baseAddress = nullptr)
            {
                m_address_rw = nullptr;
                m_address_ro = MapViewOfFileEx(fileMapping.m_handle, desiredAccess, (fileOffset >> 32) & 0xffffffff, fileOffset & 0xffffffff, bytesToMap, baseAddress);
                return m_address_ro != nullptr;
            }

            bool is_writeable() const { return m_address_rw != nullptr; }

            void*       address_rw() const { return m_address_rw; }
            const void* address_ro() const { return m_address_ro; }

            void close()
            {
                if (m_address_rw)
                {
                    UnmapViewOfFile(m_address_rw);
                }
                else if (m_address_ro)
                {
                    UnmapViewOfFile(m_address_ro);
                }
                m_address_rw = nullptr;
                m_address_ro = nullptr;
            }

            MEMORY_BASIC_INFORMATION query() const
            {
                MEMORY_BASIC_INFORMATION result;
                (void)VirtualQuery(m_address, &result, sizeof(result));
                return result;
            }

            void flush(size_t offset = 0, size_t bytes = 0) const
            {
                if (!FlushViewOfFile(static_cast<LPCVOID>(static_cast<const std::byte*>(m_address) + offset), bytes))
                    throw LastError();
            }

            LPVOID m_address_rw;
            LPVOID m_address_ro;
        };

        struct mappedfile_t
        {
            filehandle_t         m_file;
            size_t               m_size;
            filemapping_handle_t m_mapping;
            filemapping_view_t   m_rawView;

            DCORE_CLASS_PLACEMENT_NEW_DELETE
        };

        bool open_rw(mappedfile_t* mf, const char* path)
        {
            if (mf->m_file.open(path, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr))
            {
                mf->m_size = mf->m_file.size();
                if (mf->m_mapping.open(mf->m_file, nullptr, PAGE_READWRITE, mf->m_size, nullptr))
                {
                    return mf->m_rawView.open(mf->m_mapping, FILE_MAP_READ | FILE_MAP_WRITE);
                }
                else
                {
                    mf->m_file.close();
                }
            }
            return false;
        }

        bool open_ro(mappedfile_t* mf, const char* path)
        {
            if (mf->m_file.open(path, GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr))
            {
                mf->m_size = mf->m_file.size();
                if (mf->m_mapping.open(mf->m_file, nullptr, PAGE_READONLY, mf->m_size, nullptr))
                {
                    return mf->m_rawView.open(mf->m_mapping, FILE_MAP_READ);
                }
                else
                {
                    mf->m_file.close();
                }
            }
            return false;
        }

        bool close(mappedfile_t* mf)
        {
            mf->m_rawView.close();
            mf->m_mapping.close();
            mf->m_file.close();
            return true;
        }

        bool is_writeable(mappedfile_t* mf) { return mf->m_rawView.is_writeable(); }

        void*       address_rw(mappedfile_t* mf) { return mf->m_rawView.address_rw(); }
        const void* address_ro(mappedfile_t* mf) { return mf->m_rawView.address_ro(); }

        size_t size(mappedfile_t* mf) { return mf->m_size; }

        void sync(mappedfile_t* mf)
        {
            if (!is_writeable())
                return;
            mf->m_rawView.flush();  // async flush pages of whole mapping
            mf->m_file.flush();     // flush metadata and wait
        }

        void sync(mappedfile_t* mf, size_t offset, size_t bytes)
        {
            if (!is_writeable())
                return;
            assert(offset + bytes <= m_size);
            mf->m_rawView.flush(offset, bytes);  // async flush pages of range
            mf->m_file.flush();                  // flush metadata and wait
        }

        void allocate(alloc_t* allocator, mappedfile_t*& out_mf)
        {
            // construct a mappedfile_t using the provided allocator
            out_mf = g_construct<mappedfile_t>(allocator);
        }

        void deallocate(alloc_t* allocator, mappedfile_t* mf) { g_destruct(allocator, mf); }

    }  // namespace nmmio
}  // namespace ncore

#    endif

// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------

#    ifdef TARGET_MAC

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
#        if __APPLE__
                m_fixed = ((flags & (MAP_FIXED)) != 0);
#        else
                m_fixed = ((flags & (MAP_FIXED | MAP_FIXED_NOREPLACE)) != 0);
#        endif
                return !(m_rw_address == MAP_FAILED);
            }

            bool map_ro(const void* addr, size_t length, int flags, int fd, off_t offset)
            {
                m_size       = length;
                m_rw_address = nullptr;
                m_ro_address = mmap(const_cast<void*>(addr), length, PROT_READ, flags, fd, offset);

#        if __APPLE__
                m_fixed = ((flags & (MAP_FIXED)) != 0);
#        else
                m_fixed = ((flags & (MAP_FIXED | MAP_FIXED_NOREPLACE)) != 0);
#        endif
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

#    endif
