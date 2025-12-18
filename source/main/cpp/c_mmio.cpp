#include "cmmio/c_mmio.h"
#include "ccore/c_allocator.h"
#include "ccore/c_debug.h"
#include "ccore/c_memory.h"
#include "ccore/c_vmem.h"

#ifdef TARGET_PC
#    include <windows.h>
#    include <subauth.h>  // must come after windows.h
#endif

#if defined(TARGET_MAC) || defined(TARGET_LINUX)
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

#ifdef TARGET_PC

namespace ncore
{
    namespace nmmio
    {
#    define INVALID_HANDLE_VALUE (HANDLE)(LONG_PTR) - 1

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

            bool   is_valid() const { return m_handle != INVALID_HANDLE_VALUE; }
            HANDLE native() const { return m_handle; }

            void close()
            {
                if (is_valid())
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
                m_handle = handle_t(CreateFileA(path, .....));
                return m_handle.is_valid();
            }

            void close() { m_handle.close(); }

            bool setPointer(ptrdiff_t distance, DWORD moveMethod = FILE_BEGIN)
            {
                if (!is_valid())
                    return false;
                SetFilePointerEx(m_handle.native(), LARGE_INTEGER{.QuadPart = distance}, nullptr, moveMethod);
                return true;
            }
            void setEndOfFile() { SetEndOfFile(m_handle.native()); }

            u64 size()
            {
                if (!is_valid())
                    return false;

                LARGE_INTEGER result;
                if (!GetFileSizeEx(m_handle.native(), &result))
                    return 0;
                return result.QuadPart;
            }

            bool flush() const
            {
                if (!is_valid())
                    return false;
                return FlushFileBuffers(m_handle.native());
            }
        };

        struct filemapping_handle_t
        {
            handle_t m_handle;

            bool open(const filehandle_t& file, LPSECURITY_ATTRIBUTES fileMappingAttributes, DWORD protect, u64 maximumSize, LPCWSTR name = nullptr)
            {
                // Mapping backed by filesystem file
                return create(static_cast<HANDLE>(file), fileMappingAttributes, protect, maximumSize, name);
            }

            bool open(LPSECURITY_ATTRIBUTES fileMappingAttributes, DWORD protect, u64 maximumSize, LPCWSTR name = nullptr)
            {
                // Mapping backed by system paging file
                return create(INVALID_HANDLE_VALUE, fileMappingAttributes, protect, maximumSize, name);
            }

            void close() { m_handle.close(); }

        private:
            bool create(HANDLE fileHandle, LPSECURITY_ATTRIBUTES fileMappingAttributes, DWORD protect, u64 maximumSize, LPCWSTR name = nullptr)
            {
                m_handle = handle_t(CreateFileMappingW(fileHandle, fileMappingAttributes, protect, (maximumSize >> 32) & 0xffffffff, maximumSize & 0xffffffff, name));
            }
        };

        struct filemapping_view_t
        {
            bool open_rw(const filemapping_handle_t& fileMapping, DWORD desiredAccess, u64 fileOffset = 0, u64 bytesToMap = 0 /* 0 means to the end */, void* baseAddress = nullptr)
            {
                m_address_rw = MapViewOfFileEx(fileMapping.m_handle, desiredAccess, (fileOffset >> 32) & 0xffffffff, fileOffset & 0xffffffff, bytesToMap, baseAddress);
                m_address_ro = m_address_rw;
                return m_address_rw != nullptr;
            }

            bool open_ro(const filemapping_handle_t& fileMapping, DWORD desiredAccess, u64 fileOffset = 0, u64 bytesToMap = 0 /* 0 means to the end */, void* baseAddress = nullptr)
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

            void flush(u64 offset = 0, u64 bytes = 0) const
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
            u64                  m_size;
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

        bool create_rw(mappedfile_t* mf, const char* path) { return false; }

        bool create_ro(mappedfile_t* mf, const char* path) { return false; }

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

        u64 size(mappedfile_t* mf) { return mf->m_size; }

        void sync(mappedfile_t* mf)
        {
            if (!is_writeable())
                return;
            mf->m_rawView.flush();  // async flush pages of whole mapping
            mf->m_file.flush();     // flush metadata and wait
        }

        void sync(mappedfile_t* mf, u64 offset, u64 bytes)
        {
            if (!is_writeable())
                return;
            ASSERT(offset + bytes <= m_size);
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

#endif

// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------
// --------------------------------------------------------------------------------------------------------------------------------------------------------

#ifdef TARGET_MAC

#    define INVALID_FILE_DESCRIPTOR (i32)-1

namespace ncore
{
    namespace nmmio
    {
        class filedescr_t
        {
        public:
            i32 m_fd;

            filedescr_t()
                : m_fd(INVALID_FILE_DESCRIPTOR)
            {
            }

            enum
            {
                DEFAULT_MODE = 0666,
            };

            bool exists(const char* path)
            {
                // use stats to check for existence
                struct stat s;
                return (stat(path, &s) == 0);
            }

            bool open_ro(const char* path)
            {
                close();
                m_fd = ::open(path, O_RDONLY, DEFAULT_MODE);
                return valid();
            }

            bool open_rw(const char* path)
            {
                close();
                m_fd = ::open(path, O_RDWR, DEFAULT_MODE);
                return valid();
            }

            bool create(const char* path, i32 flags, u64 size)
            {
                close();
                m_fd = ::open(path, flags, DEFAULT_MODE);
                if (valid())
                {
                    if (size > 0)
                    {
                        if (!truncate(size))
                            close();
                    }
                }
                return valid();
            }

            bool create_ro(const char* path, u64 size) { return create(path, O_RDONLY | O_CREAT, size); }
            bool create_rw(const char* path, u64 size) { return create(path, O_RDWR | O_CREAT, size); }

            bool valid() const { return m_fd != INVALID_FILE_DESCRIPTOR; }

            void close()
            {
                if (!valid())
                    return;
                ::close(m_fd);
                m_fd = INVALID_FILE_DESCRIPTOR;
            }

            u64 size() const
            {
                if (!valid())
                    return 0;
                struct stat s;
                if (fstat(m_fd, &s) == -1)
                    return 0;
                return static_cast<u64>(s.st_size);
            }

            bool truncate(u64 size)
            {
                if (valid())
                {
                    if (::ftruncate(m_fd, size) == -1)
                    {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        };

        class memorymap_t
        {
        public:
            u64         m_size       = 0;
            void*       m_rw_address = nullptr;
            const void* m_ro_address = nullptr;
            bool        m_fixed      = false;

            bool is_valid() const { return (m_size > 0) && (m_ro_address != MAP_FAILED); }
            bool is_writeable() const { return m_rw_address != nullptr; }

            bool map_rw(void* addr, u64 length, i32 flags, filedescr_t fd, off_t offset)
            {
                m_size       = length;
                m_rw_address = mmap(addr, length, PROT_READ | PROT_WRITE, flags, fd.m_fd, offset);
                m_ro_address = m_rw_address;
#    if __APPLE__
                m_fixed = ((flags & (MAP_FIXED)) != 0);
#    else
                m_fixed = ((flags & (MAP_FIXED | MAP_FIXED_NOREPLACE)) != 0);
#    endif
                return !(m_rw_address == MAP_FAILED);
            }

            bool map_ro(const void* addr, u64 length, i32 flags, filedescr_t fd, off_t offset)
            {
                m_size       = length;
                m_rw_address = nullptr;
                m_ro_address = mmap(const_cast<void*>(addr), length, PROT_READ, flags, fd.m_fd, offset);

#    if __APPLE__
                m_fixed = ((flags & (MAP_FIXED)) != 0);
#    else
                m_fixed = ((flags & (MAP_FIXED | MAP_FIXED_NOREPLACE)) != 0);
#    endif
                return !(m_ro_address == MAP_FAILED);
            }

            void close()
            {
                unmap();

                m_size       = 0;
                m_rw_address = nullptr;
                m_ro_address = nullptr;
                m_fixed      = false;
            }

            void*       address_rw() const { return m_rw_address; }
            void*       address_rw(u64 offset) const { return m_rw_address != nullptr ? static_cast<void*>(static_cast<char*>(m_rw_address) + offset) : nullptr; }
            const void* address_ro() const { return m_ro_address; }

            inline u64 size() const { return m_size; }

            bool sync(u64 offset, u64 size) const
            {
                if (!is_writeable())
                    return false;

                ASSERT((ssize_t)(offset + size) <= m_size);
                u64   alignedOffset = offset & ~(v_alloc_get_page_size() - 1);
                u64   alignedSize   = size + offset - alignedOffset;
                void* offsetAddress = static_cast<void*>(static_cast<std::byte*>(const_cast<void*>(m_rw_address)) + alignedOffset);
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

                    m_size       = -1;
                    m_rw_address = nullptr;
                    m_ro_address = nullptr;
                    return true;
                }
                return false;
            }
        };

        struct mappedfile_t
        {
            i32         m_protection = PROT_READ | PROT_WRITE;
            filedescr_t m_file;
            memorymap_t m_mapped;
        };

        bool exists(mappedfile_t* mf, const char* path) { return mf->m_file.exists(path); }

        bool open_rw(mappedfile_t* mf, const char* path)
        {
            if (mf->m_file.open_rw(path))
            {
                return mf->m_mapped.map_rw(nullptr, mf->m_file.size(), MAP_SHARED, mf->m_file, 0);
            }
            return false;
        }

        bool open_ro(mappedfile_t* mf, const char* path)
        {
            if (mf->m_file.open_ro(path))
            {
                return mf->m_mapped.map_ro(nullptr, mf->m_file.size(), MAP_SHARED, mf->m_file, 0);
            }
            return false;
        }

        bool create_rw(mappedfile_t* mf, const char* path, u64 size)
        {
            if (mf->m_file.create_rw(path, size))
            {
                return mf->m_mapped.map_rw(nullptr, mf->m_file.size(), MAP_SHARED, mf->m_file, 0);
            }
            return false;
        }

        bool create_ro(mappedfile_t* mf, const char* path, u64 size)
        {
            if (mf->m_file.create_ro(path, size))
            {
                return mf->m_mapped.map_ro(nullptr, mf->m_file.size(), MAP_SHARED, mf->m_file, 0);
            }
            return false;
        }

        bool close(mappedfile_t* mf)
        {
            bool unmapped = mf->m_mapped.unmap();
            mf->m_file.close();
            return unmapped;
        }

        bool is_writeable(mappedfile_t* mf) { return mf->m_mapped.is_writeable(); }

        bool extend_size(mappedfile_t* mf, u64 new_size)
        {
            if (mf->m_file.truncate(new_size))
            {
                if (mf->m_mapped.unmap())
                {
                    const u64 newsize = mf->m_file.size();
                    return mf->m_mapped.map_rw(nullptr, newsize, MAP_SHARED, mf->m_file, 0);
                }
            }

            return false;
        }

        void*       address_rw(mappedfile_t* mf) { return mf->m_mapped.address_rw(); }
        const void* address_ro(mappedfile_t* mf) { return mf->m_mapped.address_ro(); }
        u64         size(mappedfile_t* mf) { return mf->m_mapped.size(); }
        void        sync(mappedfile_t* mf) { mf->m_mapped.sync(); }
        void        sync(mappedfile_t* mf, u64 offset, u64 size) { mf->m_mapped.sync(offset, size); }

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
