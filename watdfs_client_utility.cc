#include <errno.h>
#include <ctype.h>
#include <dirent.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "rpc.h"
#include "rw_lock.h"
#include "watdfs_client_utility.h"

#include "debug.h"

////////////////////////////////////////////////////////////////////////////////////////////////
int getattr_on_server(const char *path, struct stat *statbuf);
int open_on_server(const char *path, struct fuse_file_info *fi);
int read_on_server(const char *path, char *buf, size_t size, struct fuse_file_info *fi);
int truncate_on_server(const char *path);
int write_to_server(const char *path, const char *buf, size_t size, struct fuse_file_info *fi);
int close_on_server(const char *path, struct fuse_file_info *fi);
int utimens_on_server(const char *path, const struct timespec ts[2]);
////////////////////////////////////////////////////////////////////////////////////////////////
int lock_on_server(const char *path, rw_lock_mode_t mode);
int unlock_on_server(const char *path, rw_lock_mode_t mode);
////////////////////////////////////////////////////////////////////////////////////////////////

int download_file(FileUtil& fileUtil, const char *path, struct fuse_file_info *fi) {
    const char* full_path = fileUtil.getAbsolutePath(path);
    DLOG("Download file: %s\n", full_path);

    int ret = 0;
    FileData* clientFileData = fileUtil.getClientFileData(path);
    bool isOpen = (clientFileData != nullptr);

    ret = lock_on_server(path, RW_READ_LOCK);
    if (ret < 0) {
        DLOG("Failed to accquire lock on server: %d\n", -ret);
        return ret;
    }

    // getattr of file from server
    RAII<struct stat> statbuf;
    statbuf->st_size = 0; //set it to 0 before making the call
    ret = getattr_on_server(path, statbuf.ptr);
    if (ret < 0) {
        DLOG("Failed to get the attributes due to error: %d\n", -ret);
        unlock_on_server(path, RW_READ_LOCK);
        return ret;
    }
    DLOG("Size: %ld\n", statbuf->st_size);

    // open file on server based on provided flags
    // fi->fh will have server file descriptor
    if (!isOpen) {
        int temp_flags = fi->flags;
        if((fi->flags&O_ACCMODE) == O_WRONLY) fi->flags = O_RDWR;
        ret = open_on_server(path, fi);
        if (ret < 0) {
            DLOG("Failed to open/create file on server due to error: %d\n", -ret);
            unlock_on_server(path, RW_READ_LOCK);
            return ret;
        }
        fi->flags = temp_flags;
        DLOG("File Descriptor On Server: %ld\n", fi->fh);
    }

    // open/create file in client cache, truncate if it already exists
    ret = open(full_path, O_CREAT|O_RDWR|O_TRUNC, statbuf->st_mode);
    if (ret < 0) {
        DLOG("Unable to create/open file in client cache due to error: %d\n", errno);
        unlock_on_server(path, RW_READ_LOCK);
        return -errno;
    }
    int temp_fd_client = ret;
    DLOG("Temp File Descriptor: %d\n", temp_fd_client);

    if (statbuf->st_size > 0) { //read and write if file is not empty
        char *buf = new char[statbuf->st_size];
        //read file from server
        ret = read_on_server(path, buf, statbuf->st_size, fi);
        if (ret < 0) {
            DLOG("Failed to read from server due to error: %d\n", -ret);
            delete []buf;
            unlock_on_server(path, RW_READ_LOCK);
            return ret;
        }
        DLOG("Buffer: %s\n", buf);

        //write the file in client cache
        ret = write(temp_fd_client, buf, statbuf->st_size);
        if (ret < 0) {
            DLOG("Unable to write in client cache due to error: %d\n", errno);
            delete []buf;
            unlock_on_server(path, RW_READ_LOCK);
            return -errno;
        }
        delete []buf;
    }

    //close on client
    ret = close(temp_fd_client);
    if (ret < 0) {
        DLOG("Unable to close in cache cache due to error: %d\n", errno);
        unlock_on_server(path, RW_READ_LOCK);
        return -errno;
    }

    int fd_client = -1;
    if(isOpen) {
        fd_client = clientFileData->fh;
        fileUtil.updateTc(path);
    } else {
        ret = open(full_path, fi->flags);
        if (ret < 0) {
            DLOG("Unable to open corresponding to given flags: %d\n", errno);
            unlock_on_server(path, RW_READ_LOCK);
            return -errno;
        }
        fd_client = ret;

        fileUtil.addClientFileData(path, fd_client, fi->fh, fi->flags);
    }

    //update file metadata in client cache
    struct timespec times[] {statbuf->st_atim, statbuf->st_mtim};
    ret = futimens(fd_client, times);
    if (ret < 0) {
        DLOG("Unable to update file time metadata in client cache due to error: %d\n", errno);
        unlock_on_server(path, RW_READ_LOCK);
        return -errno;
    }

    ret = unlock_on_server(path, RW_READ_LOCK);
    if (ret < 0) DLOG("Unable to unlock it on server: %d\n", -ret);
    DLOG("unblock done");

    return 0;
}


int upload_file(FileUtil& fileUtil, const char *path,
                struct fuse_file_info *fi, bool closeFiles) {
    const char* full_path = fileUtil.getAbsolutePath(path);
    DLOG("Upload file: %s\n", full_path);

    int ret = 0;

    FileData* clientFileData = fileUtil.getClientFileData(path);
    if (clientFileData == nullptr) {
        DLOG("cannot find file in cache");
        return -1; //TODO: better error
    }
    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    ret = fsync(fd_client);
    if (ret < 0) {
        DLOG("fsync failed on the client due to error: %d", errno);
        return -errno;
    }

    ret = open(full_path, O_RDWR);
    if (ret < 0) {
        DLOG("Unable to open file in client cache due to error: %d\n", errno);
        return -errno;
    }
    int temp_fd_client = ret;
    DLOG("Temp File Descriptor: %d\n", temp_fd_client);

    ret = lock_on_server(path, RW_WRITE_LOCK);
    if (ret < 0) {
        DLOG("Failed to accquire lock on server: %d\n", -ret);
        return ret;
    }

    AccessType accessType = processAccessType(fi->flags);
    if (accessType == WRITE) {
        // getattr of file from cache
        RAII<struct stat> statbuf;
        statbuf->st_size = 0; //set it to 0 before making the call
        ret = fstat(temp_fd_client, statbuf.ptr);
        if (ret < 0) {
            DLOG("Failed to get the attributes due to error: %d\n", errno);
            unlock_on_server(path, RW_WRITE_LOCK);
            return -errno;
        }
        DLOG("Size: %ld\n", statbuf->st_size);

        ret = truncate_on_server(path);
        if (ret < 0) {
            DLOG("Failed to truncate on server due to error: %d\n", -ret);
            unlock_on_server(path, RW_WRITE_LOCK);
            return ret;
        }

        if (statbuf->st_size > 0) { //read and write if file is not empty
            char *buf = new char[statbuf->st_size];

            //read file from cache
            ret = read(temp_fd_client, buf, statbuf->st_size);
            if (ret < 0) {
                DLOG("Failed to read from cache due to error: %d\n", errno);
                delete []buf;
                unlock_on_server(path, RW_WRITE_LOCK);
                return -errno;
            }
            DLOG("Buffer: %s\n", buf);

            //write the file to server
            ret = write_to_server(path, buf, statbuf->st_size, fi);
            if (ret < 0) {
                DLOG("Unable to write in client cache due to error: %d\n", -ret);
                delete []buf;
                unlock_on_server(path, RW_WRITE_LOCK);
                return ret;
            }
            delete []buf;
        }

        struct timespec times[] {statbuf->st_atim, statbuf->st_mtim};
        ret = utimens_on_server(path, times);
        if (ret < 0) {
            DLOG("Unable to update time on server due to error: %d\n", -ret);
            unlock_on_server(path, RW_WRITE_LOCK);
            return ret;
        }

        fileUtil.updateTc(path);
    }

    //close on tmp file on client
    ret = close(temp_fd_client);
    if (ret < 0) {
        DLOG("Unable to close tmp file due to error: %d\n", errno);
        unlock_on_server(path, RW_WRITE_LOCK);
        return -errno;
    }

    if (closeFiles) {
        //close from server
        ret = close_on_server(path, fi);
        if (ret < 0) {
            DLOG("Unable to close on server due to error: %d\n", -ret);
            unlock_on_server(path, RW_WRITE_LOCK);
            return ret;
        }

        //close on client
        ret = close(fd_client);
        if (ret < 0) {
            DLOG("Unable to close in cache cache due to error: %d\n", errno);
            unlock_on_server(path, RW_WRITE_LOCK);
            return -errno;
        }

        fileUtil.removeFile(path);
    }

    ret = unlock_on_server(path, RW_WRITE_LOCK);
    if (ret < 0) DLOG("Unable to unlock it on server: %d\n", -ret);

    return ret;
}

bool isFresh(FileUtil& fileUtil, const char *path, struct fuse_file_info *fi) {
    DLOG("isFresh called for '%s'", path);

    int ret = 0;

    FileData* clientFileData = fileUtil.getClientFileData(path);
    if (clientFileData == nullptr) {
        DLOG("isFresh: Unable to find the file in cache");
        return false; //TODO: Find a appropriate error code
    }
    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    struct timespec tp;
    ret = clock_gettime(CLOCK_REALTIME, &tp);
    if (ret < 0) {
        DLOG("isFresh: Failed to get the current time due to error: %d\n", errno);
        return false;
    }
    time_t t = tp.tv_sec;

    // [T - Tc < t]
    if (t - clientFileData->tc < fileUtil.cacheInterval) return true;

    // getattr of file from cache
    RAII<struct stat> statbuf;
    ret = fstat(fd_client, statbuf.ptr);
    if (ret < 0) {
        DLOG("isFresh: Failed to get the attributes from client due to error: %d\n", errno);
        return false;
    }
    time_t t_client = statbuf->st_mtim.tv_sec;

    memset(statbuf.ptr, 0, sizeof(struct stat));

    // getattr of file on server
    ret = getattr_on_server(path, statbuf.ptr);
    if (ret < 0) {
        DLOG("isFresh: Failed to get the attributes from server due to error: %d\n", -ret);
        return false;
    }
    time_t t_server = statbuf->st_mtim.tv_sec;

    if (t_client == t_server) return true;

    return false;
}

//////////////////////////////////////////////////////////////////////////////////////////

int getattr_on_server(const char *path, struct stat *statbuf) {
    DLOG("download getattr called for '%s'", path);
    
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(no, yes, yes, ARG_CHAR, (uint) sizeof(struct stat)); //statbuf
    args[1] = (void *)statbuf;

    RAII<int> ret(0);
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret.ptr;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"getattr", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("getattr rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else fxn_ret = *ret;

    if (fxn_ret < 0) memset(statbuf, 0, sizeof(struct stat));

    delete []args;

    return fxn_ret;
}

int open_on_server(const char *path, struct fuse_file_info *fi) {
    DLOG("download open called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, yes, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[1] = (void *)(fi);

    RAII<int> ret(0);
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret.ptr;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"open", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("open rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    delete []args;

    return fxn_ret;
}

int read_on_server(const char *path, char *buf, size_t size, struct fuse_file_info *fi) {
    DLOG("download read called for '%s'", path);

    int ARG_COUNT = 6;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(no, yes, yes, ARG_CHAR, MAX_ARRAY_LEN); //buf
    args[1] = (void *)buf;

    RAII<size_t> m_size((size_t)MAX_ARRAY_LEN);
    arg_types[2] = argTypeFrmtr(yes, no, no, ARG_LONG); //size
    args[2] = (void *)m_size.ptr;

    RAII<off_t> offset((off_t)0);
    arg_types[3] = argTypeFrmtr(yes, no, no, ARG_LONG); //offset
    args[3] = (void *)offset.ptr;

    arg_types[4] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[4] = (void *)(fi);

    RAII<int> ret(0);
    arg_types[5] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[5] = (void *)ret.ptr;
    
    arg_types[6] = 0; // the null terminator

    // Remember that size may be greater then the maximum array size of the RPC library
    long fxn_ret = 0;
    try {

        while (size > MAX_ARRAY_LEN) {
            int rpc_ret = rpcCall((char *)"read", arg_types, args);
            if (rpc_ret < 0) { DLOG("read rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; throw 1;}
            else if (*ret < 0) { fxn_ret = *ret; throw 1; } //trouble in reading at server
            else fxn_ret += *ret;

            if (*ret < MAX_ARRAY_LEN) throw 1; //EOF

            size -= MAX_ARRAY_LEN;
            *offset += MAX_ARRAY_LEN;
            args[1] = (void *)(buf+MAX_ARRAY_LEN);
        }

        arg_types[1] = argTypeFrmtr(no, yes, yes, ARG_CHAR, size); //buf
        *m_size = size;

        int rpc_ret = rpcCall((char *)"read", arg_types, args);
        if (rpc_ret < 0) { DLOG("read rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
        else if (*ret < 0) fxn_ret = *ret;
        else fxn_ret += *ret;
    }
    catch (...) {}

    delete []args;

    return fxn_ret;
}

int truncate_on_server(const char *path) {
    DLOG("upload truncate called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    RAII<off_t> newsize((off_t)0);
    arg_types[1] = argTypeFrmtr(yes, no, no, ARG_LONG); //newsize
    args[1] = (void *)newsize.ptr;

    RAII<int> ret(0);
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret.ptr;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"truncate", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("truncate rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    delete []args;

    return fxn_ret;
}

int write_to_server(const char *path, const char *buf, size_t size, struct fuse_file_info *fi) {
    DLOG("upload write called for '%s'", path);

    int ARG_COUNT = 6;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, MAX_ARRAY_LEN); //buf
    args[1] = (void *)buf;

    RAII<size_t> m_size((size_t)MAX_ARRAY_LEN);
    arg_types[2] = argTypeFrmtr(yes, no, no, ARG_LONG); //size
    args[2] = (void *)m_size.ptr;

    RAII<off_t> offset((off_t)0);
    arg_types[3] = argTypeFrmtr(yes, no, no, ARG_LONG); //offset
    args[3] = (void *)offset.ptr;

    arg_types[4] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[4] = (void *)(fi);

    RAII<int> ret(0);
    arg_types[5] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[5] = (void *)ret.ptr;
    
    arg_types[6] = 0; // the null terminator

    // Remember that size may be greater then the maximum array size of the RPC library
    long fxn_ret = 0;
    try {

        while (size > MAX_ARRAY_LEN) {
            int rpc_ret = rpcCall((char *)"write", arg_types, args);
            if (rpc_ret < 0) { DLOG("write rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; throw 1;}
            else if (*ret < 0) { fxn_ret = *ret; throw 1; } //trouble in writing at server
            else fxn_ret += *ret;

            if (*ret < MAX_ARRAY_LEN) throw 1; //EOF

            size -= MAX_ARRAY_LEN;
            *offset += MAX_ARRAY_LEN;
            args[1] = (void *)(buf+MAX_ARRAY_LEN);
        }

        arg_types[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, size); //buf
        *m_size = size;

        int rpc_ret = rpcCall((char *)"write", arg_types, args);
        if (rpc_ret < 0) { DLOG("write rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
        else if (*ret < 0) fxn_ret = *ret;
        else fxn_ret += *ret;
    }
    catch (...) {}

    delete []args;

    return fxn_ret;
}

int close_on_server(const char *path, struct fuse_file_info *fi) {
    DLOG("upload release called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[1] = (void *)(fi);

    RAII<int> ret(0);
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret.ptr;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"release", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("release rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    delete []args;

    return fxn_ret;
}

int utimens_on_server(const char *path, const struct timespec ts[2]) {
    DLOG("upload utimens called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) (sizeof(struct timespec)*2)); //ts
    args[1] = (void *)(ts);

    RAII<int> ret(0);
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret.ptr;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"utimens", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("utimens rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    delete []args;

    return fxn_ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////////////

int lock_on_server(const char *path, rw_lock_mode_t mode) {
    DLOG("lock called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, no, ARG_INT); //mode
    args[1] = (void *)(&mode);

    RAII<int> ret(0);
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret.ptr;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"lock", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("lock rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    delete []args;

    return fxn_ret;
}

int unlock_on_server(const char *path, rw_lock_mode_t mode) {
    DLOG("unlock called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, no, ARG_INT); //mode
    args[1] = (void *)(&mode);

    RAII<int> ret(0);
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret.ptr;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"unlock", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("unlock rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    delete []args;

    return fxn_ret;
}
