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
int read_on_server(const char *path, char *buf, size_t size, struct fuse_file_info *fi);
int truncate_on_server(const char *path);
int write_to_server(const char *path, const char *buf, size_t size, struct fuse_file_info *fi);
int close_on_server(const char *path, struct fuse_file_info *fi);
int fsync_on_server(const char *path, struct fuse_file_info *fi);
int utimens_on_server(const char *path, const struct timespec ts[2]);
////////////////////////////////////////////////////////////////////////////////////////////////
int lock_on_server(const char *path, rw_lock_mode_t mode);
int unlock_on_server(const char *path, rw_lock_mode_t mode);
////////////////////////////////////////////////////////////////////////////////////////////////

int download_file(FileUtil* fileUtil, const char *path, struct fuse_file_info* fi) {
    DLOG("Download file: %s\n", path);

    FileData* clientFileData = fileUtil->getClientFileData(path);
    if (clientFileData == nullptr) {
        DLOG("cannot find file in cache");
        return -1; //TODO: better error
    }
    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    int ret = 0;

    ret = ftruncate(fd_client, 0);
    if (ret < 0) {
        DLOG("Unable to truncate the file: %d\n", errno);
        return -errno;
    }

    ret = fsync_on_server(path, fi);
    if (ret < 0) {
        DLOG("Failed to fsync on server due to error: %d\n", -ret);
        return ret;
    }

    // getattr of file from server
    RAII<struct stat> statbuf;
    statbuf->st_size = 0; //set it to 0 before making the call
    ret = getattr_on_server(path, statbuf.ptr);
    if (ret < 0) {
        DLOG("Failed to get the attributes due to error: %d\n", -ret);
        return ret;
    }
    DLOG("Size: %ld\n", statbuf->st_size);

    ret = lock_on_server(path, RW_READ_LOCK);
    if (ret < 0) {
        DLOG("Failed to accquire lock on server: %d\n", -ret);
        return ret;
    }

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

    ret = unlock_on_server(path, RW_READ_LOCK);
    if (ret < 0) {
        delete []buf;
        DLOG("Unable to unlock it on server: %d\n", -ret);
        return ret;
    }

    //write the file in client cache
    ret = pwrite(fd_client, buf, statbuf->st_size, 0);
    if (ret < 0) {
        DLOG("Unable to write in client cache due to error: %d\n", errno);
        delete []buf;
        return -errno;
    }
    delete []buf;

    //update file metadata in client cache
    struct timespec times[] {statbuf->st_atim, statbuf->st_mtim};
    ret = futimens(fd_client, times);
    if (ret < 0) {
        DLOG("Unable to update file time metadata in client cache due to error: %d\n", errno);
        return -errno;
    }

    fileUtil->updateTc(path);

    return 0;
}


int upload_file(FileUtil* fileUtil, const char *path, struct fuse_file_info *fi) {
    DLOG("Upload file: %s\n", path);

    FileData* clientFileData = fileUtil->getClientFileData(path);
    if (clientFileData == nullptr) {
        DLOG("cannot find file in cache");
        return -1; //TODO: better error
    }
    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    int ret = 0;

    //fsync all the data to file
    ret = fsync(fd_client);
    if (ret < 0) {
        DLOG("Failed to fsync on cache file due to error: %d\n", errno);
        return -errno;
    }

    RAII<struct stat> statbuf;
    statbuf->st_size = 0; //set it to 0 before making the call
    // getattr of file from cache
    ret = fstat(fd_client, statbuf.ptr);
    if (ret < 0) {
        DLOG("Failed to get the attributes due to error: %d\n", errno);
        return -errno;
    }
    DLOG("Size: %ld\n", statbuf->st_size);

    char *buf = new char[statbuf->st_size];
    // read file from cache
    ret = pread(fd_client, buf, statbuf->st_size, 0);
    if (ret < 0) {
        DLOG("Failed to read from cache due to error: %d\n", errno);
        delete []buf;
        return -errno;
    }
    DLOG("Buffer: %s\n", buf);

    //truncate on server
    ret = truncate_on_server(path);
    if (ret < 0) {
        DLOG("Failed to truncate on server due to error: %d\n", -ret);
        delete []buf;
        return ret;
    }

    ret = lock_on_server(path, RW_WRITE_LOCK);
    if (ret < 0) {
        DLOG("Failed to accquire lock on server: %d\n", -ret);
        delete []buf;
        return ret;
    }

    //write the file to server
    ret = write_to_server(path, buf, statbuf->st_size, fi);
    if (ret < 0) {
        DLOG("Unable to write in client cache due to error: %d\n", -ret);
        delete []buf;
        unlock_on_server(path, RW_WRITE_LOCK);
        return ret;
    }

    delete []buf;

    ret = unlock_on_server(path, RW_WRITE_LOCK);
    if (ret < 0) {
        DLOG("Unable to unlock it on server: %d\n", -ret);
        return ret;
    }

    //update metadata on server
    struct timespec times[] {statbuf->st_atim, statbuf->st_mtim};
    ret = utimens_on_server(path, times);
    if (ret < 0) {
        DLOG("Unable to update time on server due to error: %d\n", -ret);
        return ret;
    }

    fileUtil->updateTc(path);

    return 0;
}

bool isFresh(FileUtil* fileUtil, const char *path, struct fuse_file_info *fi) {
    DLOG("isFresh called for '%s'", path);

    FileData* clientFileData = fileUtil->getClientFileData(path);
    if (clientFileData == nullptr) {
        DLOG("isFresh: Unable to find the file in cache");
        return false; //TODO: Find a appropriate error code
    }
    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    int ret = 0;

    struct timespec tp;
    ret = clock_gettime(CLOCK_REALTIME, &tp);
    if (ret < 0) {
        DLOG("isFresh: Failed to get the current time due to error: %d\n", errno);
        return false;
    }
    time_t t = tp.tv_sec;

    // [T - Tc < t]
    if (t - clientFileData->tc < fileUtil->cacheInterval) return true;

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

int fsync_on_server(const char *path, struct fuse_file_info *fi) {
    DLOG("fsync called for '%s'", path);

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

    int rpc_ret = rpcCall((char *)"fsync", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("fsync rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
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
