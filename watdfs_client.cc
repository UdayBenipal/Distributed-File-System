#include "watdfs_client.h"
#include "rpc.h"
#include "utility.h"

#include "debug.h"
INIT_LOG

# ifdef PRINT_ERR
#include <iostream>
#endif

#include "watdfs_client_utility.h"

FileUtil fileUtil;

// SETUP AND TEARDOWN
void *watdfs_cli_init(struct fuse_conn_info *conn, const char *path_to_cache,
                      time_t cache_interval, int *ret_code) {

    *ret_code = 0;

    int ret = 0;
    ret = rpcClientInit(); // RPC library setup

    if (ret < 0) {
        *ret_code = ret;
# ifdef PRINT_ERR
        std::cerr << "Failed to initialize RPC Client" << std::endl;
#endif
    }

    fileUtil.setDir(path_to_cache);
    fileUtil.cacheInterval = cache_interval;

    // TODO Initialize any global state that you require for the assignment and return it.
    // The value that you return here will be passed as userdata in other functions.
    // In A1, you might not need it, so you can return `nullptr`.
    void *userdata = nullptr;

    // TODO: save `path_to_cache` and `cache_interval` (for A3).

    // TODO: set `ret_code` to 0 if everything above succeeded else some appropriate
    // non-zero value.

    // Return pointer to global state data.
    return userdata;
}

void watdfs_cli_destroy(void *userdata) {
    // TODO: clean up your userdata state.

    int ret = 0;
    ret = rpcClientDestroy();

    if (ret < 0) {
# ifdef PRINT_ERR
        std::cerr << "Failed to delete RPC Client" << std::endl;
#endif
    }
}

// GET FILE ATTRIBUTES
int watdfs_cli_getattr(void *userdata, const char *path, struct stat *statbuf) {
    // SET UP THE RPC CALL
    DLOG("watdfs_cli_getattr called for '%s'", path);
    
    int ret = 0;

    FileData* clientFileData = fileUtil.getClientFileData(path);
    const bool isOpen = (clientFileData != nullptr);

    RAII<struct fuse_file_info> fi;
    if (isOpen) { fi->flags = clientFileData->flags; fi->fh = clientFileData->server_fh; }
    else fi->flags = O_RDONLY;

    AccessType accessType = processAccessType(fi->flags);
    if (!isOpen || (READ == accessType && !isFresh(fileUtil, path, fi.ptr))) {
        ret = download_file(fileUtil, path, fi.ptr);
        if (ret < 0) {
            DLOG("getattr: download failed");
            memset(statbuf, 0, sizeof(struct stat));
            return ret;
        }
        if (!isOpen) {
            clientFileData = fileUtil.getClientFileData(path);
            if (clientFileData == nullptr) {
                DLOG("getattr: cannot find file in cache after download");
                memset(statbuf, 0, sizeof(struct stat));
                return -1; //TODO: better error
            }
        }
    } else DLOG("getattr: its fresh");

    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    ret = fstat(fd_client, statbuf);
    if (ret < 0) {
        DLOG("Failed to get the attributes due to error: %d\n", errno);
        memset(statbuf, 0, sizeof(struct stat));
        return -errno;
    }

    if (!isOpen) {
        ret = upload_file(fileUtil, path, fi.ptr, true);
        if (ret < 0) {
            DLOG("getAttr: upload failed");
            return -1; //TODO: better error
        }
    }
    return 0;
}

// CREATE, OPEN AND CLOSE
int watdfs_cli_mknod(void *userdata, const char *path, mode_t mode, dev_t dev) {
    DLOG("watdfs_cli_mknod called for '%s'", path);

    int ARG_COUNT = 4;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, no, ARG_INT); //mode
    args[1] = (void *)(&mode);

    arg_types[2] = argTypeFrmtr(yes, no, no, ARG_LONG); //dev
    args[2] = (void *)(&dev);

    RAII<int> ret(0);
    arg_types[3] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[3] = (void *)ret.ptr;

    arg_types[4] = 0;

    int rpc_ret = rpcCall((char *)"mknod", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("mknod rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    delete []args;

    const char* full_path = fileUtil.getAbsolutePath(path);
    int sys_ret = mknod(full_path, mode, dev);
    if (sys_ret < 0) { DLOG("mknod failed for cache with error: %d", errno); fxn_ret = -errno; }

    return fxn_ret;
}

int watdfs_cli_open(void *userdata, const char *path, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_open called for '%s'", path);

    const bool isOpen = (fileUtil.getClientFileData(path) != nullptr);
    if (isOpen) { DLOG("File is already open"); return -EMFILE; }

    int ret = 0;
    ret = download_file(fileUtil, path, fi);

    return ret;
}

int watdfs_cli_release(void *userdata, const char *path, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_release called for '%s'", path);

    int ret = 0;
    ret = upload_file(fileUtil, path, fi, true);

    return ret;
}

// READ AND WRITE DATA
int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_read called for '%s'", path);

    int ret = 0;

    AccessType accessType = processAccessType(fi->flags);
    if (READ == accessType && !isFresh(fileUtil, path, fi)) {
        ret = download_file(fileUtil, path, fi);
        if (ret < 0) {
            DLOG("read: download failed");
            return -1; //TODO: better error
        }
    } else DLOG("read: its fresh");

    FileData* clientFileData = fileUtil.getClientFileData(path);
    if (clientFileData == nullptr) {
        DLOG("read: cannot find file in cache even after download");
        return -1; //TODO: better error
    }
    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    // fd_client = open(fileUtil.getAbsolutePath(path), fi->flags);

    DLOG("File Descriptor: %d\n", fd_client);
    //read from file on cache
    ret = pread(fd_client, buf, size, offset);
    if (ret < 0) {
        DLOG("read: failed to read from cache due to error: %d\n", errno);
        return -errno;
    }

    return ret;
}

int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_write called for '%s'", path);

    int ret = 0;

    FileData* clientFileData = fileUtil.getClientFileData(path);
    if (clientFileData == nullptr) {
        DLOG("write: cannot find file in cache even after download");
        return -1; //TODO: better error
    }
    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    //write to file on cache
    ret = pwrite(fd_client, buf, size, offset);
    if (ret < 0) {
        DLOG("write: failed to write to cache due to error: %d\n", errno);
        return -errno;
    }

    if (!isFresh(fileUtil, path, fi)) {
        ret = upload_file(fileUtil, path, fi);
        if (ret < 0) {
            DLOG("write: upload failed");
            return -1; //TODO: better error
        }
    } else DLOG("write: its fresh");

    return ret;
}

int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize) {
    DLOG("watdfs_cli_truncate called for '%s'", path);

    FileData* clientFileData = fileUtil.getClientFileData(path);
    const bool isOpen = (clientFileData != nullptr);
    AccessType accessType = NONE;
    if(isOpen) accessType = processAccessType(clientFileData->flags);

    RAII<struct fuse_file_info> fi;

    if (!isOpen) {
        fi->flags = O_RDWR;
        download_file(fileUtil, path, fi.ptr);
        clientFileData = fileUtil.getClientFileData(path);
        if (clientFileData == nullptr) {
            DLOG("truncate: cannot find file in cache after download");
            return -1; //TODO: better error
        }
    } else if (READ==accessType) { DLOG("File is open in read mode"); return -EMFILE; }

    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    int ret = 0;
    ret = ftruncate(fd_client, newsize);
    if (ret < 0) {
        DLOG("truncate failed on cache file with error: %d\n", errno);
        return -errno;
    }

    if (!isOpen || !isFresh(fileUtil, path, fi.ptr)) {
        ret = upload_file(fileUtil, path, fi.ptr, !isOpen);
        if (ret < 0) {
            DLOG("truncate: upload failed");
            return -1; //TODO: better error
        }
    }

    return 0;
}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    DLOG("watdfs_cli_fsync called for '%s'", path);

    
    FileData* clientFileData = fileUtil.getClientFileData(path);
    if (clientFileData == nullptr) {
        DLOG("fsync called on a closed file");
        return -10;
    }

    AccessType accessType = processAccessType(clientFileData->flags);
    if (accessType == READ) {
        DLOG("fsync called on read only file");
        return -1;
    }

    int ret = 0;
    ret = upload_file(fileUtil, path, fi);
    return ret;
}

// CHANGE METADATA
int watdfs_cli_utimens(void *userdata, const char *path, const struct timespec ts[2]) {
    FileData* clientFileData = fileUtil.getClientFileData(path);
    const bool isOpen = (clientFileData != nullptr);
    AccessType accessType = NONE;
    if(isOpen) accessType = processAccessType(clientFileData->flags);

    RAII<struct fuse_file_info> fi;

    if (!isOpen) {
        fi->flags = O_RDWR;
        download_file(fileUtil, path, fi.ptr);
        clientFileData = fileUtil.getClientFileData(path);
        if (clientFileData == nullptr) {
            DLOG("utimens: cannot find file in cache after download");
            return -1; //TODO: better error
        }
    } else if (READ==accessType) { DLOG("File is open in read mode"); return -EMFILE; }

    int fd_client = clientFileData->fh;
    DLOG("File Descriptor: %d\n", fd_client);

    int ret = 0;
    ret = futimens(fd_client, ts);
    if (ret < 0) {
        DLOG("utimens failed on cache file with error: %d\n", errno);
        return -errno;
    }

    if (!isOpen || !isFresh(fileUtil, path, fi.ptr)) {
        ret = upload_file(fileUtil, path, fi.ptr, !isOpen);
        if (ret < 0) {
            DLOG("utimens: upload failed");
            return -1; //TODO: better error
        }
    }

    return 0;
}
