#include "watdfs_server.h"
#include "rpc.h"
#include "utility.h"
#include "lock_server.h"
#include "debug.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <fuse.h>
#include <cstring>

// Important: the server needs to handle multiple concurrent client requests.
// You have to be carefuly in handling global variables, esp. for updating them.
// Hint: use locks before you update any global variable.

struct RegisterError { 
    int code;
    RegisterError(int code) : code(code) {} 
};

FileUtil fileUtil;
void set_server_persist_dir(char *dir) { fileUtil.setDir(dir); }

///////////////////////////////////////////////////////////////////////////////////////////////////

int watdfs_getattr(int *argTypes, void **args) {
    const char* short_path = (const char*)args[0]; //the path relative to the mountpoint
    const char* full_path = fileUtil.getAbsolutePath(short_path);

    struct stat *statbuf = (struct stat *)args[1]; //stat structure

    int *ret = (int *)args[2]; //return code, which should be set be 0 or -errno.
    *ret = 0; // initially set the return code to be 0.

    int sys_ret = 0; // sys_ret the return code from the stat system call
    sys_ret = stat(full_path, statbuf);
    if (sys_ret < 0) *ret = -errno;

    DLOG("Returning code: %d", *ret);
    return 0;
}

void watdfs_getattr_register() {
    int argTypes[4];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); // path
    argTypes[1] = argTypeFrmtr(no, yes, yes, ARG_CHAR, 1); // statbuf
    argTypes[2] = argTypeFrmtr(no, yes, no, ARG_INT); // retcode
    argTypes[3] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"getattr", argTypes, watdfs_getattr);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int watdfs_mknod(int *argTypes, void **args) {
    const char* short_path = (const char*)args[0];
    const char* full_path = fileUtil.getAbsolutePath(short_path);

    mode_t* mode = (mode_t *)args[1];

    dev_t* dev = (dev_t *)args[2];

    int *ret = (int *)args[3];
    *ret = 0;

    int sys_ret = 0;
    sys_ret = mknod(full_path, *mode, *dev);
    if (sys_ret < 0) *ret = -errno;

    DLOG("Returning code: %d", *ret);
    return 0;
}

void watdfs_mknod_register() {
    int argTypes[5];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(yes, no, no, ARG_INT); //mode
    argTypes[2] = argTypeFrmtr(yes, no, no, ARG_LONG); //dev
    argTypes[3] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[4] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"mknod", argTypes, watdfs_mknod);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int watdfs_open(int *argTypes, void **args) {
    const char* short_path = (const char*)args[0];
    const char* full_path = fileUtil.getAbsolutePath(short_path);

    struct fuse_file_info *fi = (struct fuse_file_info *)args[1];

    int *ret = (int *)args[2];
    *ret = 0;

    AccessType accessType = processAccessType(fi->flags);
    if (accessType == WRITE && fileUtil.serverFilePresent(short_path)) {
        *ret = -EACCES;
        DLOG("File already opended in write mode: %d", *ret);
        return 0;
    }

    int sys_ret = 0;
    sys_ret = open(full_path, fi->flags);
    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        fi->fh = sys_ret;
        if (accessType == WRITE) fileUtil.addServerFile(short_path);
    }

    DLOG("Returning code: %d", *ret);
    return 0;
}

void watdfs_open_register() {
    int argTypes[4];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(yes, yes, yes, ARG_CHAR, 1); //fi
    argTypes[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[3] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"open", argTypes, watdfs_open);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int watdfs_release(int *argTypes, void **args) {
    const char* short_path = (const char*)args[0]; //path

    struct fuse_file_info *fi = (struct fuse_file_info *)args[1]; //fi

    int *ret = (int *)args[2];
    *ret = 0;

    int sys_ret = 0;
    sys_ret = close(fi->fh);
    if (sys_ret < 0) {
        *ret = -errno;
    } else {
        AccessType accessType = processAccessType(fi->flags);
        if (accessType == WRITE) fileUtil.removeFile(short_path);
    }

    DLOG("Returning code: %d", *ret);
    return 0;
}

void watdfs_release_register() {
    int argTypes[4];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //fi
    argTypes[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[3] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"release", argTypes, watdfs_release);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int watdfs_read(int *argTypes, void **args) {
    void *buf = args[1];

    size_t *size = (size_t *)args[2];

    off_t *offset = (off_t *)args[3];

    struct fuse_file_info *fi = (struct fuse_file_info *)args[4];

    int *ret = (int *)args[5];

    int sys_ret = 0;
    sys_ret = pread(fi->fh, buf, *size, *offset);
    if (sys_ret < 0) *ret = -errno; else *ret = sys_ret; //update the bytes read

    DLOG("Returning code: %d", *ret);
    return 0;
}

void watdfs_read_register() {
    int argTypes[7];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(no, yes, yes, ARG_CHAR, 1); //buf
    argTypes[2] = argTypeFrmtr(yes, no, no, ARG_LONG); //size
    argTypes[3] = argTypeFrmtr(yes, no, no, ARG_LONG); //offset
    argTypes[4] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //fi
    argTypes[5] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[6] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"read", argTypes, watdfs_read);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int watdfs_write(int *argTypes, void **args) {
    void *buf = args[1];

    size_t *size = (size_t *)args[2];

    off_t *offset = (off_t *)args[3];

    struct fuse_file_info *fi = (struct fuse_file_info *)args[4];

    int *ret = (int *)args[5];

    int sys_ret = 0;
    sys_ret = pwrite(fi->fh, buf, *size, *offset);
    if (sys_ret < 0) *ret = -errno; else *ret = sys_ret; //update the bytes read

    DLOG("Returning code: %d", *ret);
    return 0;
}

void watdfs_write_register() {
    int argTypes[7];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //buf
    argTypes[2] = argTypeFrmtr(yes, no, no, ARG_LONG); //size
    argTypes[3] = argTypeFrmtr(yes, no, no, ARG_LONG); //offset
    argTypes[4] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //fi
    argTypes[5] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[6] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"write", argTypes, watdfs_write);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int watdfs_truncate(int *argTypes, void **args) {
    const char* short_path = (const char*)args[0];
    const char* full_path = fileUtil.getAbsolutePath(short_path);

    off_t *newsize = (off_t *)args[1];

    int *ret = (int *)args[2];
    *ret = 0;

    int sys_ret = 0;
    sys_ret = truncate(full_path, *newsize);
    if (sys_ret < 0) *ret = -errno;

    DLOG("Returning code: %d", *ret);
    return 0;
}

void watdfs_truncate_register() {
    int argTypes[4];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(yes, no, no, ARG_LONG); //newsize
    argTypes[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[3] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"truncate", argTypes, watdfs_truncate);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int watdfs_fsync(int *argTypes, void **args) {
    struct fuse_file_info *fi = (struct fuse_file_info *)args[1];

    int *ret = (int *)args[2];
    *ret = 0;

    int sys_ret = 0;
    sys_ret = fsync(fi->fh);
    if (sys_ret < 0) *ret = -errno;

    DLOG("Returning code: %d", *ret);
    return 0;
}

void watdfs_fsync_register() {
    int argTypes[4];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //fi
    argTypes[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[3] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"fsync", argTypes, watdfs_fsync);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int watdfs_utimens(int *argTypes, void **args) {
    const char* short_path = (const char*)args[0];
    const char* full_path = fileUtil.getAbsolutePath(short_path);

    struct timespec *ts = (struct timespec *)args[1];

    int *ret = (int *)args[2];
    *ret = 0;

    int sys_ret = 0;
    sys_ret = utimensat(-1, full_path, ts, 0);
    if (sys_ret < 0) *ret = -errno;

    DLOG("Returning code: %d", *ret);
    return 0;
}

void watdfs_utimens_register() {
    int argTypes[4];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //ts
    argTypes[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[3] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"utimens", argTypes, watdfs_utimens);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int rpc_watdfs_server_register() {
    int ret_code = 0;

    try {
        watdfs_getattr_register();
        watdfs_mknod_register();
        watdfs_open_register();
        watdfs_release_register();
        watdfs_read_register();
        watdfs_write_register();
        watdfs_truncate_register();
        watdfs_fsync_register();
        watdfs_utimens_register();
    } 
    catch ( RegisterError& err) { ret_code = err.code; }

    return ret_code;
}
