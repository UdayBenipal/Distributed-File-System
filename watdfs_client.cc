#include "watdfs_client.h"
#include "rpc.h"
#include "argTypeFormatter.h"

#include "debug.h"
INIT_LOG

# ifdef PRINT_ERR
#include <iostream>
#endif

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
    
    int ARG_COUNT = 3; // getattr has 3 arguments
    void **args = new void*[ARG_COUNT]; // Allocate space for the output arguments
    int arg_types[ARG_COUNT + 1]; // Allocate the space for arg types

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(no, yes, yes, ARG_CHAR, (uint) sizeof(struct stat)); //statbuf
    args[1] = (void *)statbuf;

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret;

    arg_types[3] = 0;

    // MAKE THE RPC CALL
    int rpc_ret = rpcCall((char *)"getattr", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("getattr rpc failed with error '%d'", rpc_ret);
        // Something went wrong with the rpcCall, return a sensible return
        // value. In this case lets return, -EINVAL
        fxn_ret = -EINVAL;
    } else {
        // Our RPC call succeeded. However, it's possible that the return code
        // from the server is not 0, that is it may be -errno. Therefore, we
        // should set our function return value to the retcode from the server.
        fxn_ret = *ret;
    }

    // If the return code of watdfs_cli_getattr is negative (an error), then 
    // we need to make sure that the stat structure is filled with 0s. Otherwise,
    // FUSE will be confused by the contradicting return values.
    if (fxn_ret < 0) memset(statbuf, 0, sizeof(struct stat));

    // Clean up the memory we have allocated.
    free(ret);
    delete []args;

    // Finally return the value we got from the server.
    return fxn_ret;
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

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[3] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[3] = (void *)ret;

    arg_types[4] = 0;

    int rpc_ret = rpcCall((char *)"mknod", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("mknod rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    free(ret);
    delete []args;

    return fxn_ret;
}

int watdfs_cli_open(void *userdata, const char *path, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_open called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, yes, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[1] = (void *)(fi);

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"open", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("open rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    free(ret);
    delete []args;

    return fxn_ret;
}

int watdfs_cli_release(void *userdata, const char *path, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_release called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[1] = (void *)(fi);

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"release", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("release rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    free(ret);
    delete []args;

    return fxn_ret;
}

// READ AND WRITE DATA
int watdfs_cli_read(void *userdata, const char *path, char *buf, size_t size,
                    off_t offset, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_read called for '%s'", path);

    int ARG_COUNT = 6;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(no, yes, yes, ARG_CHAR, MAX_ARRAY_LEN); //buf
    args[1] = (void *)buf;

    size_t *m_size = (size_t *)malloc(sizeof(size_t)); *m_size = MAX_ARRAY_LEN;
    arg_types[2] = argTypeFrmtr(yes, no, no, ARG_LONG); //size
    args[2] = (void *)m_size;

    arg_types[3] = argTypeFrmtr(yes, no, no, ARG_LONG); //offset
    args[3] = (void *)&offset;

    arg_types[4] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[4] = (void *)(fi);

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[5] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[5] = (void *)ret;
    
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
            offset += MAX_ARRAY_LEN;
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

    free(ret);
    free(m_size);
    delete []args;

    return fxn_ret;
}

int watdfs_cli_write(void *userdata, const char *path, const char *buf,
                     size_t size, off_t offset, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_write called for '%s'", path);

    int ARG_COUNT = 6;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, MAX_ARRAY_LEN); //buf
    args[1] = (void *)buf;

    size_t *m_size = (size_t *)malloc(sizeof(size_t)); *m_size = MAX_ARRAY_LEN;
    arg_types[2] = argTypeFrmtr(yes, no, no, ARG_LONG); //size
    args[2] = (void *)m_size;

    arg_types[3] = argTypeFrmtr(yes, no, no, ARG_LONG); //offset
    args[3] = (void *)&offset;

    arg_types[4] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[4] = (void *)(fi);

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[5] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[5] = (void *)ret;
    
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
            offset += MAX_ARRAY_LEN;
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

    free(ret);
    free(m_size);
    delete []args;

    return fxn_ret;
}

int watdfs_cli_truncate(void *userdata, const char *path, off_t newsize) {
    DLOG("watdfs_cli_truncate called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, no, ARG_LONG); //newsize
    args[1] = (void *)(&newsize);

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"truncate", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("truncate rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    free(ret);
    delete []args;

    return fxn_ret;
}

int watdfs_cli_fsync(void *userdata, const char *path,
                     struct fuse_file_info *fi) {
    DLOG("watdfs_cli_fsync called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[1] = (void *)(fi);

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"fsync", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("fsync rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    free(ret);
    delete []args;

    return fxn_ret;
}

// CHANGE METADATA
int watdfs_cli_utimens(void *userdata, const char *path, const struct timespec ts[2]) {
    DLOG("watdfs_cli_utimens called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = argTypeFrmtr(yes, no, yes, ARG_CHAR, (uint) (sizeof(struct timespec)*2)); //ts
    args[1] = (void *)(ts);

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"utimens", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) { DLOG("utimens rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
    else fxn_ret = *ret;

    free(ret);
    delete []args;

    return fxn_ret;
}
