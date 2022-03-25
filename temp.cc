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
#include "temp.h"

#include "debug.h"


#define yes true
#define no false

int frmtr(bool input, bool output, bool array, unsigned int type, unsigned int length = 0) {
    // assert(!array || (array && length>0));
    unsigned int code = 0;
    if (input) code = 1u << ARG_INPUT;
    if (output) code |= (1u << ARG_OUTPUT);
    if (array) code |= ((1u << ARG_ARRAY) | length);
    code |= (type << 16u);
    return code;
}

const char *path_to_cache_global = nullptr;

void set_path_to_cache(const char *path_to_cache) { path_to_cache_global = path_to_cache; }

char *get_full_path(const char *short_path) {
    int short_path_len = strlen(short_path);
    int dir_len = strlen(path_to_cache_global);
    int full_len = dir_len + short_path_len + 1;

    char *full_path = (char *)malloc(full_len);

    // First fill in the directory.
    strcpy(full_path, path_to_cache_global);
    // Then append the path.
    strcat(full_path, short_path);
    DLOG("Full path: %s\n", full_path);

    return full_path;
}

int getattr_from_server(const char *path, struct stat *statbuf) {
    DLOG("download getattr called for '%s'", path);
    
    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = frmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = frmtr(no, yes, yes, ARG_CHAR, (uint) sizeof(struct stat)); //statbuf
    args[1] = (void *)statbuf;

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[2] = frmtr(no, yes, no, ARG_INT); //retcode
    args[2] = (void *)ret;

    arg_types[3] = 0;

    int rpc_ret = rpcCall((char *)"getattr", arg_types, args);

    int fxn_ret = 0;
    if (rpc_ret < 0) {
        DLOG("getattr rpc failed with error '%d'", rpc_ret);
        fxn_ret = -EINVAL;
    } else fxn_ret = *ret;

    if (fxn_ret < 0) memset(statbuf, 0, sizeof(struct stat));

    free(ret);
    delete []args;

    return fxn_ret;
}


int open_from_server(const char *path, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_open called for '%s'", path);

    int ARG_COUNT = 3;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = frmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = frmtr(yes, yes, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[1] = (void *)(fi);

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[2] = frmtr(no, yes, no, ARG_INT); //retcode
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


int read_from_server(const char *path, char *buf, size_t size, struct fuse_file_info *fi) {
    DLOG("watdfs_cli_read called for '%s'", path);

    int ARG_COUNT = 6;
    void **args = new void*[ARG_COUNT];
    int arg_types[ARG_COUNT + 1];

    int pathlen = strlen(path) + 1;
    arg_types[0] = frmtr(yes, no, yes, ARG_CHAR, (uint) pathlen); //path
    args[0] = (void *)path;

    arg_types[1] = frmtr(no, yes, yes, ARG_CHAR, MAX_ARRAY_LEN); //buf
    args[1] = (void *)buf;

    size_t *m_size = (size_t *)malloc(sizeof(size_t)); *m_size = MAX_ARRAY_LEN;
    arg_types[2] = frmtr(yes, no, no, ARG_LONG); //size
    args[2] = (void *)m_size;

    off_t *offset = (off_t *)malloc(sizeof(off_t)); *offset = 0;
    arg_types[3] = frmtr(yes, no, no, ARG_LONG); //offset
    args[3] = (void *)offset;

    arg_types[4] = frmtr(yes, no, yes, ARG_CHAR, (uint) sizeof(struct fuse_file_info)); //fi
    args[4] = (void *)(fi);

    int *ret = (int *)malloc(sizeof(int)); *ret = 0;
    arg_types[5] = frmtr(no, yes, no, ARG_INT); //retcode
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

        arg_types[1] = frmtr(no, yes, yes, ARG_CHAR, size); //buf
        *m_size = size;

        int rpc_ret = rpcCall((char *)"read", arg_types, args);
        if (rpc_ret < 0) { DLOG("read rpc failed with error '%d'", rpc_ret); fxn_ret = -EINVAL; }
        else if (*ret < 0) fxn_ret = *ret;
        else fxn_ret += *ret;
    }
    catch (...) {}

    free(ret);
    free(m_size);
    free(offset);
    delete []args;

    return fxn_ret;
}


int download_file(const char *path) {
    char *full_path = get_full_path(path);

    DLOG("Download file: %s\n", full_path);

    //truncate file if it exists on the client
    int ret = 0;
    ret = open(full_path, O_CREAT|O_WRONLY|O_TRUNC);
    int fd = ret;
    DLOG("File Descriptor: %d\n", fd);
    DLOG("Error: %d\n", errno);

    //getattr of file from server
    struct stat *statbuf = (struct stat *)malloc(sizeof(struct stat));
    ret = getattr_from_server(path, statbuf);

    DLOG("Size: %ld\n", statbuf->st_size);

    //read file from server
    char *buf = new char[statbuf->st_size];
    struct fuse_file_info* fi = (struct fuse_file_info *)malloc(sizeof(struct fuse_file_info));
    //update fi->flags accordingly
    fi->flags = O_RDONLY;
    ret = open_from_server(path, fi);

    DLOG("open ret: %d\n", ret);
    DLOG("File D: %ld\n", fi->fh);

    ret = read_from_server(path, buf, statbuf->st_size, fi);

    DLOG("read ret: %d\n", ret);
    DLOG("Buffer: %s\n", buf);

    //write the file on client
    ret = write(fd, buf, statbuf->st_size);

    //update file metadata on the client
    

    close(fd);

    free(full_path);
    free(statbuf);
    free(fi);
    delete []buf;

    return 0;
}
