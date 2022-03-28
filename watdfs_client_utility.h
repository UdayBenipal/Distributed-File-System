#ifndef WATDFS_CLIENT_UTILITY_H
#define WATDFS_CLIENT_UTILITY_H
#include "utility.h"

int getattr_on_server(const char *path, struct stat *statbuf);

int open_on_server(const char *path, struct fuse_file_info *fi);

int close_on_server(const char *path, struct fuse_file_info *fi);

int download_file(FileUtil& fileUtil, const char *path, struct fuse_file_info *fi);

int upload_file(FileUtil& fileUtil, const char *path, struct fuse_file_info *fi);

bool isFresh(FileUtil& fileUtil, const char *path, struct fuse_file_info *fi);

#endif
