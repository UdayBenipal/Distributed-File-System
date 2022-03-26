#ifndef WATDFS_CLIENT_UTILITY_H
#define WATDFS_CLIENT_UTILITY_H
#include "utility.h"

int download_file(FileUtil& fileUtil, const char *path, struct fuse_file_info *fi);

int upload_file(FileUtil& fileUtil, const char *path, struct fuse_file_info *fi, bool closeFiles = false);

bool isFresh(FileUtil& fileUtil, const char *path, struct fuse_file_info *fi);

#endif
