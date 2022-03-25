#ifndef WATDFS_CLIENT_UTILITY_H
#define WATDFS_CLIENT_UTILITY_H

void set_path_to_cache(const char *path_to_cache);

int download_file(const char *path, struct fuse_file_info *fi);

#endif
