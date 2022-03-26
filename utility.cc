#include <assert.h>
#include <cstring>
#include <fcntl.h>
#include "rpc.h"
#include "utility.h"

#include "debug.h"

AccessType processAccessType(int flags) {
    int mode = flags & O_ACCMODE;
    if (mode == O_RDONLY) return READ;
    return WRITE;
}

int argTypeFrmtr(bool input, bool output, bool array, unsigned int type, unsigned int length) {
    assert(!array || (array && length>0));
    unsigned int code = 0;
    if (input) code = 1u << ARG_INPUT;
    if (output) code |= (1u << ARG_OUTPUT);
    if (array) code |= ((1u << ARG_ARRAY) | length);
    code |= (type << 16u);
    return code;
}

void FileUtil::setDir(const char *curr_dir) {
    FileUtil::curr_dir = curr_dir;
}

const char* FileUtil::getAbsolutePath(const char* file_path) {
    int file_path_len = strlen(file_path);
    int dir_len = strlen(curr_dir);
    int full_len = dir_len + file_path_len + 1;

    char *full_path = (char *)malloc(full_len);

    // First fill in the directory.
    strcpy(full_path, curr_dir);
    // Then append the path.
    strcat(full_path, file_path);

    return full_path;
}

void FileUtil::addClientFileData(const char* file, int fh, struct fuse_file_info *fi) {
    DLOG("addClientFileData for %s", file);
    std::string key(file);

    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);

    mtx.lock();
    map[key] = new FileData(fh, t.tv_sec, fi);
    mtx.unlock();
}

void FileUtil::updateTc(const char* file) {
    DLOG("updateTc for %s", file);
    std::string key(file);

    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);

    mtx.lock();
    if (map.find(key) != map.end()) map.at(key)->tc = t.tv_sec;
    mtx.unlock();
}

FileData* FileUtil::getClientFileData(const char* file) {
    DLOG("getClientFileData for %s", file);
    std::string key(file);
    FileData *fileData = nullptr;

    mtx.lock();
    if (map.find(key) != map.end()) fileData = map.at(key);
    mtx.unlock();

    return fileData;
}

void FileUtil::addServerFile(const char* file) {
    std::string key(file);
    mtx.lock();
    set.emplace(key);
    mtx.unlock();
}

bool FileUtil::serverFilePresent(const char* file) {
    std::string key(file);
    mtx.lock();
    bool isPresent = (set.find(key) != set.end());
    mtx.unlock();
    return isPresent;
}

void FileUtil::removeFile(const char *file) {
    std::string key(file);

    mtx.lock();
    map.erase(key);
    set.erase(key);
    mtx.unlock();
}

