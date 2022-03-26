#include <assert.h>
#include <cstring>
#include <fcntl.h>
#include "rpc.h"
#include "utility.h"


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


bool FileUtil::openForWrite(const char* file) {
    std::string key(file);

    mtx.lock();

    AccessType accessType = NONE;
    if (map.find(key) != map.end()) accessType = map.at(key);

    mtx.unlock();

    return accessType==WRITE;
}

bool FileUtil::isOpen(const char* file) {
    std::string key(file);

    mtx.lock();

    bool isOpen = (map.find(key) != map.end());

    mtx.unlock();

    return isOpen;
}

void FileUtil::updateAccessType(const char *file, AccessType accessType) {
    assert(accessType != NONE);

    std::string key(file);

    mtx.lock();

    map[key] = accessType;

    mtx.unlock();
}

void FileUtil::removeFile(const char *file) {
    std::string key(file);

    mtx.lock();

    map.erase(key);

    mtx.unlock();
}


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
