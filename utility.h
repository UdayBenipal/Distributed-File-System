#ifndef UTILITY_H
#define UTILITY_H
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <time.h>

#include "debug.h"

template <class T> struct RAII {
    T *ptr;
    RAII() { ptr = (T *)malloc(sizeof(T)); }
    RAII(T val) { ptr = (T *)malloc(sizeof(T)); *ptr = val; }
    T& operator*() { return *ptr; }
    T* operator->() { return ptr; }
    ~RAII() { free((void *)ptr); }
};

enum AccessType { NONE, READ, WRITE };
AccessType processAccessType(int flags);

struct FileData {
    int fh;
    int server_fh;
    AccessType accessType;
    int flags;
    time_t tc;

    FileData(int fh, int server_fh, AccessType accessType, int flags, time_t tc): 
        fh(fh), server_fh(server_fh), accessType(accessType), flags(flags), tc(tc) {}
};

#define yes true
#define no false
int argTypeFrmtr(bool input, bool output, bool array, unsigned int type, unsigned int length = 0);

class FileUtil {
    const char *curr_dir;

    std::mutex mtx; 
    std::unordered_map<std::string, FileData*> map;

    std::unordered_set<std::string> set;

  public:
    time_t cacheInterval;

    void setDir(const char *curr_dir);
    const char* getAbsolutePath(const char* file_path);
	
    void addClientFileData(const char* file, int fh, int server_fh, int flags);
    void updateTc(const char* file);
    FileData* getClientFileData(const char* file);

    void addServerFile(const char* file);
    bool serverFilePresent(const char* file);

    void removeFile(const char* file);

    ~FileUtil() { for(auto& it: map) { delete it.second; } }
};

#endif
