#ifndef UTILITY_H
#define UTILITY_H
#include <cstdlib>
#include <mutex>
#include <unordered_map>

template <class T> struct RAII {
    T *ptr;
    RAII() { ptr = (T *)malloc(sizeof(T)); }
    RAII(T *ptr): ptr(ptr) {}
    RAII(T val) { ptr = (T *)malloc(sizeof(T)); *ptr = val; }
    T& operator*() { return *ptr; }
    T* operator->() { return ptr; }
    ~RAII() { free((void *)ptr); }
};

enum AccessType { NONE, READ, WRITE };
AccessType processAccessType(int flags);

class FileUtil {
    const char *curr_dir;

    std::mutex mtx; 
    std::unordered_map<std::string, AccessType> map;

  public:
    void setDir(const char *curr_dir);
    const char* getAbsolutePath(const char* file_path);

    bool openForWrite(const char* file);
	bool isOpen(const char* file);
    void updateAccessType(const char* file, AccessType accessType);
	void removeFile(const char* file);
};

#define yes true
#define no false
int argTypeFrmtr(bool input, bool output, bool array, unsigned int type, unsigned int length = 0);

#endif
