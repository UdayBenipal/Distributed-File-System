#ifndef UTILITY_H
#define UTILITY_H
#include <cstdlib>

template <class T> struct RAII {
    T *ptr;
    RAII() { ptr = (T *)malloc(sizeof(T)); }
    RAII(T *ptr): ptr(ptr) {}
    RAII(T val) { ptr = (T *)malloc(sizeof(T)); *ptr = val; }
    T& operator*() { return *ptr; }
    T* operator->() { return ptr; }
    ~RAII() { free((void *)ptr); }
};

class FileUtil {
    const char *curr_dir;
  public:
    void setDir(const char *curr_dir);
    const char* getAbsolutePath(const char* file_path);
};

#define yes true
#define no false
int argTypeFrmtr(bool input, bool output, bool array, unsigned int type, unsigned int length = 0);

#endif
