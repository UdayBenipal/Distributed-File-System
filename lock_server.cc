#include "watdfs_server.h"
#include "rpc.h"
#include "utility.h"
#include "rw_lock.h"
#include "debug.h"

#include <string>
#include <cstdlib>
#include <mutex>
#include <unordered_map>

struct RegisterError { 
    int code;
    RegisterError(int code) : code(code) {} 
};

////////////////////////////////////////////helper//////////////////////////////////////////////////

class LockUtil {
    std::mutex mtx; 
    std::unordered_map<std::string, rw_lock_t*> map;

  public:
    int accuqire(const char *path, rw_lock_mode_t mode) {
        std::string key(path);

        mtx.lock();

        int ret = 0;
        rw_lock_t* lock = nullptr;

        if (map.find(key) == map.end()) {
            lock = (rw_lock_t*)malloc(sizeof(rw_lock_t));
            ret = rw_lock_init(lock);
            if(ret < 0) {
                DLOG("unable to init the lock for %s\n", path);
                mtx.unlock();
                return ret;
            }
            map[key] = lock;
        } else {
            lock = map.at(key);
        }

        ret = rw_lock_lock(lock, mode);
        if(ret < 0) {
            DLOG("unable to lock the lock for %s\n", path);
            mtx.unlock();
            return ret;
        }

        mtx.unlock();
        return 0;
    }

    int release(const char *path, rw_lock_mode_t mode) {
        std::string key(path);

        mtx.lock();

        int ret = 0;

        if (map.find(key) == map.end()) {
            DLOG("lock not found to unlock for %s\n", path);
            mtx.unlock();
            return -1;
        }

        rw_lock_t* lock = map.at(key); 
        ret = rw_lock_unlock(lock, mode);
        if(ret < 0) {
            DLOG("unable to unlock the lock for %s\n", path);
            mtx.unlock();
            return ret;
        }

        if (lock->num_readers_ == 0 && lock->num_writers_ == 0) {
            ret = rw_lock_destroy(lock);
            if(ret < 0) {
                DLOG("unable to destroy lock for %s\n", path);
                mtx.unlock();
                return ret;
            }
            free(lock);
            map.erase(key);
        }

        mtx.unlock();
        return 0;
    }

    ~LockUtil() {
        for (auto& it: map) { rw_lock_destroy(it.second); free(it.second); }
    }
} util;

///////////////////////////////////////////////////////////////////////////////////////////////////

int lock(int *argTypes, void **args) {
    const char* path = (const char*)args[0];

    rw_lock_mode_t* mode = (rw_lock_mode_t*)args[1];

    int* ret = (int*)args[2];

    *ret = util.accuqire(path, *mode);

    DLOG("Returning code: %d", *ret);
    return 0;
}

void lock_register() {
    int argTypes[4];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(yes, no, no, ARG_INT); //mode
    argTypes[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[3] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"lock", argTypes, lock);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int unlock(int *argTypes, void **args) {
    const char* path = (const char*)args[0];

    rw_lock_mode_t* mode = (rw_lock_mode_t*)args[1];

    int* ret = (int*)args[2];

    *ret = util.release(path, *mode);

    DLOG("Returning code: %d", *ret);
    return 0;
}

void unlock_register() {
    int argTypes[4];
    argTypes[0] = argTypeFrmtr(yes, no, yes, ARG_CHAR, 1); //path
    argTypes[1] = argTypeFrmtr(yes, no, no, ARG_INT); //mode
    argTypes[2] = argTypeFrmtr(no, yes, no, ARG_INT); //retcode
    argTypes[3] = 0; // the null terminator

    int ret = 0;
    ret = rpcRegister((char *)"unlock", argTypes, unlock);
    if (ret < 0) throw RegisterError(ret);
}

///////////////////////////////////////////////////////////////////////////////////////////////////

int rpc_lock_server_register() {
    int ret_code = 0;

    try {
        lock_register();
        unlock_register();
    } 
    catch ( RegisterError& err) { ret_code = err.code; }

    return ret_code;
}
