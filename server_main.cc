#include "rpc.h"
#include "watdfs_server.h"
#include "debug.h"

# ifdef PRINT_ERR
#include <iostream>
#endif

INIT_LOG

// The main function of the server.
int main(int argc, char *argv[]) {

    // argv[1] should contain the directory where you should store data on the server
    if (argc != 2) {
#ifdef PRINT_ERR
        std::cerr << "Usage:" << argv[0] << " server_persist_dir";
#endif
        return -1;
    }

    // Store the directory in a global variable.
    set_server_persist_dir(argv[1]);

    DLOG("Initializing server...");

    int ret = 0;

    // Initializing the rpc library by calling `rpcServerInit`
    ret = rpcServerInit();
    if (ret < 0) { DLOG("RPC SERVER COULD NOT BE INITIALIZED"); return ret; }

    // register functions with the RPC library
    ret = rpc_watdfs_server_register();
    if (ret < 0) { DLOG("WATDFS SERVER FUNCTIONS COULD NOT BE REGISTERED FOR RPC"); return ret; }

    ret = rpcExecute();
    if (ret < 0) { DLOG("RPC EXECUTE FAILED"); return ret; }

    return 0; // if we reach here server rpc was initialized without any problem
}
