# Distributed File System

Supports creating, opening, reading, writing and closing files on local or remote machines, it has been developed using upload-download model following the client server architecture, where client and server interact through RPC protocol. Client downloads and locally caches a copy of the file from the server, then performs file operations on the cached copy and uploads the file copy back to the server when the client is done using it. Uses a timeout-based caching scheme, in this timeoutbased caching scheme, a file is periodically re-downloaded from the server, or re-uploaded to the server, when a freshness condition no longer holds. Uses locks to provide mutually exclusive write access of a file to the client.



## Usage

#### Server
1. Run `make watdfs_server` command to generate `watdfs_server`
2. Run: `./watdfs_server path_to_remote_directory`<br/>
   This will give servers address and port it is listening on

#### Client
1. Run `make watdfs_client` command to generate `watdfs_client`
2. Store environment variables `SERVER_ADDRESS`, `SERVER_PORT` and `CACHE_INTERVAL_SEC`
3. Run: `./watdfs_client -s -f -o direct_io path_to_cache_directory path_to_mouting_directory`
