#include <bits/types/struct_iovec.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <ostream>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>
class Socket {
    int server_fd;
    bool client_connected=false;
    int client_fd;
public:
    Socket() {
        server_fd = socket(AF_INET, SOCK_STREAM, 0);    
        if (server_fd < 0) {
            std::cerr << "Failed to create server socket: " << std::endl;
            exit(1);
        }
        int reuse = 1;
        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
            close(server_fd);
            std::cerr << "setsockopt failed: " << std::endl;
            exit(1);
        }
    }
    int bindToPort(int port) {
        struct sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(port);
        if (bind(server_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
            close(server_fd);
            std::cerr << "Failed to bind to port "<<port<< std::endl;
            return 1;
        }
        return 0;
    }
    int ListenForConnection() {
        int connection_backlog = 5;
        if (listen(server_fd, connection_backlog) != 0) {
            close(server_fd);
            std::cerr << "listen failed" << std::endl;
            return 1;
        }

        std::cout << "Waiting for a client to connect...\n";
        return 0;
    }
    int AcceptConnection() {
        struct sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);
        client_fd = accept(server_fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_addr_len);
        client_connected=true;
        std::cout << "Client connected\n";
        return 0;
    }
    int RecieveRequest(int size) {
        auto buf=std::make_unique<char[]>(size+1);
        size=read(client_fd,buf.get(),size);
        if (size<0) {
            std::cout<<"read failed..."<<std::endl;
            return 1;
        }
        buf[size]=0;
        std::cout<<"client "<<client_fd<<"> "<<buf.get()<<std::endl;
        return 0;
    }
    int RespondV0(int size,int correlation_id) {
        size=htonl(size);
        correlation_id=htonl(correlation_id);
        write(client_fd,&size,4);
        write(client_fd,&correlation_id,4);
        return 0;
    }
    ~Socket() {
        if (client_connected) close(client_fd);
        close(server_fd);
    }
};
int main(int argc, char* argv[]) {
    Socket s;
    s.bindToPort(9092);
    s.ListenForConnection();
    s.AcceptConnection();
    s.RecieveRequest(100);
    s.RespondV0(0,7);
    return 0;
}