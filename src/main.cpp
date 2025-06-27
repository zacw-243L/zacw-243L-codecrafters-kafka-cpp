#include <bits/types/struct_iovec.h>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <ostream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>
struct Message
{
    int32_t size;
    int16_t request_api_key;
    int16_t request_api_version;
    int32_t correlation_id;
    std::unique_ptr<char[]> buf;
    ssize_t payload_size;

    Message(int32_t client_fd)
    {
        int32_t net_size;
        int16_t net_api_key, net_api_version;
        int32_t net_correlation_id;
        if (recv(client_fd, &net_size, 4, 0) != 4)
            throw std::runtime_error("size read failed");
        if (recv(client_fd, &net_api_key, 2, 0) != 2)
            throw std::runtime_error("api_key read failed");
        if (recv(client_fd, &net_api_version, 2, 0) != 2)
            throw std::runtime_error("api_version read failed");
        if (recv(client_fd, &net_correlation_id, 4, 0) != 4)
            throw std::runtime_error("correlation_id read failed");
        size = ntohl(net_size);
        request_api_key = ntohs(net_api_key);
        request_api_version = ntohs(net_api_version);
        correlation_id = ntohl(net_correlation_id);
        payload_size = size - 12; // 4+2+2+4=12
        if (payload_size < 0)
            throw std::runtime_error("negative payload size");
        buf = std::make_unique<char[]>(payload_size + 1);
        ssize_t bytes_read = recv(client_fd, buf.get(), payload_size, 0);
        if (bytes_read != payload_size)
            throw std::runtime_error("invalid payload size");
        buf[payload_size] = '\0';
    }
};
int16_t bit16htonl(uint16_t num)
{
    return (num >> 8) + (num << 8);
}
class Socket
{
    int32_t server_fd;
    bool client_connected = false;
    int32_t client_fd;

public:
    Socket()
    {
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd < 0)
        {
            std::cerr << "Failed to create server socket: " << std::endl;
            exit(1);
        }
        int32_t reuse = 1;
        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
        {
            close(server_fd);
            std::cerr << "setsockopt failed: " << std::endl;
            exit(1);
        }
    }
    int32_t bindToPort(int32_t port)
    {
        struct sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(port);
        if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr), sizeof(server_addr)) != 0)
        {
            close(server_fd);
            std::cerr << "Failed to bind to port " << port << std::endl;
            return 1;
        }
        return 0;
    }
    int32_t ListenForConnection()
    {
        int32_t connection_backlog = 5;
        if (listen(server_fd, connection_backlog) != 0)
        {
            close(server_fd);
            std::cerr << "listen failed" << std::endl;
            return 1;
        }

        std::cout << "Waiting for a client to connect...\n";
        return 0;
    }
    int32_t AcceptConnection()
    {
        struct sockaddr_in client_addr{};
        socklen_t client_addr_len = sizeof(client_addr);
        client_fd = accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
        client_connected = true;
        std::cout << "Client connected\n";
        return 0;
    }
    Message RecieveV0()
    {
        Message msg(client_fd);
        return msg;
    }

    int32_t RespondV0(int32_t correlation_id)
    {
        int32_t size = 0;
        int16_t error_code = 35;
        int32_t net_size = htonl(size);
        int32_t net_correlation_id = htonl(correlation_id);
        int16_t net_error_code = bit16htonl(error_code);
        send(client_fd, &net_size, 4, 0);
        send(client_fd, &net_correlation_id, 4, 0);
        send(client_fd, &net_error_code, 2, 0);
        return 0;
    }
    ~Socket()
    {
        shutdown(client_fd, SHUT_WR);
        if (client_connected)
            close(client_fd);
        close(server_fd);
    }
};
int main(int argc, char *argv[])
{
    Socket s;
    s.bindToPort(9092);
    s.ListenForConnection();
    s.AcceptConnection();
    Message m = s.RecieveV0();
    s.RespondV0(m.correlation_id);
    return 0;
}