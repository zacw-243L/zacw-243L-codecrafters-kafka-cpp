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

// ... [Message and bit16htonl as before] ...

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

    // Helper to write all bytes
    void writeall(const void* buf, size_t len) {
        const char* p = (const char*)buf;
        while (len > 0) {
            ssize_t n = send(client_fd, p, len, 0);
            if (n <= 0) throw std::runtime_error("send failed");
            p += n;
            len -= n;
        }
    }

    int32_t RespondApiVersionsV4(int32_t correlation_id)
    {
        // Build the response in a buffer
        std::vector<uint8_t> buf;

        // Reserve space for total size
        buf.resize(4);

        // CorrelationId
        int32_t corr = htonl(correlation_id);
        buf.insert(buf.end(), (uint8_t*)&corr, (uint8_t*)&corr + 4);

        // error_code (INT16)
        int16_t error_code = 0;
        int16_t net_error_code = htons(error_code);
        buf.insert(buf.end(), (uint8_t*)&net_error_code, (uint8_t*)&net_error_code + 2);

        // api_versions (ARRAY)
        // Array length (INT32)
        int32_t api_versions_count = htonl(1);
        buf.insert(buf.end(), (uint8_t*)&api_versions_count, (uint8_t*)&api_versions_count + 4);

        // For each api_version:
        // api_key (INT16), min_version (INT16), max_version (INT16)
        int16_t api_key = htons(18); // API_VERSIONS
        int16_t min_version = htons(0); // earliest supported, e.g. 0
        int16_t max_version = htons(4); // v4
        buf.insert(buf.end(), (uint8_t*)&api_key, (uint8_t*)&api_key + 2);
        buf.insert(buf.end(), (uint8_t*)&min_version, (uint8_t*)&min_version + 2);
        buf.insert(buf.end(), (uint8_t*)&max_version, (uint8_t*)&max_version + 2);

        // throttle_time_ms (INT32)
        int32_t throttle_time_ms = 0;
        int32_t net_throttle = htonl(throttle_time_ms);
        buf.insert(buf.end(), (uint8_t*)&net_throttle, (uint8_t*)&net_throttle + 4);

        // supported_features (v3+) -- empty array
        int32_t supported_features_count = htonl(0);
        buf.insert(buf.end(), (uint8_t*)&supported_features_count, (uint8_t*)&supported_features_count + 4);

        // finalize_features_epoch (v3+) -- INT64, -1 means not present
        int64_t finalize_features_epoch = -1;
        int64_t net_finalize_features_epoch = htobe64(finalize_features_epoch);
        buf.insert(buf.end(), (uint8_t*)&net_finalize_features_epoch, (uint8_t*)&net_finalize_features_epoch + 8);

        // finalized_features (v3+) -- empty array
        int32_t finalized_features_count = htonl(0);
        buf.insert(buf.end(), (uint8_t*)&finalized_features_count, (uint8_t*)&finalized_features_count + 4);

        // Now set the size field at the start (excluding itself)
        int32_t msglen = buf.size() - 4;
        int32_t net_msglen = htonl(msglen);
        memcpy(buf.data(), &net_msglen, 4);

        // Write all
        writeall(buf.data(), buf.size());
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
    // If APIVersions request
    if (m.request_api_key == 18) {
        s.RespondApiVersionsV4(m.correlation_id);
    } else {
        s.RespondV0(m.correlation_id); // fallback
    }
    return 0;
}