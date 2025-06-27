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

// Message struct (as used in your original code)
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
        // The protocol requires us to first read 4+2+2+4 = 12 bytes header, then the rest as body.
        if (recv(client_fd, &net_size, 4, MSG_WAITALL) != 4)
            throw std::runtime_error("size read failed");
        if (recv(client_fd, &net_api_key, 2, MSG_WAITALL) != 2)
            throw std::runtime_error("api_key read failed");
        if (recv(client_fd, &net_api_version, 2, MSG_WAITALL) != 2)
            throw std::runtime_error("api_version read failed");
        if (recv(client_fd, &net_correlation_id, 4, MSG_WAITALL) != 4)
            throw std::runtime_error("correlation_id read failed");
        size = ntohl(net_size);
        request_api_key = ntohs(net_api_key);
        request_api_version = ntohs(net_api_version);
        correlation_id = ntohl(net_correlation_id);
        payload_size = size - 12; // 4+2+2+4=12
        if (payload_size < 0)
            throw std::runtime_error("negative payload size");
        buf = std::make_unique<char[]>(payload_size + 1);
        ssize_t bytes_read = 0;
        while (bytes_read < payload_size)
        {
            ssize_t r = recv(client_fd, buf.get() + bytes_read, payload_size - bytes_read, 0);
            if (r <= 0)
                throw std::runtime_error("invalid payload size");
            bytes_read += r;
        }
        buf[payload_size] = '\0';
    }
};

// Helper for 16-bit host to network
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

    // Helper to write all bytes
    void writeall(const void *buf, size_t len)
    {
        const char *p = (const char *)buf;
        while (len > 0)
        {
            ssize_t n = send(client_fd, p, len, 0);
            if (n <= 0)
                throw std::runtime_error("send failed");
            p += n;
            len -= n;
        }
    }

    // Helper: encode unsigned varint (Kafka base 128)
    void encode_unsigned_varint(std::vector<uint8_t> &buf, uint32_t val)
    {
        while (val > 127)
        {
            buf.push_back(static_cast<uint8_t>((val & 0x7F) | 0x80));
            val >>= 7;
        }
        buf.push_back(static_cast<uint8_t>(val & 0x7F));
    }

    // Helper: encode compact string (length as unsigned varint + data)
    void encode_compact_string(std::vector<uint8_t> &buf, const std::string &str)
    {
        encode_unsigned_varint(buf, str.size() + 1);
        buf.insert(buf.end(), str.begin(), str.end());
    }

    int32_t RespondApiVersionsV4(int32_t correlation_id)
    {
        std::vector<uint8_t> buf;
        buf.resize(4); // Reserve 4 bytes for message length

        // Correlation ID (int32)
        int32_t corr = htonl(correlation_id);
        buf.insert(buf.end(), (uint8_t *)&corr, (uint8_t *)&corr + 4);

        // Error code (int16)
        int16_t error_code = 0;
        int16_t net_error_code = htons(error_code);
        buf.insert(buf.end(), (uint8_t *)&net_error_code, (uint8_t *)&net_error_code + 2);

        // Api versions (compact array, 1 entry)
        encode_unsigned_varint(buf, 1 + 1); // Count=1, encoded as 2
        // Api key=18 (API_VERSIONS), min_version=0, max_version=4
        int16_t api_key = htons(18);
        int16_t min_version = htons(0);
        int16_t max_version = htons(4);
        buf.insert(buf.end(), (uint8_t *)&api_key, (uint8_t *)&api_key + 2);
        buf.insert(buf.end(), (uint8_t *)&min_version, (uint8_t *)&min_version + 2);
        buf.insert(buf.end(), (uint8_t *)&max_version, (uint8_t *)&max_version + 2);
        // Api versions TAG_BUFFER (empty)
        buf.push_back(0);

        // Throttle time (int32)
        int32_t throttle_time_ms = 0;
        int32_t net_throttle = htonl(throttle_time_ms);
        buf.insert(buf.end(), (uint8_t *)&net_throttle, (uint8_t *)&net_throttle + 4);

        // Supported features (compact array, empty)
        encode_unsigned_varint(buf, 0 + 1); // Count=0, encoded as 1

        // Finalize features epoch (int64)
        int64_t finalize_features_epoch = -1;
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        int64_t net_finalize_features_epoch = __builtin_bswap64(finalize_features_epoch);
#else
        int64_t net_finalize_features_epoch = finalize_features_epoch;
#endif
        buf.insert(buf.end(), (uint8_t *)&net_finalize_features_epoch, (uint8_t *)&net_finalize_features_epoch + 8);

        // Finalized features (compact array, empty)
        encode_unsigned_varint(buf, 0 + 1); // Count=0, encoded as 1

        // Final TAG_BUFFER (empty)
        buf.push_back(0);

        // Set message length (excluding the 4-byte length field itself)
        int32_t msglen = buf.size() - 4;
        int32_t net_msglen = htonl(msglen);
        memcpy(buf.data(), &net_msglen, 4);

        // Send the response
        writeall(buf.data(), buf.size());
        return 0;
    }

    // Fallback for non-ApiVersions requests (just returns error)
    int32_t RespondV0(int32_t correlation_id)
    {
        int32_t size = 6; // correlation_id (4) + error_code (2)
        int32_t net_size = htonl(size);
        int32_t net_correlation_id = htonl(correlation_id);
        int16_t error_code = 35; // e.g., unsupported request
        int16_t net_error_code = htons(error_code);

        writeall(&net_size, 4);
        writeall(&net_correlation_id, 4);
        writeall(&net_error_code, 2);
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

    // Loop to handle multiple sequential requests on the same connection
    while (true)
    {
        try
        {
            Message m = s.RecieveV0();
            if (m.request_api_key == 18 && m.request_api_version >= 0 && m.request_api_version <= 4)
            {
                s.RespondApiVersionsV4(m.correlation_id);
            }
            else
            {
                s.RespondV0(m.correlation_id); // fallback
            }
        }
        catch (const std::exception &e)
        {
            // Probably client closed connection, so break loop
            break;
        }
    }
    return 0;
}