#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <pthread.h>
#include <vector>

struct Header
{
    uint16_t request_api_key;
    uint16_t request_api_version;
    uint32_t correlation_id;
    uint16_t client_id_length;
};

struct api_version
{
    int16_t api_key;
    int16_t min_version;
    int16_t max_version;
    char tag_buffer;
};

struct api_versions
{
    int8_t size;
    api_version *array;
};

uint32_t read_unsigned_varint(int fd) {
    uint32_t result = 0;
    int shift = 0;
    while (true) {
        uint8_t byte;
        if (read(fd, &byte, 1) != 1) throw std::runtime_error("Failed to read uvarint");
        result |= (uint32_t)(byte & 0x7F) << shift;
        if (!(byte & 0x80)) break;
        shift += 7;
    }
    return result;
}

std::string read_compact_string(int fd) {
    uint32_t len = read_unsigned_varint(fd);
    if (len == 0) return "";
    std::vector<char> buf(len - 1);
    size_t got = 0;
    while (got < buf.size()) {
        ssize_t r = read(fd, buf.data() + got, buf.size() - got);
        if (r <= 0) throw std::runtime_error("Failed to read compact string");
        got += r;
    }
    return std::string(buf.data(), buf.size());
}

void write_null_uuid(int fd) {
    uint8_t uuid[16] = {0};
    write(fd, uuid, 16);
}

void *process_client(void *arg)
{
    int client_fd = *(int *)arg;

    while (true)
    {
        uint32_t size;
        uint16_t error_code;
        Header h;
        int bytes = read(client_fd, &size, sizeof(size));
        if (bytes == 0)
        {
            break;
        }
        if (bytes != 4) break;
        read(client_fd, &h, sizeof(h));
        h.request_api_key = ntohs(h.request_api_key);
        h.request_api_version = ntohs(h.request_api_version);
        h.client_id_length = ntohs(h.client_id_length);

        char *client_id = new char[h.client_id_length];
        read(client_fd, client_id, h.client_id_length);

        size = ntohl(size);
        char *body = new char[size - sizeof(h) - h.client_id_length];
        read(client_fd, body, size - sizeof(h) - h.client_id_length);

        // --- DescribeTopicPartitions (API key 75, version 0) ---
        if (h.request_api_key == 75 && h.request_api_version == 0) {
            // Parse request body directly from client_fd
            uint32_t topics_count = read_unsigned_varint(client_fd);
            if (topics_count < 1) throw std::runtime_error("Invalid topics array");
            std::string topic_name = read_compact_string(client_fd);
            uint8_t topic_tagged_fields;
            read(client_fd, &topic_tagged_fields, 1);

            // Skip remaining topics for robustness
            for (uint32_t t = 1; t < topics_count - 1; ++t) {
                (void)read_compact_string(client_fd);
                read(client_fd, &topic_tagged_fields, 1);
            }

            int32_t response_partition_limit;
            read(client_fd, &response_partition_limit, 4);

            std::string cursor_topic_name = read_compact_string(client_fd);
            int32_t cursor_partition_index;
            read(client_fd, &cursor_partition_index, 4);
            uint8_t cursor_tagged_fields;
            read(client_fd, &cursor_tagged_fields, 1);

            uint8_t req_tagged_fields;
            read(client_fd, &req_tagged_fields, 1);

            // Build response
            std::vector<uint8_t> resp;
            resp.resize(4); // Reserve for length

            // Correlation ID
            uint32_t corr = h.correlation_id;
            corr = htonl(corr);
            resp.insert(resp.end(), (uint8_t*)&corr, (uint8_t*)&corr + 4);

            // Topics array (1 entry)
            resp.push_back(0x02); // 1 + 1 = 2

            // Topic name
            uint32_t name_len = topic_name.size();
            uint32_t name_len_uvarint = name_len + 1;
            resp.push_back((uint8_t)name_len_uvarint);
            resp.insert(resp.end(), topic_name.begin(), topic_name.end());

            // Topic ID (null UUID)
            write_null_uuid(client_fd); // Directly write to avoid vector overflow

            // Error code
            int16_t err = htons(3); // UNKNOWN_TOPIC_OR_PARTITION
            resp.insert(resp.end(), (uint8_t*)&err, (uint8_t*)&err + 2);

            // Partitions (empty)
            resp.push_back(0x01); // 0 + 1 = 1

            // Tagged fields
            resp.push_back(0x00); // Topic tagged fields
            resp.push_back(0x00); // Response tagged fields

            // Set message length
            int32_t msglen = resp.size() - 4 + 16; // Add 16 bytes for UUID written directly
            int32_t net_msglen = htonl(msglen);
            memcpy(resp.data(), &net_msglen, 4);

            // Write response
            size_t w = 0;
            while (w < resp.size()) {
                ssize_t n = write(client_fd, resp.data() + w, resp.size() - w);
                if (n <= 0) break;
                w += n;
            }

            delete[] client_id;
            delete[] body;
            continue;
        }

        // ---- APIVersions ----
        api_versions content;
        content.size = 3; // 2 entries, size = count+1
        content.array = new api_version[content.size - 1];

        content.array[0].api_key = htons(18);
        content.array[0].min_version = htons(0);
        content.array[0].max_version = htons(4);
        content.array[0].tag_buffer = 0;

        content.array[1].api_key = htons(75);
        content.array[1].min_version = htons(0);
        content.array[1].max_version = htons(0);
        content.array[1].tag_buffer = 0;

        uint32_t throttle_time_ms = 0;
        int8_t tag = 0;
        uint32_t res_size;

        if (h.request_api_version > 4)
        {
            error_code = htons(35);
            res_size = sizeof(h.correlation_id) + sizeof(error_code);
            write(client_fd, &res_size, sizeof(res_size));
            write(client_fd, &h.correlation_id, sizeof(h.correlation_id));
            write(client_fd, &(error_code), sizeof(error_code));
        }
        else
        {
            error_code = 0;
            res_size = htonl(sizeof(h.correlation_id) + sizeof(error_code) + sizeof(content.size) + (content.size - 1) * 7 + sizeof(throttle_time_ms) + sizeof(tag));
            write(client_fd, &res_size, sizeof(res_size));
            write(client_fd, &h.correlation_id, sizeof(h.correlation_id));
            write(client_fd, &(error_code), sizeof(error_code));
            write(client_fd, &content.size, sizeof(content.size));
            for (int i = 0; i < content.size - 1; i++)
            {
                write(client_fd, &content.array[i], 7);
            }
            write(client_fd, &(throttle_time_ms), sizeof(throttle_time_ms));
            write(client_fd, &(tag), sizeof(tag));
        }
        delete[] client_id;
        delete[] content.array;
        delete[] body;
    }

    close(client_fd);
    return nullptr;
}

int main(int argc, char *argv[])
{
    pthread_t clients[50];

    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        close(server_fd);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(9092);

    if (bind(server_fd, reinterpret_cast<struct sockaddr *>(&server_addr), sizeof(server_addr)) != 0)
    {
        close(server_fd);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0)
    {
        close(server_fd);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);

    std::cerr << "Logs from your program will appear here!\n";

    int client_fd;
    int idx = 0;
    while (1)
    {
        client_fd = accept(server_fd, reinterpret_cast<struct sockaddr *>(&client_addr), &client_addr_len);
        pthread_create(&clients[idx++], nullptr, process_client, &client_fd);
    }

    close(server_fd);
    return 0;
}