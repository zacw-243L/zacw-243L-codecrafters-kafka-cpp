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

void write_all(int fd, const void* data, size_t len) {
    const char* p = (const char*)data;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) break;
        p += n;
        len -= n;
    }
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
            // For DescribeTopicPartitions v0, request header version 2, the body is compact format.
            // Use a memory stream to parse the body buffer, not fd (so as not to eat bytes for next request).
            size_t offset = 0;
            auto read_body_uvarint = [&]() -> uint32_t {
                uint32_t result = 0;
                int shift = 0;
                while (true) {
                    if (offset >= size - sizeof(h) - h.client_id_length) throw std::runtime_error("Body overflow");
                    uint8_t byte = (uint8_t)body[offset++];
                    result |= (uint32_t)(byte & 0x7F) << shift;
                    if (!(byte & 0x80)) break;
                    shift += 7;
                }
                return result;
            };
            auto read_body_compact_string = [&]() -> std::string {
                uint32_t len = read_body_uvarint();
                if (len == 0) return "";
                if (offset + (len-1) > size - sizeof(h) - h.client_id_length) throw std::runtime_error("Body overflow");
                std::string s(body + offset, body + offset + (len - 1));
                offset += (len - 1);
                return s;
            };
            auto read_body_uint8 = [&]() -> uint8_t {
                if (offset >= size - sizeof(h) - h.client_id_length) throw std::runtime_error("Body overflow");
                return (uint8_t)body[offset++];
            };
            auto read_body_int32 = [&]() -> int32_t {
                if (offset + 4 > size - sizeof(h) - h.client_id_length) throw std::runtime_error("Body overflow");
                int32_t v;
                memcpy(&v, body + offset, 4);
                offset += 4;
                return v;
            };

            // topics array
            uint32_t topics_count = read_body_uvarint();
            if (topics_count < 1) throw std::runtime_error("Invalid topics array");
            std::string topic_name = read_body_compact_string();
            uint8_t topic_tagged_fields = read_body_uint8();

            // skip remaining topics, if any
            for (uint32_t t = 1; t < topics_count - 1; ++t) {
                (void)read_body_compact_string();
                read_body_uint8();
            }

            // response_partition_limit
            int32_t response_partition_limit = read_body_int32();

            // cursor: compact string, int32, tagged fields
            std::string cursor_topic_name = read_body_compact_string();
            int32_t cursor_partition_index = read_body_int32();
            uint8_t cursor_tagged_fields = read_body_uint8();

            // request tagged_fields
            uint8_t req_tagged_fields = read_body_uint8();

            // Build DescribeTopicPartitionsResponse (v0)
            std::vector<uint8_t> resp;
            resp.resize(4); // reserve for length

            // correlation_id
            int32_t corr = h.correlation_id;
            corr = htonl(corr);
            resp.insert(resp.end(), (uint8_t*)&corr, (uint8_t*)&corr + 4);

            // topics (compact array, 1 entry)
            resp.push_back(0x02); // 1 + 1 = 2

            // topic_name (compact string)
            uint32_t name_len_uvarint = topic_name.size() + 1;
            resp.push_back((uint8_t)name_len_uvarint);
            resp.insert(resp.end(), topic_name.begin(), topic_name.end());

            // topic_id: 16 bytes of zeros
            for (int i = 0; i < 16; ++i) resp.push_back(0);

            // error_code (int16)
            int16_t err = htons(3); // UNKNOWN_TOPIC_OR_PARTITION
            resp.insert(resp.end(), (uint8_t*)&err, (uint8_t*)&err + 2);

            // partitions (empty compact array)
            resp.push_back(0x01); // 0 + 1 = 1

            // topic tagged fields
            resp.push_back(0x00);

            // response tagged fields
            resp.push_back(0x00);

            // Set length at start
            int32_t msglen = resp.size() - 4;
            int32_t net_msglen = htonl(msglen);
            memcpy(resp.data(), &net_msglen, 4);

            write_all(client_fd, resp.data(), resp.size());

            delete[] client_id;
            delete[] body;
            continue;
        }

        // ---- APIVersions ----
        api_versions content;
        content.size = 3; // 2 entries, size = count+1 (like original code)
        content.array = new api_version[content.size - 1];

        // API_VERSIONS (18)
        content.array[0].api_key = htons(18);
        content.array[0].min_version = htons(0);
        content.array[0].max_version = htons(4);
        content.array[0].tag_buffer = 0;

        // DESCRIBE_TOPIC_PARTITIONS (75)
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