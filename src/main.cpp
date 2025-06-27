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
        read(client_fd, &h, sizeof(h));
        h.request_api_key = ntohs(h.request_api_key);
        h.request_api_version = ntohs(h.request_api_version);
        h.client_id_length = ntohs(h.client_id_length);

        char *client_id = new char[h.client_id_length];
        read(client_fd, client_id, h.client_id_length);

        size = ntohl(size);
        char *body = new char[size - sizeof(h) - h.client_id_length];
        read(client_fd, body, size - sizeof(h) - h.client_id_length);

        // ---- Modified Section: Add both API_VERSIONS (18) and DESCRIBE_TOPIC_PARTITIONS (75) ----
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
        // -----------------------------------------------------------------------------

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