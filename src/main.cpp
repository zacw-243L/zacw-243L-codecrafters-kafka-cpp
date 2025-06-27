#include "kafka_connection.hpp"
#include <cstdint>
#include <cstring>
#include <iostream>

void KafkaConnection::send_response(Response response)
{
    std::print(std::cout, "Sending the following Response:\n{}\n", response.to_string());

    // there's most likely some better way to write this but fine for now...
    std::int32_t message_size = htonl(response.get_message_size());
    int bytes_written = send(m_socket_fd, &message_size, sizeof(message_size), 0);
    if (bytes_written < 0)
    {
        std::cerr << "Failed to write to message size to client\n";
        throw std::runtime_error(std::format("Error writing to client: {}", m_socket_fd));
    }

    std::int32_t header = htonl(response.get_header());
    bytes_written = send(m_socket_fd, &header, sizeof(header), 0);
    if (bytes_written < 0)
    {
        std::cerr << "Failed to write to header to client\n";
        throw std::runtime_error(std::format("Error writing to client: {}", m_socket_fd));
    }

    auto body = response.get_body();
    bytes_written = send(m_socket_fd, body.data(), body.size(), 0);
    if (bytes_written < 0)
    {
        std::cerr << "Failed to write to body to client\n";
        throw std::runtime_error(std::format("Error writing to client: {}", m_socket_fd));
    }
}

void KafkaConnection::process_request()
{
    Request request = read_request();
    // expect versions 0-4
    Response response = {static_cast<std::int32_t>(request.get_correlation_id())};

    // we're expecting "ApiVersions" here, e.g. request key == 18. Don't check for now
    auto version = request.get_request_api_version();
    if (version >= 0 && version <= 4)
    {
        response.append(htons(0));                // error code
        response.append(static_cast<uint8_t>(2)); // Tag field for api key
        response.append(htons(18));               // API index (18 == ApiVersions)
        response.append(htons(0));                // Min versions
        response.append(htons(4));                // Max version (at least 4)
        response.append(static_cast<uint8_t>(0)); // Tag field for api key
        response.append(htonl(0));                // Throttle field
        response.append(static_cast<uint8_t>(0)); // Tag field for response
    }
    else
    {
        response.append(htons(35)); // error code
    }

    send_response(response);
}

Request KafkaConnection::read_request()
{
    std::vector<char> data;
    char buffer[1024];

    std::cout << "Reading client request\n";
    while (true)
    {
        int bytes_read = recv(m_socket_fd, buffer, sizeof(buffer), 0);

        if (bytes_read == -1)
        {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
            {
                std::cout << "Failed to read client request: " << std::strerror(errno) << std::endl;
                break; // no more data to read
            }
            else
            {
                throw std::runtime_error(std::format("Error reading from socket: {}", m_socket_fd));
            }
        }
        else if (bytes_read == 0)
        {
            std::cout << "No more bytes to read" << std::endl;
            break; // connection closed/no more data
        }
        else
        {
            std::cout << "Read " << bytes_read << " bytes." << std::endl;
            data.insert(data.end(), buffer, buffer + bytes_read);
            if (static_cast<uint>(bytes_read) < sizeof(buffer))
                break;
        }
    }

    Request request = Request(data);
    std::print(std::cout, "Parsed the following Request:\n{}\n", request.to_string());
    return request;
}