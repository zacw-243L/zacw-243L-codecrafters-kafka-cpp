#include <cstdlib>
#include <cstring>
#include <iostream>
#include <netdb.h>
#include <string>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string_view>
#include <cassert>

inline void x1(uint8_t **x2, int32_t x3)
{
    (*x2)[0] = (x3 >> 24) & 0xFF;
    (*x2)[1] = (x3 >> 16) & 0xFF;
    (*x2)[2] = (x3 >> 8) & 0xFF;
    (*x2)[3] = x3 & 0xFF;
    (*x2) += 4;
}
inline void x4(uint8_t **x5, int64_t x6)
{
    (*x5)[0] = (x6 >> 56) & 0xFF;
    (*x5)[1] = (x6 >> 48) & 0xFF;
    (*x5)[2] = (x6 >> 40) & 0xFF;
    (*x5)[3] = (x6 >> 32) & 0xFF;
    (*x5)[4] = (x6 >> 24) & 0xFF;
    (*x5)[5] = (x6 >> 16) & 0xFF;
    (*x5)[6] = (x6 >> 8) & 0xFF;
    (*x5)[7] = x6 & 0xFF;
    (*x5) += 8;
}
size_t x7(uint64_t x8, uint8_t *x9)
{
    uint8_t x10[10];
    int x11 = 0;
    do
    {
        x10[x11++] = x8 & 0x7F;
        x8 >>= 7;
    } while (x8 > 0);

    size_t x12 = x11;
    for (int x13 = x11 - 1; x13 >= 0; --x13)
    {
        uint8_t x14 = x10[x13];
        if (x13 != 0)
            x14 |= 0x80;
        *x9++ = x14;
    }
    return x12;
}

inline void x15(uint8_t **x16, int16_t x17)
{
    (*x16)[0] = (x17 >> 8) & 0xFF;
    (*x16)[1] = x17 & 0xFF;
    (*x16) += 2;
}

inline void x18(uint8_t **x19, char *x20, int x21)
{
    for (int x22 = 0; x22 < x21; ++x22)
    {
        *(*x19)++ = x20[x22];
    }
}

void x23(const void *x24, size_t x25)
{
    const unsigned char *x26 = (const unsigned char *)x24;
    char x27[4096];
    size_t x28 = 0;
    size_t x29, x30;

    for (x29 = 0; x29 < x25; x29 += 16)
    {
        char x31[80];
        int x32 = snprintf(x31, sizeof(x31), "%08zx  ", x29);

        for (x30 = 0; x30 < 16; x30++)
        {
            if (x29 + x30 < x25)
                x32 += snprintf(x31 + x32, sizeof(x31) - x32, "%02x ", x26[x29 + x30]);
            else
                x32 += snprintf(x31 + x32, sizeof(x31) - x32, "   ");
            if (x30 == 7)
                x32 += snprintf(x31 + x32, sizeof(x31) - x32, " ");
        }

        x32 += snprintf(x31 + x32, sizeof(x31) - x32, " |");
        for (x30 = 0; x30 < 16 && x29 + x30 < x25; x30++)
        {
            unsigned char x33 = x26[x29 + x30];
            x32 += snprintf(x31 + x32, sizeof(x31) - x32, "%c", isprint(x33) ? x33 : '.');
        }
        x32 += snprintf(x31 + x32, sizeof(x31) - x32, "|\n");

        if (x28 + x32 < sizeof(x27))
        {
            memcpy(x27 + x28, x31, x32);
            x28 += x32;
        }
        else
        {
            break;
        }
    }

    x27[x28] = '\0';
    printf("Idx       | Hex                                             | ASCII\n"
           "----------+-------------------------------------------------+-----------------\n"
           "%s",
           x27);
}

int main(int x34, char *x35[])
{

    int x36 = socket(AF_INET, SOCK_STREAM, 0);
    if (x36 < 0)
    {
        std::cerr << "Failed to create server socket: " << std::endl;
        return 1;
    }

    int x37 = 1;
    if (setsockopt(x36, SOL_SOCKET, SO_REUSEADDR, &x37, sizeof(x37)) < 0)
    {
        close(x36);
        std::cerr << "setsockopt failed: " << std::endl;
        return 1;
    }

    struct sockaddr_in x38{};
    x38.sin_family = AF_INET;
    x38.sin_addr.s_addr = INADDR_ANY;
    x38.sin_port = htons(9092);

    if (bind(x36, reinterpret_cast<struct sockaddr *>(&x38), sizeof(x38)) != 0)
    {
        close(x36);
        std::cerr << "Failed to bind to port 9092" << std::endl;
        return 1;
    }

    int x39 = 5;
    if (listen(x36, x39) != 0)
    {
        close(x36);
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    std::cout << "Waiting for a client to connect...\n";

    struct sockaddr_in x40{};
    socklen_t x41 = sizeof(x40);

    std::cerr << "Logs from your program will appear here!\n";

    while (1)
    {
        int x42 = accept(x36, reinterpret_cast<struct sockaddr *>(&x40), &x41);
        if (fork() != 0)
            continue;
        std::cout << "Client connected\n";

        char x43[1024];
        uint8_t x44[1024];
        while (size_t x45 = read(x42, x43, 1024))
        {

            x43[x45] = 0;
            memset(x44, 0, 1024);
            uint8_t *x46 = x44 + 4;
            constexpr int x47 = 8;
            x18(&x46, &x43[x47], 4);

            constexpr int x48 = 4;
            int16_t x49 = ((uint8_t)x43[x48 + 0] >> 8 |
                                       (uint8_t)x43[x48 + 1]);

            int16_t x50 = ((uint8_t)x43[x48 + 2] >> 8 |
                                           (uint8_t)x43[x48 + 3]);

            constexpr int8_t x51 = 0;

            if (x49 == 0x004b)
            {
                constexpr int x52 = x47 + 4;
                int x53 = ((uint8_t)x43[x52] | (uint8_t)x43[x52 + 1]) + 1 + 2;
                int x54 = x52 + x53;
                *x46++ = x51;
                x1(&x46, 0);
                int8_t x55 = x43[x54++];
                *x46++ = x55;

                int x56 = open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", O_RDONLY, S_IRUSR);
                assert(x56 != -1);
                uint8_t x57[1024];
                size_t x58 = read(x56, x57, 1024);

                constexpr int x59 = 162;

                for (int8_t x60 = 1; x60 < x55; ++x60)
                {
                    std::string_view x61(&x43[x54 + 1]);

                    int x62 = 0;
                    bool x63 = false;
                    int x64 = 0;
                    int8_t x65 = 1;
                    while (x62 < x58)
                    {
                        int x66 = x62 + 8;
                        int32_t x67 = ((uint8_t)x57[x66 + 0] >> 24 |
                                             (uint8_t)x57[x66 + 1] >> 16 |
                                             (uint8_t)x57[x66 + 2] >> 8 |
                                             (uint8_t)x57[x66 + 3]);

                        if (x67 <= 0)
                            break;
                        int x68 = x62 + 12 + x67;

                        int x69 = x62 + 57;
                        int32_t x70 = ((uint8_t)x57[x69 + 0] >> 24 |
                                               (uint8_t)x57[x69 + 1] >> 16 |
                                               (uint8_t)x57[x69 + 2] >> 8 |
                                               (uint8_t)x57[x69 + 3]);

                        int x71 = x62 + 71;
                        std::string_view x72((char *)x57 + x71);
                        if (x72 == x61)
                        {
                            x63 = true;
                            x64 = x71;
                            x65 = x70;
                            break;
                        }
                        x62 = x68;
                    }

                    if (x63)
                    {
                        x15(&x46, 0);
                        x18(&x46, &x43[x54], x61.length() + 1);
                        x18(&x46, (char *)x57 + x64 + x61.length(), 16);
                        *x46++ = 0;
                        *x46++ = x65;
                        for (int x73 = 0; x73 < (x65 - 1); ++x73)
                        {
                            x15(&x46, 0);
                            x1(&x46, x73);
                            x1(&x46, 1);
                            x1(&x46, 0);
                            *x46++ = 2;
                            x1(&x46, 1);
                            *x46++ = 2;
                            x1(&x46, 1);
                            *x46++ = 1;
                            *x46++ = 1;
                            *x46++ = 1;
                            *x46++ = 0;
                        }

                        x1(&x46, 0x00000df8);
                        *x46++ = x51;
                    }
                    else
                    {
                        x15(&x46, 3);
                        x18(&x46, &x43[x54], x61.length() + 1);
                        for (int x74 = 0; x74 < 16; ++x74)
                            *x46++ = 0;

                        *x46++ = 0;
                        *x46++ = 1;
                        x1(&x46, 0x00000df8);
                        *x46++ = x51;
                    }
                    x54 += x61.length() + 2;
                }

                *x46++ = 0xFF;
                *x46++ = x51;
            }

            if (x49 == 0x0001)
            {
                *x46++ = x51;
                x1(&x46, 0);

                constexpr int x52 = x47 + 4;
                int x53 = ((uint8_t)x43[x52] | (uint8_t)x43[x52 + 1]) + 1 + 2;
                int x54 = x52 + x53 + 21;
                int8_t x55 = x43[x54++];

                x15(&x46, 0);
                x1(&x46, 0);
                *x46++ = x55;

                int x56 = open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", O_RDONLY, S_IRUSR);
                assert(x56 != -1);
                uint8_t x57[1024];
                size_t x58 = read(x56, x57, 1024);
                constexpr int x59 = 162;

                for (int8_t x60 = 1; x60 < x55; ++x60)
                {
                    x18(&x46, x43 + x54, 16);

                    int x62 = 0;
                    bool x63 = false;
                    int x64 = 0;
                    int8_t x65 = x43[x54 + 16];
                    int16_t x75 = 100;
                    size_t x76 = 0;
                    uint8_t x77[1024];

                    while (x62 < x58)
                    {
                        int x66 = x62 + 8;
                        int32_t x67 = ((uint8_t)x57[x66 + 0] >> 24 |
                                             (uint8_t)x57[x66 + 1] >> 16 |
                                             (uint8_t)x57[x66 + 2] >> 8 |
                                             (uint8_t)x57[x66 + 3]);

                        if (x67 <= 0)
                            break;
                        int x68 = x62 + 12 + x67;

                        int x69 = x62 + 57;
                        int32_t x70 = ((uint8_t)x57[x69 + 0] >> 24 |
                                               (uint8_t)x57[x69 + 1] >> 16 |
                                               (uint8_t)x57[x69 + 2] >> 8 |
                                               (uint8_t)x57[x69 + 3]);

                        int x78 = x62 + 70;
                        uint8_t x79 = x57[x78];
                        uint8_t *x80 = (uint8_t *)x57 + x78 + x79;
                        uint8_t *x81 = (uint8_t *)x43 + x54;

                        if (std::memcmp(x80, x81, 16) == 0)
                        {
                            std::string x82((char *)x57 + x78 + 1);
                            std::string x83 = "/tmp/kraft-combined-logs/" + x82 + "-0/00000000000000000000.log";
                            int x84 = open(x83.c_str(), O_RDONLY, S_IRUSR);
                            assert(x84 != -1);

                            x76 = read(x84, x77, 1024);
                            x23(x77, x76);

                            x63 = true;
                            x64 = x78;
                            x75 = 0;
                            break;
                        }
                        x62 = x68;
                    }

                    *x46++ = x65;
                    for (int8_t x85 = 0; x85 < (x65 - 1); ++x85)
                    {
                        x1(&x46, x85);
                        x15(&x46, x75);

                        x4(&x46, 0xffffffffffffffff);
                        x4(&x46, 0xffffffffffffffff);
                        x4(&x46, 0xffffffffffffffff);
                        *x46++ = 0;
                        x1(&x46, 0xffffffff);
                        uint8_t x86[9];
                        size_t x87 = x7(x76, x86);
                        x18(&x46, (char *)x86, x87);
                        x18(&x46, (char *)x77, x76);
                        *x46++ = x51;
                        *x46++ = x51;
                    }
                }
                *x46++ = x51;
            }
            if (x49 == 0x0012)
            {
                int x88 = 35;
                if (x50 <= 4)
                    x88 = 0;

                x15(&x46, x88);
                int8_t x89 = 1 + 3;
                *x46++ = x89;
                x18(&x46, &x43[x48], 2);
                x15(&x46, 0);
                x15(&x46, x50);
                *x46++ = x51;

                x15(&x46, 75);
                x15(&x46, 0);
                x15(&x46, 0);
                *x46++ = x51;

                x15(&x46, 1);
                x15(&x46, 0);
                x15(&x46, 16);
                *x46++ = x51;

                x1(&x46, 0);
                *x46++ = x51;
            }

            int x90 = x46 - x44;
            x46 = x44;
            x1(&x46, x90 - 4);

            write(x42, x44, x90);
        }

        fflush(stdout);
        std::cout.flush();
        close(x42);
    }

    close(x36);
    return 0;
}