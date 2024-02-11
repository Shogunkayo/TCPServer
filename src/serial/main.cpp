/* 
 * tcpserver.c - A multithreaded TCP echo server 
 * usage: tcpserver <port>
 * 
 * Testing : 
 * nc localhost <port> < input.txt
 */

#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <cstring>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unordered_map>
#include <vector>
#include <numeric>

#define BACKLOG 128
#define BUFF_SIZE 1024

std::unordered_map <std::string, std::string> kvstore;

void* get_in_addr(struct sockaddr *);
int create_socket(const char *, struct addrinfo *);
void handle_incomplete(int);

int main(int argc, char ** argv) {
    int sockfd, new_fd, rv;
    socklen_t sin_size;
    char s[INET6_ADDRSTRLEN];
    char buf_in[BUFF_SIZE];
    struct addrinfo hints;
    struct sockaddr_storage their_addr;

    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

    if (atoi(argv[1]) < 1025 || atoi(argv[1]) > 65535) {
        fprintf(stderr, "port number out of bounds");
        exit(1);
    }

    sockfd = create_socket(argv[1], &hints);

    if (listen(sockfd, BACKLOG) == -1) {
        perror("server: listen");
        exit(1);
    }
 
    #ifdef DEBUG
        printf("server: waiting for connections...\n");
    #endif

    while(1) {
        sin_size = sizeof their_addr;
        if ((new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size)) == -1) {
            perror("server: accept");
            continue;
        }

        #ifdef DEBUG
            inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr *)&their_addr), s, sizeof s);
            printf("server: got connection from %s\n", s);
        #endif
        
        if ((rv = recv(new_fd, buf_in, BUFF_SIZE, 0)) == -1) {
            perror("server: recv");
            break;
        }

        char *prev;
        char *token = strtok(buf_in, "\n");

        while (token != NULL) {
            if (strcmp(token, "WRITE") == 0) {
                // handle WRITE operations
                char *key = strtok(NULL, "\n");
                char *value = strtok(NULL, "\n") + 1;

                if (key == NULL || value == NULL) {
                    handle_incomplete(new_fd);
                    break;
                }

                kvstore[key] = value;
                send(new_fd, "FIN\n", 4, 0);
            }
            else if (strcmp(token, "READ") == 0) {
                // handle READ operations
                char *key = strtok(NULL, "\n");

                if (key == NULL) {
                    handle_incomplete(new_fd);
                    break;
                }
                
                if (kvstore.count(key) > 0) {
                    std::string res = kvstore[key];
                    res += "\n";
                    send(new_fd, res.c_str(), res.size(), 0);
                }
                else {
                    send(new_fd, "NULL\n", 5, 0);
                }
            }
            else if (strcmp(token, "COUNT") == 0) {
                // handle COUNT OPERATIONS
                std::string res = std::to_string(kvstore.size());

                send(new_fd, res.c_str(), res.size(), 0);
            }
            else if (strcmp(token, "DELETE") == 0) {
                // handle DELETE operations
                char *key = strtok(NULL, "\n");
                if (key == NULL) {
                    handle_incomplete(new_fd);
                    break;
                }

                if (kvstore.count(key) > 0) {
                    kvstore.erase(key);
                    send(new_fd, "FIN\n", 4, 0);
                }
                else {
                    send(new_fd, "NULL\n", 5, 0);
                }
            }
            else if (strcmp(token, "END") == 0) {
                // handle sending reply to client
                /* std::string result = std::accumulate(std::begin(reply), std::end(reply), std::string(),
                        [](const std::string& accumulated, const std::string& current) {
                            return accumulated.empty() ? current : accumulated + "\n" + current;
                        });
                */
                
                send(new_fd, "\n", 1, 0);
                close(new_fd);
            }
            
            prev = token;
            token = strtok(NULL, "\n");

            if (token == NULL && strcmp(prev, "END") != 0) {
                handle_incomplete(new_fd);
                break;
            }
        }
    }

    close(sockfd);
    return EXIT_SUCCESS;
}

void* get_in_addr(struct sockaddr *sa) {
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in *)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6 *)sa)->sin6_addr);
}

int create_socket(const char *portno, struct addrinfo *hints) {
    int rv, sockfd;
    int yes = 1;
    struct addrinfo *servinfo, *p;

    memset(hints, 0, sizeof(*hints));
    hints->ai_family = AF_UNSPEC;
    hints->ai_socktype = SOCK_STREAM;
    hints->ai_flags = AI_PASSIVE;

    if ((rv = getaddrinfo(NULL, portno, hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        exit(1);
    } 

    for (p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            perror("server: socket");
            continue;
        }

        if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes) == -1) {
            perror("server: setsockopt");
            exit(1);
        }

        if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            close(sockfd);
            perror("server: bind");
            continue;
        }

        break;
    }

    freeaddrinfo(servinfo);

    if (p == NULL) {
        fprintf(stderr, "server: failed to bind\n");
        exit(1);
    }
    
    return sockfd;
}

void handle_incomplete(int new_fd) {
    send(new_fd, "NULL\n", 5, 0);
    close(new_fd);
}
