#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <map>
#include <unordered_map>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>
#include <queue>

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t que = PTHREAD_MUTEX_INITIALIZER;

std::unordered_map<std::string, std::string> keyValuePairs;
std::queue<std::pair<int, std::string>> requestQueue;

void *handle_requests(void *arg);

int main(int argc, char **argv) {
    int sockfd, newsockfd, portno;
    socklen_t clilen;
    struct sockaddr_in serv_addr, cli_addr;

    if (argc != 2) exit(1);
    portno = atoi(argv[1]);
    sockfd = socket(AF_INET, SOCK_STREAM, 0);


    memset((char *)&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);

    bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) ;
    listen(sockfd, 5);
    printf("Server listening on port %d...\n", portno);

    clilen = sizeof(cli_addr);

    pthread_t request_thread;
    if (pthread_create(&request_thread, NULL, handle_requests, NULL) != 0) {
        perror("ERROR creating request handling thread");
        exit(1);
    }
    if (pthread_create(&request_thread, NULL, handle_requests, NULL) != 0) {
        perror("ERROR creating request handling thread");
        exit(1);
    }
    if (pthread_create(&request_thread, NULL, handle_requests, NULL) != 0) {
        perror("ERROR creating request handling thread");
        exit(1);
    }

    while (1) {
        newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clilen);
        if (newsockfd < 0) {
            perror("ERROR on accept");
            continue;
        }

        char buffer[2048];
        ssize_t bytes_received = recv(newsockfd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_received <= 0) {
            perror("ERROR receiving data");
            close(newsockfd);
            continue;
        }

        buffer[bytes_received] = '\0';

        std::string request(buffer);
        std::pair<int, std::string> requestPair = {newsockfd, request};

        pthread_mutex_lock(&que);
        requestQueue.push(requestPair);
        pthread_mutex_unlock(&que);
    }

    close(sockfd);

    return 0;
}

void *handle_requests(void *arg) {
    while (true) {
        pthread_mutex_lock(&mutex);
        if (!requestQueue.empty()) {
            std::pair<int, std::string> requestPair = requestQueue.front();
            requestQueue.pop();
            pthread_mutex_unlock(&mutex);

            int sockfd = requestPair.first;
            std::string request = requestPair.second;

            std::istringstream iss(request);
            std::string line;

            while (std::getline(iss, line)) {
                if (line == "WRITE") {
                    std::getline(iss, line);
                    std::string key = line.substr(0, line.length());

                    std::getline(iss, line);
                    std::string value = line.substr(1, -1);

                    pthread_mutex_lock(&mutex);
                    keyValuePairs[key] = value;
                    pthread_mutex_unlock(&mutex);

                    std::string response = "FIN\n";
                    send(sockfd, response.c_str(), response.length(), 0);

                } else if (line == "END") {
                    std::string response = "\n";
                    send(sockfd, response.c_str(), response.length(), 0);
                    close(sockfd);

                } else if (line == "READ") {
                    std::getline(iss, line);
                    std::string key = line.substr(0, line.length());

                    pthread_mutex_lock(&mutex);
                    auto it = keyValuePairs.find(key);
                    if (it != keyValuePairs.end()) {
                        std::string response = it->second + "\n";
                        send(sockfd, response.c_str(), response.length(), 0);
                    } else {
                        std::string response = "NULL\n";
                        send(sockfd, response.c_str(), response.length(), 0);
                    }
                    pthread_mutex_unlock(&mutex);

                } else if (line == "COUNT") {
                    pthread_mutex_lock(&mutex);
                    std::string count = std::to_string(keyValuePairs.size()) + "\n";
                    send(sockfd, count.c_str(), count.length(), 0);
                    pthread_mutex_unlock(&mutex);

                } else if (line == "DELETE") {
                    pthread_mutex_lock(&mutex);

                    std::getline(iss, line);
                    std::string key = line.substr(0, line.length());
                    auto it = keyValuePairs.find(key);

                    if (it != keyValuePairs.end()) {
                        keyValuePairs.erase(it);
                        std::string response = "FIN\n";
                        send(sockfd, response.c_str(), response.length(), 0);
                    } else {
                        std::string response = "NULL\n";
                        send(sockfd, response.c_str(), response.length(), 0);
                    }
                    pthread_mutex_unlock(&mutex);

                } else {
                    std::string response = "INVALID\n";
                    send(sockfd, response.c_str(), response.length(), 0);
                }
            }

            close(sockfd);
        } else {
            pthread_mutex_unlock(&mutex);
            usleep(1000);
        }
    }
}
