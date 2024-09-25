#include "unit_test.h"

//int main(int argc, char *argv[]) {
//    const char *server_host = "127.0.0.1";  // 127.0.0.1 192.168.31.25
//    int server_port = PORT_DEFAULT;
//    std::string test_name = argv[1];
//
//
//    int sockfd;
//
//    sockfd = init_tcp_sock(server_host, server_port);
//
//    if (sockfd < 0) {
//        return 1;
//    }
//
//    std::ifstream test;
//    std::string sql;
//
//    test.open(test_name);
//    while(std::getline(test, sql)) {
//        send_recv_sql(sockfd, sql);
//    }
//
//    close(sockfd);
//
//    return 0;
//}