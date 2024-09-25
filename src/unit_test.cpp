//
// Created by Pepephant on 2024/7/26.
//

#include "unit_test.h"
#include <gtest/gtest.h>

const char *server_host = "127.0.0.1";
int server_port = PORT_DEFAULT;

TEST(AriesTest, SmallTest) {
    int sockfd = init_tcp_sock(server_host, server_port);
    std::string sql;

    int scale = 200;
    const int n = 8;
    sql = "create table warehouse(w_id int, w_s char(500));";
    send_recv_sql(sockfd, sql);
    sql = "create table stock(s_id int, s_s char(500));";
    send_recv_sql(sockfd, sql);

    for (int i = 0; i < scale * 3.5; i++) {
        std::stringstream ss;
        if (i % scale == 0) {
            sql = "begin;";
            send_recv_sql(sockfd, sql);
        } else if (i % scale == scale - 1) {
            sql = "commit;";
            send_recv_sql(sockfd, sql);
            sql = "select * from warehouse;";
            send_recv_sql(sockfd, sql);
            sql = "select * from stock;";
            send_recv_sql(sockfd, sql);
        }

        switch (i % n) {
            case 0: ss << "insert into warehouse values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 1: ss << "insert into stock values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 2: ss << "update warehouse set w_id = " << i / n << " where w_id = " << int(i / 1.5 / n) << ";"; break;
            case 3: ss << "update stock set s_id = " << i / n << " where s_id = " << int(i / 1.5 / n) << ";"; break;
            case 4: ss << "delete from warehouse where w_id = " << i / 2 / n << ";"; break;
            case 5: ss << "delete from stock where s_id = " << i / 2 / n << ";"; break;
            case 6: ss << "select * from stock where s_id = "<< i / n << ";"; break;
            case 7: ss << "select * from warehouse where w_id = "<< i / n << ";"; break;
            default: break;
        }

        sql = ss.str();
        send_recv_sql(sockfd, sql);
    }
}

TEST(AriesTest, RandomTest) {
    int sockfd = init_tcp_sock(server_host, server_port);
    std::string sql;

    int scale = 2000;
    const int n = 8;
    sql = "create table warehouse(w_id int, w_s char(500));";
    send_recv_sql(sockfd, sql);
    sql = "create table stock(s_id int, s_s char(500));";
    send_recv_sql(sockfd, sql);

    for (int i = 0; i < scale * 3.5; i++) {
        std::stringstream ss;
        if (i % scale == 0) {
            sql = "begin;";
            send_recv_sql(sockfd, sql);
        } else if (i % scale == scale - 1) {
            sql = "commit;";
            send_recv_sql(sockfd, sql);
            sql = "select * from warehouse;";
            send_recv_sql(sockfd, sql);
            sql = "select * from stock;";
            send_recv_sql(sockfd, sql);
        }

        int cond = (i < n) ? 0 : rand() % (i / n);

        switch (i % n) {
            case 0: ss << "insert into warehouse values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 1: ss << "insert into stock values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 2: ss << "update warehouse set w_id = " << i / n << " where w_id = " << cond << ";"; break;
            case 3: ss << "update stock set s_id = " << i / n << " where s_id = " << cond << ";"; break;
            case 4: ss << "delete from warehouse where w_id = " << cond << ";"; break;
            case 5: ss << "delete from stock where s_id = " << cond << ";"; break;
            case 6: ss << "select * from stock where s_id = "<< cond << ";"; break;
            case 7: ss << "select * from warehouse where w_id = "<< cond << ";"; break;
            default: break;
        }

        sql = ss.str();
        send_recv_sql(sockfd, sql);
    }
}

TEST(AriesTest, CheckRedo) {
    int sockfd = init_tcp_sock(server_host, server_port);
    std::string sql;

    int scale = 2000;
    const int n = 8;
    sql = "create table warehouse(w_id int, w_s char(500));";
    send_recv_sql(sockfd, sql);
    sql = "create table stock(s_id int, s_s char(500));";
    send_recv_sql(sockfd, sql);

    for (int i = 0; i < scale * 3.5; i++) {
        std::stringstream ss;
        if (i % scale == 0) {
            sql = "begin;";
            send_recv_sql(sockfd, sql);
        } else if (i % scale == scale - 1) {
            sql = "commit;";
            send_recv_sql(sockfd, sql);
            sql = "select * from warehouse;";
            send_recv_sql(sockfd, sql);
            sql = "select * from stock;";
            send_recv_sql(sockfd, sql);
        }

        int cond = (i < n) ? 0 : rand() % (i / n);

        switch (i % n) {
            case 0: ss << "insert into warehouse values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 1: ss << "insert into stock values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 2: ss << "update warehouse set w_id = " << i / n << " where w_id = " << cond << ";"; break;
            case 3: ss << "update stock set s_id = " << i / n << " where s_id = " << cond << ";"; break;
            case 4: ss << "delete from warehouse where w_id = " << cond << ";"; break;
            case 5: ss << "delete from stock where s_id = " << cond << ";"; break;
            case 6: ss << "select * from stock where s_id = "<< cond << ";"; break;
            case 7: ss << "select * from warehouse where w_id = "<< cond << ";"; break;
            default: break;
        }

        sql = ss.str();
        send_recv_sql(sockfd, sql);
    }

    sql = "commit;";
    send_recv_sql(sockfd, sql);
    sql = "begin";
    send_recv_sql(sockfd, sql);
    sql = "select * from warehouse;";
    send_recv_sql(sockfd, sql);
    sql = "select * from stock;";
    send_recv_sql(sockfd, sql);
    sql = "commit;";
    send_recv_sql(sockfd, sql);
}

TEST(AriesTest, CheckPoint) {
    int sockfd = init_tcp_sock(server_host, server_port);
    std::string sql;

    int scale = 2000;
    const int n = 8;
    sql = "create table warehouse(w_id int, w_s char(500));";
    send_recv_sql(sockfd, sql);
    sql = "create table stock(s_id int, s_s char(500));";
    send_recv_sql(sockfd, sql);

    for (int i = 0; i < scale * 3.5; i++) {
        std::stringstream ss;
        if (i % scale == 0) {
            sql = "begin;";
            send_recv_sql(sockfd, sql);
        } else if (i % scale == scale - 1) {
            sql = "commit;";
            send_recv_sql(sockfd, sql);
            sql = "select * from warehouse;";
            send_recv_sql(sockfd, sql);
            sql = "select * from stock;";
            send_recv_sql(sockfd, sql);
        }

        if (i % scale == scale * 0.25) {
            sql = "create static checkpoint;";
            send_recv_sql(sockfd, sql);
        }

        int cond = (i < n) ? 0 : rand() % (i / n);

        switch (i % n) {
            case 0: ss << "insert into warehouse values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 1: ss << "insert into stock values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 2: ss << "update warehouse set w_id = " << i / n << " where w_id = " << cond << ";"; break;
            case 3: ss << "update stock set s_id = " << i / n << " where s_id = " << cond << ";"; break;
            case 4: ss << "delete from warehouse where w_id = " << cond << ";"; break;
            case 5: ss << "delete from stock where s_id = " << cond << ";"; break;
            case 6: ss << "select * from stock where s_id = "<< cond << ";"; break;
            case 7: ss << "select * from warehouse where w_id = "<< cond << ";"; break;
            default: break;
        }

        sql = ss.str();
        send_recv_sql(sockfd, sql);
    }
}

TEST(AriesTest, CkpRedo) {
    int sockfd = init_tcp_sock(server_host, server_port);
    std::string sql;

    int scale = 2000;
    const int n = 8;
    sql = "create table warehouse(w_id int, w_s char(500));";
    send_recv_sql(sockfd, sql);
    sql = "create table stock(s_id int, s_s char(500));";
    send_recv_sql(sockfd, sql);

    for (int i = 0; i < scale * 3.5; i++) {
        std::stringstream ss;
        if (i % scale == 0) {
            sql = "begin;";
            send_recv_sql(sockfd, sql);
        } else if (i % scale == scale - 1) {
            if (i / scale == 1) {
                sql = "abort;";
                send_recv_sql(sockfd, sql);
            } else {
                sql = "commit;";
                send_recv_sql(sockfd, sql);
            };
            sql = "select * from warehouse;";
            send_recv_sql(sockfd, sql);
            sql = "select * from stock;";
            send_recv_sql(sockfd, sql);
        }

        if (i % scale == scale * 0.25) {
            sql = "create static checkpoint;";
            send_recv_sql(sockfd, sql);
        }

        int cond = (i < n) ? 0 : rand() % (i / n);

        switch (i % n) {
            case 0: ss << "insert into warehouse values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 1: ss << "insert into stock values (" << i / n << ", '" << i / n * 1000 << "');"; break;
            case 2: ss << "update warehouse set w_id = " << i / n << " where w_id = " << cond << ";"; break;
            case 3: ss << "update stock set s_id = " << i / n << " where s_id = " << cond << ";"; break;
            case 4: ss << "delete from warehouse where w_id = " << cond << ";"; break;
            case 5: ss << "delete from stock where s_id = " << cond << ";"; break;
            case 6: ss << "select * from stock where s_id = "<< cond << ";"; break;
            case 7: ss << "select * from warehouse where w_id = "<< cond << ";"; break;
            default: break;
        }

        sql = ss.str();
        send_recv_sql(sockfd, sql);
    }

    sql = "commit;";
    send_recv_sql(sockfd, sql);
    sql = "begin";
    send_recv_sql(sockfd, sql);
    sql = "select * from warehouse;";
    send_recv_sql(sockfd, sql);
    sql = "select * from stock;";
    send_recv_sql(sockfd, sql);
    sql = "commit;";
    send_recv_sql(sockfd, sql);
}