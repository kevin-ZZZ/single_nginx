//created on 11.11/2018
#include <sys/time.h>
#include <sys/types.h>
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/sendfile.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <net/if.h>
#include <fcntl.h>
#include <time.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <assert.h>
#include <signal.h>
#include <sys/epoll.h>
#include <pthread.h>
#include <errno.h>

#define MAX_EPOLL_FD 4096
#define MAX_BUF_SIZE (1 << 20)
#define WORKER_COUNT 2
#define LISTEN_PORT 8000
#define FORWORD_PORT 9000
#define FORWORD_IP "127.0.0.1"

int ep_fd[WORKER_COUNT];     // epoll instace, 每个worker独立
int listen_fd;               // 用来监听requests的socket对应的文件描述符
int g_shutdown_flag;         // 通过signal的方式标记是否要shutdown
int g_quiet;                 // 是否输出log
int g_static;                // 是否作为静态文件服务器
FILE *g_logger;              // 全局的logger
int g_pipe[WORKER_COUNT][2]; // 通过pipe的方式进程间通信

struct thread_data_t
{
    int ep_fd;   // epoll instace fd
    int pipe_fd; // pipe fd，通过它获取connection
};

// 处理io的数据结构，每个client创建一个
struct io_data_t
{
    int client_fd;           // client对应socket的文件描述符
    int forward_fd;          // 用于转发请求的socket对应的文件描述符
    int in_fd;               // 当前Input fd
    int out_fd;              // 当前Output fd
    struct sockaddr_in addr; // client的地址
    char *in_buf;            // Input数据的buffer
    char *out_buf;           // Output数据的buffer
    int in_buf_cur;          // in_buf当前写到哪个位置了
    int out_buf_cur;         // out_buf当前输出到哪个位置了
    int out_buf_total;       // out_buf总共的长度
    int left_out_bytes;      // Output数据还剩余多少字节
};

static void *handle_event_loop(void *param);

static void mylog(const char *fmt, ...);

/* 设置fd为non-blocking */
static void setnonblocking(int fd)
{
    int opts;
    opts = fcntl(fd, F_GETFL);
    if (opts < 0)
    {
        fprintf(stderr, "fcntl failed\n");
        return;
    }
    opts = opts | O_NONBLOCK;
    if (fcntl(fd, F_SETFL, opts) < 0)
    {
        fprintf(stderr, "fcntl failed\n");
        return;
    }
    return;
}

static void usage()
{
    printf("usage: proxy-server -l <ip> -p <port> [-q quiet] \n");
}

/* 为每个client分配一个关联的数据结构，用于控制输入输出 */
static struct io_data_t *alloc_io_data(int client_fd, struct sockaddr_in *client_addr)
{
    struct io_data_t *io_data_ptr = (struct io_data_t *)malloc(sizeof(struct io_data_t));
    io_data_ptr->client_fd = client_fd;
    io_data_ptr->forward_fd = -1; // -1表示转发fd当前不可用
    io_data_ptr->in_fd = client_fd;
    io_data_ptr->out_fd = -1; // -1表示当前不可写
    io_data_ptr->left_out_bytes = 0;
    io_data_ptr->in_buf = (char *)malloc(MAX_BUF_SIZE);
    io_data_ptr->out_buf = (char *)malloc(MAX_BUF_SIZE);
    io_data_ptr->in_buf_cur = 0;
    io_data_ptr->out_buf_cur = 0;
    io_data_ptr->out_buf_total = 0;
    if (client_addr)
        io_data_ptr->addr = *client_addr;
    return io_data_ptr;
}

/* 释放内存 */
static void destroy_io_data(struct io_data_t *io_data_ptr)
{
    if (NULL == io_data_ptr)
        return;
    if (io_data_ptr->in_buf)
        free(io_data_ptr->in_buf);
    if (io_data_ptr->out_buf)
        free(io_data_ptr->out_buf);
    io_data_ptr->in_buf = NULL;
    io_data_ptr->out_buf = NULL;
    free(io_data_ptr);
}

/* 退出函数 */
void exit_hook(int number)
{
    close(listen_fd);
    g_shutdown_flag = 1;
    printf(">> [%d] will shutdown...[%d]\n", getpid(), number);
}

int main(int argc, char **argv)
{
    const char *ip_binding = "0.0.0.0";
    int port_listening = LISTEN_PORT;
    int opt;
    int on = 1;

    int client_fd = 0;
    int worker_count = WORKER_COUNT, i;
    register int worker_pointer = 0;

    struct sockaddr_in server_addr;

    pthread_t tid[WORKER_COUNT];
    pthread_attr_t tattr[WORKER_COUNT];
    struct thread_data_t tdata[WORKER_COUNT];

    char ip_buf[256] = {0};
    struct sockaddr_in client_addr;
    socklen_t client_n;

    g_shutdown_flag = 0;
    g_quiet = 0;
    g_static = 0;
    // 获取命令行参数
    while ((opt = getopt(argc, argv, "l:p:hqs")) != -1)
    {
        switch (opt)
        {
        case 'l':
            ip_binding = strdup(optarg);
            break;
        case 'p':
            port_listening = atoi(optarg);
            if (port_listening == 0)
            {
                printf(">> invalid port : %s\n", optarg);
                exit(1);
            }
            break;
        case 'q':
            g_quiet = 1;
            break;
        case 's':
            g_static = 1;
            break;
        case 'h':
            usage();
            return 1;
        }
    }
    printf(">> IP listening:%s\n", ip_binding);
    printf(">> port: %d\n", port_listening);
    printf(">> quite:%d\n", g_quiet);
    printf(">> as static file server:%d\n", g_static);

    g_logger = fopen("proxy-server.log", "a");
    if (g_logger == NULL)
    {
        perror("create log file proxy-server.log failed.");
        exit(1);
    }

    // 设置信号中断
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, exit_hook);
    signal(SIGKILL, exit_hook);
    signal(SIGQUIT, exit_hook);
    signal(SIGTERM, exit_hook);
    signal(SIGHUP, exit_hook);

    // 创建pipe用于进程通信
    for (i = 0; i < WORKER_COUNT; i++)
    {
        if (pipe(g_pipe[i]) < 0)
        {
            perror("failed to create pipe");
            exit(1);
        }
    }

    // 创建listening socket
    listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (-1 == listen_fd)
    {
        perror("listen faild!");
        exit(-1);
    }
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    setsockopt(listen_fd, IPPROTO_TCP, TCP_NODELAY, (int[]){1}, sizeof(int));
    setsockopt(listen_fd, IPPROTO_TCP, TCP_QUICKACK, (int[]){1}, sizeof(int));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((short)port_listening);
    server_addr.sin_addr.s_addr = inet_addr(ip_binding);

    if (-1 == bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)))
    {
        perror("bind error");
        exit(-1);
    }

    if (-1 == listen(listen_fd, 32))
    {
        perror("listen error");
        exit(-1);
    }

    // 每个worker创建一个epoll instance，每个worker创建一个线程
    for (i = 0; i < worker_count; i++)
    {
        ep_fd[i] = epoll_create(MAX_EPOLL_FD);
        if (ep_fd[i] < 0)
        {
            perror("epoll_create failed.");
            exit(-1);
        }
    }

    for (i = 0; i < worker_count; i++)
    {
        pthread_attr_init(tattr + i);
        pthread_attr_setdetachstate(tattr + i, PTHREAD_CREATE_JOINABLE);
        tdata[i].ep_fd = ep_fd[i];
        tdata[i].pipe_fd = g_pipe[i][0];
        if (pthread_create(tid + i, tattr + i, handle_event_loop, tdata + i) != 0)
        {
            fprintf(stderr, "pthread_create failed\n");
            return -1;
        }
    }

    // 接受incoming connections，并通过之前创建的pipe分发到worker线程
    while (1)
    {
        if ((client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &client_n)) > 0)
        {
            if (write(g_pipe[worker_pointer][1], (char *)&client_fd, 4) < 0)
            {
                perror("failed to write client_fd into pipe");
                exit(1);
            }
            inet_ntop(AF_INET, &client_addr.sin_addr, ip_buf, sizeof(ip_buf));
            mylog("[CONN]Connection from %s", ip_buf);
            worker_pointer++;
            if (worker_pointer == worker_count)
                worker_pointer = 0;
        }
        else if (errno == EBADF && g_shutdown_flag)
        {
            break;
        }
        else
        {
            if (0 == g_shutdown_flag)
            {
                perror("please check ulimit -n");
                sleep(1);
            }
        }
    }

    // 清理工作
    for (i = 0; i < worker_count; i++)
    {
        close(ep_fd[i]);
    }

    if (client_fd < 0 && 0 == g_shutdown_flag)
    {
        perror("Accept failed, try ulimit -n");
        mylog("[ERROR]too many fds open, try ulimit -n");
        g_shutdown_flag = 1;
    }
    fclose(g_logger);
    printf(">> [%d]waiting worker thread....\n", getpid());

    for (i = 0; i < worker_count; i++)
        pthread_join(tid[i], NULL);

    printf(">> [%d]Bye~\n", getpid());
    return 0;
}

/* 关闭client以及一些清理工作 */
static void destroy_fd(int ep_fd, struct io_data_t *client_ptr, int case_no)
{
    struct epoll_event ev;
    ev.data.ptr = client_ptr;
    epoll_ctl(ep_fd, EPOLL_CTL_DEL, client_ptr->client_fd, &ev);
    shutdown(client_ptr->client_fd, SHUT_RDWR);
    close(client_ptr->client_fd);
    if (client_ptr->forward_fd > 0)
    {
        epoll_ctl(ep_fd, EPOLL_CTL_DEL, client_ptr->forward_fd, &ev);
        shutdown(client_ptr->forward_fd, SHUT_RDWR);
        close(client_ptr->forward_fd);
    }
    destroy_io_data(client_ptr);
    mylog("[DEBUG] close case %d, client fd: %d", case_no, client_ptr->client_fd);
}

static void mylog(const char *fmt, ...)
{
    if (0 == g_quiet)
    {
        char msg[4096];
        char buf[64];
        time_t now = time(NULL);
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(msg, sizeof(msg), fmt, ap);
        va_end(ap);
        strftime(buf, sizeof(buf), "%d %b %H:%M:%S", localtime(&now));
        fprintf(g_logger, "[%d] %s %s\n", (int)getpid(), buf, msg);
        fflush(g_logger);
    }
}

static void handle_output(int ep_fd, struct io_data_t *client_io_ptr)
{
    int fd, ret, case_no;
    struct epoll_event ev;

    fd = client_io_ptr->out_fd;
    if (fd < 0)
        return;

    // out_buf_total为0表示第一次输出，或者前面的out_buf已经输出完毕，因此从in_buf里拷贝过去
    if (client_io_ptr->out_buf_total == 0)
    {
        if (client_io_ptr->in_buf_cur == 0)
            return;
        memcpy(client_io_ptr->out_buf, client_io_ptr->in_buf, client_io_ptr->in_buf_cur);
        client_io_ptr->out_buf_total = client_io_ptr->in_buf_cur;
        client_io_ptr->out_buf_cur = 0;
        client_io_ptr->in_buf_cur = 0;
    }

    ret = send(fd, client_io_ptr->out_buf + client_io_ptr->out_buf_cur,
               client_io_ptr->out_buf_total - client_io_ptr->out_buf_cur, MSG_NOSIGNAL);
    if (ret >= 0)
    {
        client_io_ptr->out_buf_cur += ret;
        client_io_ptr->left_out_bytes -= ret;
    }

    mylog("[DEBUG]handle_output of fd %d: out_buf_cur %d, out_buf_total %d, left_out_bytes %d",
        fd, client_io_ptr->out_buf_cur, client_io_ptr->out_buf_total, client_io_ptr->left_out_bytes);

    if (0 == ret || (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK))
    {
        case_no = 2;
        destroy_fd(ep_fd, client_io_ptr, case_no);
        return;
    }
    if (client_io_ptr->out_buf_cur == client_io_ptr->out_buf_total)
    {
        mylog("[NOTICE] one buffer messages have been sent.(%d bytes)", client_io_ptr->out_buf_total);

        client_io_ptr->out_buf_cur = 0;
        client_io_ptr->out_buf_total = 0;
        if (client_io_ptr->left_out_bytes == 0)
        {
            // 所有output数据都已经发送完毕
            if (client_io_ptr->out_fd == client_io_ptr->forward_fd)
            {
                client_io_ptr->in_fd = client_io_ptr->forward_fd;
                client_io_ptr->out_fd = -1;
                ev.data.ptr = client_io_ptr;
                ev.events = EPOLLIN;
                epoll_ctl(ep_fd, EPOLL_CTL_MOD, client_io_ptr->in_fd, &ev);
            }
            else
            {
                case_no = 4;
                destroy_fd(ep_fd, client_io_ptr, case_no);
            }
        }
    }
}

static void forward_request(int efd, struct io_data_t *client_io_ptr)
{
    int sock_cli = socket(AF_INET, SOCK_STREAM, 0);
    int s;
    struct epoll_event event;

    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(FORWORD_PORT);
    servaddr.sin_addr.s_addr = inet_addr(FORWORD_IP);

    ///连接服务器，成功返回0，错误返回-1
    if (connect(sock_cli, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0)
    {
        mylog("[ERROR]failed to forword request of client %d", client_io_ptr->client_fd);
        exit(1);
    }
    mylog("[INFO]new forward fd %d for client %d", sock_cli, client_io_ptr->client_fd);

    setnonblocking(sock_cli);
    client_io_ptr->forward_fd = sock_cli;
    client_io_ptr->out_fd = sock_cli;
    client_io_ptr->in_fd = -1;
    event.data.ptr = client_io_ptr;
    event.events = EPOLLOUT;
    s = epoll_ctl(efd, EPOLL_CTL_ADD, sock_cli, &event);
    if (s == -1)
    {
        mylog("[ERROR]Failed to add forward fd %d to epoll instance", sock_cli);
        abort();
    }

    handle_output(efd, client_io_ptr);
}

static void handle_input(int ep_fd,
                         struct io_data_t *client_io_ptr)
{
    int i;
    int len = 0;
    int ret = 0;
    int case_no = 0;
    char *sep = NULL;
    const char *CRLF = "\r\n\r\n";
    const char *LF = "\n\n";
    const char *CONTENT_LENGTH = "Content-Length:";
    const char *sep_flag = NULL;
    struct epoll_event ev;
    int fd;
    const char *rsps_msg_fmt = "HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: Keep-Alive\r\nContent-Type: text/plain\r\n\r\n%s";
    const char *body = "Hello world!";

    fd = client_io_ptr->in_fd;
    if (fd < 0)
        return;

    assert(client_io_ptr->in_buf_cur >= 0);
    // TODO: in_buf maybe full
    ret = recv(fd, client_io_ptr->in_buf + client_io_ptr->in_buf_cur,
               512, MSG_DONTWAIT);
    if (0 == ret && fd == client_io_ptr->forward_fd) // 可能对方关掉了
    {
        return;
    }
    else if (0 == ret || (ret < 0 && errno != EAGAIN && errno != EWOULDBLOCK))
    {
        mylog("[ERROR]Input error with ret: %d, errno: %d, fd: %d", ret, errno, fd);
        case_no = 1;
        destroy_fd(ep_fd, client_io_ptr, case_no);
        return;
    }

    mylog("[INFO]receive %d bytes from fd %d", ret, fd);
    client_io_ptr->in_buf_cur += ret;
    client_io_ptr->in_buf[client_io_ptr->in_buf_cur] = '\0';

    // out_fd>=0表示response的header已经读完，如果还有body，则body只需转发给client即可
    if (client_io_ptr->out_fd >= 0)
    {
        return;
    }

    // 判断header部分是否读完了，没读完则直接返回
    sep = strstr(client_io_ptr->in_buf, CRLF);
    if (NULL == sep)
    {
        sep = strstr(client_io_ptr->in_buf, LF);
        if (NULL == sep)
            return; // waiting for the fd to be readable again
        else
            sep_flag = LF;
    }
    else
    {
        sep_flag = CRLF;
    }

    // 如果作为static文件处理器则不需要
    if (g_static == 1)
    {
        sprintf(client_io_ptr->in_buf, rsps_msg_fmt, strlen(body), body);
        client_io_ptr->in_buf_cur = strlen(client_io_ptr->in_buf);
        client_io_ptr->left_out_bytes = client_io_ptr->in_buf_cur;
        ev.events = EPOLLOUT;
        ev.data.ptr = client_io_ptr;
        epoll_ctl(ep_fd, EPOLL_CTL_MOD, client_io_ptr->client_fd, &ev);
        client_io_ptr->out_fd = client_io_ptr->client_fd;
        client_io_ptr->in_fd = -1;
        return;
    }

    // 需要转发，当in_fd为client fd时，直接转发
    if (client_io_ptr->in_fd == client_io_ptr->client_fd)
    {
        client_io_ptr->in_buf_cur = sep - client_io_ptr->in_buf + strlen(sep_flag);
        client_io_ptr->in_buf[client_io_ptr->in_buf_cur] = '\0';
        client_io_ptr->left_out_bytes = client_io_ptr->in_buf_cur;
        forward_request(ep_fd, client_io_ptr);
    }
    // 否则当in_fd为forward_fd时，表示当前正在读取upstream server的response
    else if (client_io_ptr->in_fd == client_io_ptr->forward_fd)
    {
        // 计算content length长度
        i = strstr(client_io_ptr->in_buf, CONTENT_LENGTH) - client_io_ptr->in_buf;
        for (i = i + strlen(CONTENT_LENGTH); i < client_io_ptr->in_buf_cur; i++)
        {
            if (client_io_ptr->in_buf[i] >= '0' && client_io_ptr->in_buf[i] <= '9')
            {
                len = len * 10 + (client_io_ptr->in_buf[i] - '0');
            }
            else if (len > 0)
            {
                break;
            }
        }
        client_io_ptr->left_out_bytes = len + (sep - client_io_ptr->in_buf) + strlen(sep_flag);
        // 可以一边读入upstream的response，一边将这个response转发给client_fd
        if (client_io_ptr->out_fd < 0)
        {
            ev.events = EPOLLOUT;
            ev.data.ptr = client_io_ptr;
            epoll_ctl(ep_fd, EPOLL_CTL_MOD, client_io_ptr->client_fd, &ev);
            client_io_ptr->out_fd = client_io_ptr->client_fd;
        }
    }
}

/* finally the event loop */
static void *handle_event_loop(void *param)
{
    register int i;
    int nfds, case_no, new_sock_fd;
    struct epoll_event events[MAX_EPOLL_FD], ev;
    struct io_data_t *client_io_ptr;
    struct thread_data_t my_tdata = *(struct thread_data_t *)param;

    /* 监听pipe的read事件 */
    ev.data.fd = my_tdata.pipe_fd;
    ev.events = EPOLLIN;
    epoll_ctl(my_tdata.ep_fd, EPOLL_CTL_ADD, my_tdata.pipe_fd, &ev);

    while (1)
    {
        nfds = epoll_wait(my_tdata.ep_fd, events, MAX_EPOLL_FD, 1000);
        //printf("nfds:%d, epoll fd:%d\n",nfds,my_tdata.ep_fd);
        if (nfds <= 0 && 0 != g_shutdown_flag)
        {
            break;
        }
        for (i = 0; i < nfds && nfds > 0; i++)
        {
            /* 判断是否是新的connection事件 */
            if ((events[i].data.fd == my_tdata.pipe_fd) && (events[i].events & EPOLLIN))
            {
                if (read(my_tdata.pipe_fd, &new_sock_fd, 4) == -1)
                {
                    perror("faild to read client_fd from pipe");
                    exit(1);
                }
                setnonblocking(new_sock_fd);
                mylog("[ACCP]accept fd %d", new_sock_fd);
                ev.data.ptr = alloc_io_data(new_sock_fd, (struct sockaddr_in *)NULL);
                ev.events = EPOLLIN;
                epoll_ctl(my_tdata.ep_fd, EPOLL_CTL_ADD, new_sock_fd, &ev);
                continue;
            }
            client_io_ptr = (struct io_data_t *)events[i].data.ptr;
            if (client_io_ptr->client_fd <= 0)
                continue;

            if (events[i].events & EPOLLIN)
            {
                handle_input(my_tdata.ep_fd, client_io_ptr);
            }
            else if (events[i].events & EPOLLOUT)
            {
                handle_output(my_tdata.ep_fd, client_io_ptr);
            }
            else if (events[i].events & EPOLLERR)
            {
                case_no = 3;
                destroy_fd(my_tdata.ep_fd, client_io_ptr, case_no);
            }
        }
    }
    return NULL;
}
