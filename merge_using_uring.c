#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <liburing.h>


#define WORD_SIZE  4096    // Block size per read

static inline long long ts_ns(const struct timespec *t) {
    return (long long)t->tv_sec * 1000000000LL + t->tv_nsec;
}

int main(int argc, char **argv) {
    if (argc <= 2) {
        printf("Include at least 3 files:\n Usage - %s file1.txt file2.txt file3.txt\n", argv[0]);
        return 1;
    }

    int num_files = argc - 1;
    int fd[num_files];
    char *buffers[num_files];
    off_t offsets[num_files];

    struct io_uring ring;
    if (io_uring_queue_init(QD, &ring, 0) < 0) {
        perror("io_uring_queue_init");
        return 1;
    }

    // Open files and submit initial reads
    for (int i = 0; i < num_files; i++) {
        fd[i] = open(argv[i+1], O_RDONLY);
        if (fd[i] < 0) {
            perror("open");
            return 1;
        }

        buffers[i] = malloc(WORD_SIZE);
        offsets[i] = 0;

        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (!sqe) {
            fprintf(stderr, "io_uring_get_sqe returned NULL\n");
            return 1;
        }

        io_uring_prep_read(sqe, fd[i], buffers[i], WORD_SIZE, offsets[i]);
        io_uring_sqe_set_data(sqe, (void *)(long)i);  // store file index
    }

    io_uring_submit(&ring);

    int count = 0;
    while (count < num_files) {
        struct io_uring_cqe *cqe;
        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) {
            fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-ret));
            break;
        }

        int idx = (int)(long)io_uring_cqe_get_data(cqe);  // file index
        int res = cqe->res;
        io_uring_cqe_seen(&ring, cqe);

        if (res < 0) {
            fprintf(stderr, "Read error on %s: %s\n", argv[idx+1], strerror(-res));
            close(fd[idx]);
            free(buffers[idx]);
            count++;
            continue;
        }

        if (res == 0) {
            struct timespec ts;
            clock_gettime(CLOCK_MONOTONIC, &ts);
            printf("\n--- Completed file: %s at %lld ns ---\n",
                   argv[idx+1], ts_ns(&ts));
            close(fd[idx]);
            free(buffers[idx]);
            count++;
            continue;
        }

        // Merge content in completion order
        fwrite(buffers[idx], 1, res, stdout);

        offsets[idx] += res;

        // Submit next read for this file
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        io_uring_prep_read(sqe, fd[idx], buffers[idx], WORD_SIZE, offsets[idx]);
        io_uring_sqe_set_data(sqe, (void *)(long)idx);
        io_uring_submit(&ring);
    }

    io_uring_queue_exit(&ring);
    printf("\n All files merged in completion order.\n");
    return 0;
}
