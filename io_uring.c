#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <liburing.h>


static inline long long ts_to_ns(const struct timespec *t) {
    return (long long)t->tv_sec * 1000000000LL + t->tv_nsec;
}

int main() {
    const char *filename = "io_uring.txt";

    size_t write_size = 4096;         // 4 KB per write
    size_t total_size = 8 * 1024 * 1024; // 8 MB total
    size_t iterations = total_size / write_size;

    // Open file
    int fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) { perror("open"); return 1; }

    // Aligned buffer
    unsigned char *buffer = aligned_alloc(4096, write_size);
    if (!buffer) { perror("aligned_alloc"); return 1; }
    memset(buffer, 'A', write_size);  // Fill with 'A'

    // Init io_uring
    struct io_uring ring;
    if (io_uring_queue_init(64, &ring, 0) < 0) {
        perror("io_uring_queue_init");
        return 1;
    }

    struct timespec start, end;
    long long total_ns = 0;

    for (size_t i = 0; i < iterations; i++) {
        off_t offset = i * write_size;

        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (!sqe) { fprintf(stderr, "get_sqe failed\n"); return 1; }

        io_uring_prep_write(sqe, fd, buffer, write_size, offset);

        clock_gettime(CLOCK_MONOTONIC, &start);

        io_uring_submit(&ring);

        struct io_uring_cqe *cqe;
        if (io_uring_wait_cqe(&ring, &cqe) < 0) {
            perror("wait_cqe");
            return 1;
        }

        if (cqe->res < 0) {
            fprintf(stderr, "Write error: %s\n", strerror(-cqe->res));
            return 1;
        }

        clock_gettime(CLOCK_MONOTONIC, &end);

        io_uring_cqe_seen(&ring, cqe);

        total_ns += ts_to_ns(&end) - ts_to_ns(&start);
    }

    double avg_ms = (double)total_ns / iterations / 1e6;
    printf("io_uring: wrote %zu MB in %zu ops (%.3f ms avg per op)\n",
           total_size / (1024*1024), iterations, avg_ms);

    io_uring_queue_exit(&ring);
    close(fd);
