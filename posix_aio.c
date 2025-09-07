#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <aio.h>
#include <errno.h>
#include <time.h>

static long long timespec_to_ns(const struct timespec *t)
{
    return (long long)t->tv_sec * 1000000000LL + t->tv_nsec;
}

int main(void)
{
    const char *filename = "posix_aio.txt";
    size_t write_size = 4096;      // 4 KB per write
    size_t total_size = 32*1024 * 1024; // 32 MB total
    size_t iterations = total_size / write_size;

    int fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) { perror("open"); return 1; }

    // Allocate buffer and fill with 'A'
    unsigned char *buffer = malloc(write_size);
    memset(buffer,'A', write_size);

    struct aiocb cb;
    struct timespec tstart, tend;

    if (clock_gettime(CLOCK_MONOTONIC, &tstart) < 0) {
        perror("clock_gettime"); return 1;
    }

    for (size_t i = 0; i < iterations; i++) {
        memset(&cb, 0, sizeof(cb));
        cb.aio_fildes = fd;
        cb.aio_buf = buffer;
        cb.aio_nbytes = write_size;
        cb.aio_offset = i * write_size;

        if (aio_write(&cb) < 0) { perror("aio_write"); return 1; }

        // Wait until this write completes
        const struct aiocb *list[1] = { &cb };
        if (aio_suspend(list, 1, NULL) < 0) { perror("aio_suspend"); return 1; }

        // Check error status
        int err = aio_error(&cb);
        if (err != 0) { fprintf(stderr, "aio_error: %s\n", strerror(err)); return 1; }

        ssize_t written = aio_return(&cb);
        if (written != (ssize_t)write_size) {
            fprintf(stderr, "aio_return short write\n"); return 1;
        }
    }

    if (clock_gettime(CLOCK_MONOTONIC, &tend) < 0) {
        perror("clock_gettime"); return 1;
    }

    long long total_ns = timespec_to_ns(&tend) - timespec_to_ns(&tstart);
    printf("Wrote %zu KB in %.3f ms using AIO\n",
           total_size/(1024*1024), total_ns / 1e6);

    close(fd);
    free(buffer);

    return 0;
}
