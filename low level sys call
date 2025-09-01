#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stddef.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#define BUFFER_SIZE 4096
int main(int argc, char *argv[])
{
    if (argc != 3) {
        printf("Usage: %s <source_file> <dest_file>\n", argv[0]);
        exit(0);
    }


int src_fd,dest_fd;
ssize_t nread,nwrite;
char buffer[BUFFER_SIZE];

//open src file
src_fd = open(argv[1],O_RDONLY);
if(src_fd < 0)
{
    perror("open");
    return 1;
}

//open dest file in write mode
dest_fd = open(argv[2], O_CREAT | O_WRONLY | O_TRUNC, 0644);
if(dest_fd < 0)
{
    perror("open");
    return 1;
}

//copy contents
while((nread = read(src_fd,buffer,BUFFER_SIZE)) > 0)
{
    nwrite = write(dest_fd,buffer,nread);
    if(nwrite<0)
    {
        perror("write");
        close(dest_fd);
        return 1;
    }
    if(nread!=nwrite)
    {
        perror("write");
        close(src_fd);
        close(dest_fd);
        return 1;
    }
}

if(nread < 0)
{
    perror("read");
    close(src_fd);
    return 1;
}

char new_character = 'X';
if(lseek(dest_fd,6,SEEK_SET) == (off_t)-1)
{
    perror("lseek");
    return -1;
}
else
{
    //overwrite with char X
    if(write(dest_fd,"X",1)!=1)
    {
        perror("overwrite");
        close(dest_fd);
    }
}

close(src_fd);
close(dest_fd);

return 0;
}
