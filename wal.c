#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#define WAL_FILE "wal.log"
#define DB_FILE  "db.txt"
#define BUF_SIZE 1024
#define MAX_KEYS 100

static int txn_id = 1;

typedef struct {
    char key[BUF_SIZE];
    char value[BUF_SIZE];
} KeyValue;

static KeyValue keyValue_store[MAX_KEYS];
static int keyValue_count = 0;

int find_key(const char *key) {
    for (int i = 0; i < keyValue_count; i++) {
        if (strcmp(keyValue_store[i].key, key) == 0) return i;
    }
    return -1;
}

void set_keyValue(const char *key, const char *value) {
    int index = find_key(key);
    if (index >= 0) {
        strncpy(keyValue_store[index].value, value, BUF_SIZE-1);
        keyValue_store[index].value[BUF_SIZE-1] = '\0';
    } else {
        if (keyValue_count >= MAX_KEYS) {
            fprintf(stderr, "Too many keys\n");
            exit(1);
        }
        strncpy(keyValue_store[keyValue_count].key, key, BUF_SIZE-1);
        strncpy(keyValue_store[keyValue_count].value, value, BUF_SIZE-1);
        keyValue_store[keyValue_count].key[BUF_SIZE-1] = '\0';
        keyValue_store[keyValue_count].value[BUF_SIZE-1] = '\0';
        keyValue_count++;
    }
}

void sync_data(int do_fsync) {
    int fd = open(DB_FILE, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { perror("open"); 
        exit(1); 
    }

    char buf[BUF_SIZE];
    for (int i = 0; i < keyValue_count; i++) {
        int len = snprintf(buf, sizeof(buf), "%s=%s\n", keyValue_store[i].key, keyValue_store[i].value);
        if (write(fd, buf, len) != len) {
            perror("write"); 
            close(fd); 
            exit(1);
        }
    }

    if (do_fsync && fsync(fd) < 0) { 
        perror("fsync db"); 
        close(fd); 
        exit(1); 
    }
    close(fd);
}

void append_to_file(const char *filename, const char *data, int do_fsync) {
    int fd = open(filename, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) { perror("open"); 
        exit(1); 
    }
    ssize_t data_len = strlen(data);
    if (write(fd, data, data_len) != data_len) 
    { perror("write"); 
        close(fd); 
        exit(1); 
    }
    if (do_fsync && fsync(fd) < 0) { 
        perror("fsync"); 
        close(fd); 
        exit(1); 
    }
    close(fd);
}

int get_next_tid() {
    int fd = open(WAL_FILE, O_RDONLY);
    if (fd < 0) {
        return 1; // No WAL file exists yet
    }
    
    int max_tid = 0;
    char buf[BUF_SIZE];
    char line[BUF_SIZE];
    int line_pos = 0;
    ssize_t n;
    
    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        for (int i = 0; i < n; i++) {
            if (buf[i] == '\n') {
                line[line_pos] = '\0';
                
              
                if (strncmp(line, "TRANSACTION", 11) == 0) {
                    int tid;
                    if (sscanf(line, "TRANSACTION %d", &tid) == 1) {
                        if (tid > max_tid) max_tid = tid;
                    }
                }
                
                line_pos = 0;
            } else if (line_pos < BUF_SIZE - 1) {
                line[line_pos++] = buf[i];
            }
        }
    }
    
    // Handle case where file doesn't end with newline
    if (line_pos > 0) {
        line[line_pos] = '\0';
        if (strncmp(line, "TRANSACTION", 11) == 0) {
            int tid;
            if (sscanf(line, "TRANSACTION %d", &tid) == 1) {
                if (tid > max_tid) max_tid = tid;
            }
        }
    }
    
    close(fd);
    return max_tid + 1;
}

// Load existing DB into memory
void load_db() {
    int fd = open(DB_FILE, O_RDONLY);
    if (fd < 0) {
        return; 
    }
    
    char buf[BUF_SIZE];
    char line[BUF_SIZE];
    int line_pos = 0;
    ssize_t n;
    
    keyValue_count = 0; // Reset the count
    
    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        for (int i = 0; i < n; i++) {
            if (buf[i] == '\n') {
                line[line_pos] = '\0';
                
                // Parse key=value lines
                char *equals = strchr(line, '=');
                if (equals != NULL) {
                    *equals = '\0'; // Split at equals sign
                    char *key = line;
                    char *value = equals + 1;
                    set_keyValue(key, value);
                }
                
                line_pos = 0;
            } else if (line_pos < BUF_SIZE - 1) {
                line[line_pos++] = buf[i];
            }
        }
    }
    
    // Handle last line without newline
    if (line_pos > 0) {
        line[line_pos] = '\0';
        char *equals = strchr(line, '=');
        if (equals != NULL) {
            *equals = '\0';
            char *key = line;
            char *value = equals + 1;
            set_keyValue(key, value);
        }
    }
    
    close(fd);
}

void cmd_write(const char *key, const char *value, int do_fsync) {
    char buf[BUF_SIZE];
    int tid = txn_id++;

    snprintf(buf, sizeof(buf), "TRANSACTION %d BEGIN\n", tid);
    append_to_file(WAL_FILE, buf, do_fsync);

    snprintf(buf, sizeof(buf), "SET %s %s\n", key, value);
    append_to_file(WAL_FILE, buf, do_fsync);

    snprintf(buf, sizeof(buf), "TRANSACTION %d COMMIT\n", tid);
    append_to_file(WAL_FILE, buf, do_fsync);

    set_keyValue(key, value);
    sync_data(do_fsync);

    printf("Wrote (%s=%s) with%s fsync\n", key, value, do_fsync ? "" : "out");
}

void cmd_crash_after_wal(const char *key, const char *value) {
    char buf[BUF_SIZE];
    int tid = txn_id++;

    snprintf(buf, sizeof(buf), "TRANSACTION %d BEGIN\n", tid);
    append_to_file(WAL_FILE, buf, 1);

    snprintf(buf, sizeof(buf), "SET %s %s\n", key, value);
    append_to_file(WAL_FILE, buf, 1);

    snprintf(buf, sizeof(buf), "TRANSACTION %d COMMIT\n", tid);
    append_to_file(WAL_FILE, buf, 1);

    printf("Simulated crash AFTER WAL (before DB apply)\n");
    exit(1);
}

void cmd_recover() {
    int fd = open(WAL_FILE, O_RDONLY);
    if (fd < 0) {
        printf("No WAL file to recover from\n");
        return;
    }

    load_db();

    char buf[BUF_SIZE];
    char line[BUF_SIZE];
    int line_pos = 0;
    ssize_t n;
    
    int current_tid = 0;
    char current_key[BUF_SIZE] = "";
    char current_value[BUF_SIZE] = "";
    int in_transaction = 0;

    while ((n = read(fd, buf, sizeof(buf))) > 0) {
        for (int i = 0; i < n; i++) {
            if (buf[i] == '\n') {
                line[line_pos] = '\0';
                
                // Process the complete line
                if (strncmp(line, "TRANSACTION", 11) == 0) {
                    if (strstr(line, "BEGIN")) {
                        sscanf(line, "TRANSACTION %d BEGIN", &current_tid);
                        in_transaction = 1;
                        current_key[0] = '\0';
                        current_value[0] = '\0';
                    } else if (strstr(line, "COMMIT")) {
                        int tid;
                        sscanf(line, "TRANSACTION %d COMMIT", &tid);
                        if (in_transaction && tid == current_tid && current_key[0] != '\0') {
                            set_keyValue(current_key, current_value);
                        }
                        in_transaction = 0;
                    }
                } else if (in_transaction && strncmp(line, "SET ", 4) == 0) {
                    sscanf(line + 4, "%s %s", current_key, current_value);
                }
                
                line_pos = 0;
            } else if (line_pos < BUF_SIZE - 1) {
                line[line_pos++] = buf[i];
            }
        }
    }
    
    close(fd);
    sync_data(1);
    printf("Recovery done. Applied committed transactions.\n");
}

void cmd_display() {
    printf("---- WAL LOG ----\n");
    int fd = open(WAL_FILE, O_RDONLY);
    if (fd >= 0) {
        char buf[BUF_SIZE];
        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf))) > 0) {
            write(STDOUT_FILENO, buf, n);
        }
        close(fd);
    } else {
        printf("(empty)\n");
    }

    printf("\n---- DB FILE ----\n");
    fd = open(DB_FILE, O_RDONLY);
    if (fd >= 0) {
        char buf[BUF_SIZE];
        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf))) > 0) {
            write(STDOUT_FILENO, buf, n);
        }
        close(fd);
    } else {
        printf("(empty)\n");
    }
}

int main(int argc, char *argv[]) {
  
    load_db();
    txn_id = get_next_tid();

    if (argc < 2) {
        fprintf(stderr,
            "Usage:\n"
            "  %s write <key> <value>\n"
            "  %s write-nosync <key> <value>\n"
            "  %s crash-after-wal <key> <value>\n"
            "  %s recover\n"
            "  %s display\n",
            argv[0], argv[0], argv[0], argv[0], argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "write") == 0 && argc == 4) {
        cmd_write(argv[2], argv[3], 1);
    } else if (strcmp(argv[1], "write-nosync") == 0 && argc == 4) {
        cmd_write(argv[2], argv[3], 0);
    } else if (strcmp(argv[1], "crash-after-wal") == 0 && argc == 4) {
        cmd_crash_after_wal(argv[2], argv[3]);
    } else if (strcmp(argv[1], "recover") == 0) {
        cmd_recover();
    } else if (strcmp(argv[1], "display") == 0) {
        cmd_display();
    } else {
        fprintf(stderr, "Unknown command\n");
        return 1;
    }

    return 0;
}
