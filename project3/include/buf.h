#ifndef __BUF_H__
#define __BUF_H__

#include <file.h>

typedef struct bufferStructure
{
    union
    {
        header_page_t header_page;
        page_t page;
    };
    int table_id;
    pagenum_t page_num;
    int is_dirty;
    int is_pinned;
    int prev;
    int next;
} buf_t;

typedef struct bufferHeader
{
    int buf_size;
    int used_size;
    int head;
    int tail;
} buf_header_t;

typedef struct tableStruct
{
    int fd_table[11];
    char *table_path[11];
    int num_of_table;
} table_t;

int init_db(int num_buf);
int open_table(char *pathname);
int close_table(int table_id);
int shutdown_db(void);
void buf_read_page(int table_id, pagenum_t pagenum, page_t *dest);
void buf_write_page(int table_id, pagenum_t pagenum, const page_t *src);
int file_read_buf(int table_id, pagenum_t pagenum);
int file_write_buf(int table_id, pagenum_t pagenum);
void buf_free_page(int table_id, pagenum_t pagenum);
pagenum_t buf_alloc_page(int table_id);

buf_t *buf;
buf_header_t *buf_header;
table_t *table;

#endif