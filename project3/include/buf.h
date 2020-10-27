#ifndef __BUF_H__
#define __BUF_H__

#include <file.h>

typedef struct bufferStructure
{
    union
    {
        header_page_t *header_page;
        page_t *page;
    };
    int table_id;
    pagenum_t page_num;
    int is_dirty;
    int is_pinned;
    int prev;
    int next;
} buf_t;

typedef struct free_buf_t
{
    int buf_index;
    struct free_buf_t *next;
} free_buf_t;

typedef struct bufferHeader
{
    int buf_size;
    int used_size;
    int head;
    int tail;
    free_buf_t *free;
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
int buf_read_page(int table_id, pagenum_t pagenum);
int buf_write_page(int buffer_index);
int buf_get_page(int buffer_index, int table_id, pagenum_t pagenum);
int buf_put_page(int index);
void buf_free_page(int table_id, pagenum_t pagenum);
int buf_alloc_page(int table_id);

buf_t *buf;
buf_header_t *buf_header;
table_t *table;

#endif