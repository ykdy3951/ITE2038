#include "buf.h"
#include <string.h>
#include <stdlib.h>

// Initialize buffer pool with given number and buffer manager.
int init_db(int num_buf)
{
    buf = (buf_t *)malloc(sizeof(buf_t) * num_buf);
    if (buf == NULL)
    {
        return 1;
    }
    buf_header = (buf_header_t *)malloc(sizeof(buf_header_t));
    buf_header->buf_size = num_buf;
    buf_header->used_size = 0;

    buf_header->head = -1;
    buf_header->tail = -1;

    for (int i = 0; i < num_buf; i++)
    {
        buf[i].is_dirty = 0;
        buf[i].is_pinned = 0;
        buf[i].prev = -1;
        buf[i].next = -1;
    }
    return 0;
}

int open_table(char *pathname)
{
}

int close_table(int table_id)
{
    for (int i = 0; i < buf_header->used_size; i++)
    {
        if (buf[i].table_id == table_id)
        {
            if (buf[i].is_dirty == 1 && buf[i].is_pinned == 0)
            {
                file_write_buf(table_id, buf[i].page_num);
            }
        }
    }
    return 0;
}

int shutdown_db(void)
{
    if (buf == NULL)
    {
        return 1;
    }
    else
    {
        for (int i = 0; i <= buf_header->used_size; i++)
        {
            if (buf[i].is_dirty && !buf[i].is_pinned)
            {
            }
        }
    }
}

void buf_read_page(int table_id, pagenum_t pagenum, page_t *dest)
{
    int i;
    // 기존의 buffer에 destination page가 already exist 할 때
    for (i = 0; i < buf_header->used_size; i++)
    {
        if (buf[i].table_id == table_id && buf[i].page_num == pagenum)
        {
            if (pagenum == 0)
            {
                dest = &buf[i].header_page;
            }
            else
            {
                dest = &buf[i].page;
            }
            return;
        }
    }
    // 새로 불러와야 할 때
    i = file_read_buf(table_id, pagenum);
    if (pagenum == 0)
    {
        dest = &buf[i].header_page;
    }
    else
    {
        dest = &buf[i].page;
    }
    return;
}

void buf_write_page(int table_id, pagenum_t pagenum, const page_t *src)
{
    for (int i = 0; i < buf_header->used_size; i++)
    {
        if (buf[i].table_id == table_id && buf[i].page_num == pagenum)
        {
            buf[i].is_dirty = 1;
            buf[i].is_pinned = 0;
            if (pagenum == 0)
            {
                buf[i].header_page = *(header_page_t *)src;
            }
            else
            {
                buf[i].page = *src;
            }
        }
    }
}

// buf_mgr와 file_mgr랑 통신하도록 하는 function
int file_read_buf(int table_id, pagenum_t pagenum)
{
    int buf_index;
    // if buffer is already full
    if (buf_header->used_size == buf_header->buf_size)
    {
        // LRU algorithm 구현하기
    }
    else
    {
        if (pagenum == 0)
        {
            file_read_page(table_id, pagenum, (page_t *)&buf[buf_header->used_size].header_page);
        }
        else
        {
            file_read_page(table_id, pagenum, &buf[buf_header->used_size].page);
        }
        buf_index = buf_header->used_size;
    }
    if (buf_header->used_size == 0)
    {
        buf_header->head = 0;
        buf_header->tail = 0;
    }
    else
    {
    }
    buf_index = buf_header->used_size++;
}

int file_write_buf(int table_id, pagenum_t pagenum)
{
    int buf_index;

    for (int i = 0; i < buf_header->used_size; i++)
    {
    }
}

void buf_free_page(int table_id, pagenum_t pagenum)
{
}

pagenum_t buf_alloc_page(int table_id)
{
}