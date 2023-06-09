#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "file.h"
#include "buf.h"

int open_table(char *path_name)
{
    // table이 없으면 새로 만든다.
    int ret = -1;
    if (table == NULL)
    {
        table = (table_t *)malloc(sizeof(table_t));
        if (table == NULL)
        {
            return error;
        }
        memset(table->fd_table, -1, sizeof(table->fd_table));
        table->num_of_open = 0;
        table->num_of_table = 0;
        for (int i = 1; i <= 10; i++)
        {
            table->table_path[i] = NULL;
        }
    }

    // 기존의 table인 경우
    for (int i = 1; i <= table->num_of_open; i++)
    {
        if (!strcmp(table->table_path[i], path_name))
        {
            // 한 번 close된 table일 경우
            if (table->fd_table[i] == -1)
            {
                ret = i;
                break;
            }
            // table이 존재하는 경우
            else
            {
                return i;
            }
        }
    }

    // 열렸던 table이 10개 이고 reopen의 대상이 아니라면 종료한다.
    if (table->num_of_open == 10 && ret == -1)
    {
        return error;
    }

    // 그렇지 않을 경우 page를 새로 만들거나 다시 등록한다.
    int fd = open(path_name, O_RDWR | O_SYNC | O_CREAT, 0777);
    if (fd < 0)
    {
        fd = -1;
        return error;
    }
    // mapping table에 등록하기
    // 새 table 일 경우 pathname 크기 만큼 할당하고 넣어준다.
    if (ret == -1)
    {
        ret = ++table->num_of_open;
        table->table_path[ret] = (char *)malloc(sizeof(char) * 20);
        strcpy(table->table_path[ret], path_name);
    }
    // 공통 수행 부분
    table->num_of_table++;
    table->fd_table[ret] = fd;

    header_page_t *header_page = (header_page_t *)malloc(page_size);
    memset(header_page, 0, sizeof(header_page_t));
    int flag = pread(fd, header_page, page_size, 0);

    // 새로 만들기
    if (flag != page_size || !header_page->number_of_pages)
    {
        header_page->root_page_number = 0;
        header_page->free_page_number = 0;
        header_page->number_of_pages = 1;
        file_write_page(ret, 0, (page_t *)header_page);
    }

    free(header_page);
    return ret;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int table_id)
{
    page_t *new_free;
    header_page_t *header_page = (header_page_t *)malloc(page_size);
    file_read_page(table_id, 0, (page_t *)header_page);

    pagenum_t free_num = header_page->free_page_number;
    new_free = (page_t *)malloc(page_size);

    if (free_num)
    {
        file_read_page(table_id, free_num, new_free);
        header_page->free_page_number = new_free->header.parent_page_number;
    }

    else
    {
        free_num = header_page->number_of_pages;
        header_page->number_of_pages++;

        page_t *page = (page_t *)malloc(page_size);
        memset(page, 0, page_size);
        file_write_page(table_id, free_num, page);
        free(page);
    }
    file_write_page(table_id, 0, (page_t *)header_page);
    // header_read();
    free(new_free);
    free(header_page);
    return free_num;
}

// Free an on-disk page to the free page list
void file_free_page(int table_id, pagenum_t pagenum)
{
    page_t *page = (page_t *)malloc(page_size);
    header_page_t *header_page = (header_page_t *)malloc(page_size);
    memset(page, 0, page_size);
    file_read_page(table_id, 0, (page_t *)header_page);
    page->header.parent_page_number = header_page->free_page_number;
    header_page->free_page_number = pagenum;

    file_write_page(table_id, pagenum, page);
    file_write_page(table_id, 0, (page_t *)header_page);
    free(page);
    free(header_page);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int table_id, pagenum_t pagenum, page_t *dest)
{
    int ret;
    ret = pread(table->fd_table[table_id], dest, page_size, pagenum * page_size);
    if (ret == error)
    {
        perror("pread");
    }
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int table_id, pagenum_t pagenum, const page_t *src)
{
    int ret;
    ret = pwrite(table->fd_table[table_id], src, page_size, pagenum * page_size);
    if (ret == error)
    {
        perror("pwrite");
    }
}