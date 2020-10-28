#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "file.h"

fd = -1;

int open_table(char *path_name)
{
    fd = open(path_name, O_RDWR | O_SYNC | O_CREAT, 0777);
    if (fd < 0)
    {
        fd = -1;
        return error;
    }
    header_page = (header_page_t *)malloc(page_size);
    memset(header_page, 0, sizeof(header_page_t));
    int flag = pread(fd, header_page, page_size, 0);
    if (flag != page_size || !header_page->number_of_pages)
    {
        header_page->root_page_number = 0;
        header_page->free_page_number = 0;
        header_page->number_of_pages = 1;
        header_write();
    }
    return fd;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page()
{
    page_t *new_free;

    header_read();

    pagenum_t free_num = header_page->free_page_number;
    new_free = (page_t *)malloc(sizeof(page_t));

    if (free_num)
    {
        file_read_page(free_num, new_free);
        header_page->free_page_number = new_free->header.parent_page_number;
    }

    else
    {
        free_num = header_page->number_of_pages;
        header_page->number_of_pages++;
    }
    header_write();
    header_read();
    free(new_free);

    return free_num;
}

// Free an on-disk page to the free page list
void file_free_page(pagenum_t pagenum)
{
    page_t *page = (page_t *)malloc(page_size);
    memset(page, 0, page_size);
    header_read();
    page->header.isLeaf = -1;
    page->header.parent_page_number = header_page->free_page_number;
    header_page->free_page_number = pagenum;

    file_write_page(pagenum, page);
    header_write();
    free(page);
}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(pagenum_t pagenum, page_t *dest)
{
    int ret;
    ret = pread(fd, dest, page_size, pagenum * page_size);
    if (ret == error)
    {
        perror("pread");
    }
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(pagenum_t pagenum, const page_t *src)
{
    int ret;
    ret = pwrite(fd, src, page_size, pagenum * page_size);
    if (ret == error)
    {
        perror("pwrite");
    }
}

void header_read()
{
    int ret;
    ret = pread(fd, header_page, page_size, 0);
    if (ret == error)
    {
        perror("pread");
    }
}

void header_write()
{
    int ret;
    ret = pwrite(fd, header_page, page_size, 0);
    if (ret == error)
    {
        perror("pwrite");
    }
}