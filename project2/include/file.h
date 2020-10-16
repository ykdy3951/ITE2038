#ifndef __FILE_H__
#define __FILE_H__
//file.h
#include <stdint.h>

#pragma pack(1)
#define false 0
#define true 1
#define error -1

#define val_size 120
#define page_size 4096

// #define LEAF_ORDER 5
// #define BRANCH_ORDER 5

#define LEAF_ORDER 32
#define BRANCH_ORDER 249

typedef uint64_t pagenum_t;

typedef struct record
{
    int64_t key;
    char value[120];
} record_t;

typedef struct headerPage
{
    pagenum_t free_page_number;
    pagenum_t root_page_number;
    uint64_t number_of_pages;
    char reserved[4072];
} header_page_t;

typedef struct pageHeader
{
    pagenum_t parent_page_number;
    int isLeaf;
    int number_of_keys;
    char reserved[104];
} page_header_t;

typedef struct branchFactor
{
    int64_t key;
    pagenum_t page_number;
} branch_factor_t;

typedef struct pageNotHeader
{
    page_header_t header;
    union
    {
        pagenum_t right_sibling_number;
        pagenum_t one_more_page_number;
    };
    union
    {
        record_t records[31];
        branch_factor_t entries[248];
    };
} page_t;

int fd;

header_page_t *header_page;

int open_table(char *pathname);

pagenum_t file_alloc_page();

void file_free_page(pagenum_t pagenum);

void file_read_page(pagenum_t pagenum, page_t *dest);

void file_write_page(pagenum_t pagenum, const page_t *src);

void header_read();

void header_write();

#endif