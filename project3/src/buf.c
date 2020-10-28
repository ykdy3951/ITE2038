#include "buf.h"
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
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
    buf_header->free = NULL;

    for (int i = 0; i < num_buf; i++)
    {
        buf[i].is_dirty = 0;
        buf[i].is_pinned = 0;
        buf[i].prev = -1;
        buf[i].next = -1;
        buf[i].table_id = 0;
        buf[i].header_page = NULL;
        buf[i].page = NULL;
    }

    for (int i = num_buf - 1; i >= 0; i--)
    {
        free_buf_t *temp = (free_buf_t *)malloc(sizeof(free_buf_t));
        temp->buf_index = i;
        temp->next = buf_header->free;
        buf_header->free = temp;
    }

    return 0;
}

int close_table(int table_id)
{
    // 닫혀있는 table을 또 닫을 경우
    if (table->fd_table[table_id] == -1)
    {
        return 1;
    }

    int buffer_idx = buf_header->tail;
    while (buffer_idx != -1)
    {
        if (buf[buffer_idx].table_id == table_id)
        {
            if (buf[buffer_idx].is_pinned)
            {
                printf("find pinned buffer!\n");
                return 1;
            }
            if (buf[buffer_idx].is_dirty)
            {
                buf_put_page(buffer_idx);
            }
            else
            {
                if (buf[buffer_idx].page_num)
                {
                    free(buf[buffer_idx].page);
                }
                else
                {
                    free(buf[buffer_idx].header_page);
                }
            }

            // tail일경우
            if (buffer_idx == buf_header->tail)
            {
                buf_header->tail = buf[buffer_idx].prev;
                buf[buf[buffer_idx].prev].next = -1;
            }
            else
            {
                int prev = buf[buffer_idx].prev;
                int next = buf[buffer_idx].next;

                // prev is head
                if (prev == -1)
                {
                    buf[next].prev = prev;
                    buf_header->head = next;
                }
                else
                {
                    buf[prev].next = next;
                    buf[next].prev = prev;
                }
            }

            // free buffer index를 추가 시킨다.
            free_buf_t *temp = (free_buf_t *)malloc(sizeof(free_buf_t));
            temp->next = buf_header->free;
            temp->buf_index = buffer_idx;
            buf_header->free = temp;
            buf_header->used_size--;
        }
        buffer_idx = buf[buffer_idx].prev;
    }

    close(table->fd_table[table_id]);
    table->fd_table[table_id] = -1;

    return 0;
}

int shutdown_db(void)
{
    // buffer 할당 해제
    int buffer_idx = buf_header->tail;
    while (buffer_idx != -1)
    {
        if (buf[buffer_idx].is_dirty)
        {
            buf_put_page(buffer_idx);
        }
        else
        {
            if (buf[buffer_idx].page_num)
            {
                free(buf[buffer_idx].page);
            }
            else
            {
                free(buf[buffer_idx].header_page);
            }
        }
        buffer_idx = buf[buffer_idx].prev;
    }
    free(buf);

    // buffer free index 관리하는 linked list 할당 해제
    while (buf_header->free != NULL)
    {
        free_buf_t *temp;
        temp = buf_header->free;
        buf_header->free = temp->next;
        free(temp);
    }
    // buffer header 할당 해제
    free(buf_header);

    // 모든 table close

    for (int i = 1; i <= table->num_of_open; i++)
    {
        free(table->table_path[i]);
        // 닫혀있는 table이 아닐 경우
        if (table->fd_table[i] != -1)
        {
            close(table->fd_table[i]);
        }
    }
    // table들을 담는 전역 변수 할당 해제
    free(table);

    return 0;
}

int LRU_func(int buffer_index)
{
    // LRU 처리
    // 가장 최근에 사용 했던 buffer일 경우

    if (buf_header->head == buffer_index)
    {
        return 0;
    }

    int recent = buf_header->head;
    int next = buf[buffer_index].next;
    int prev = buf[buffer_index].prev;

    if (buf_header->tail != buffer_index)
    {
        buf[next].prev = prev;
        buf[prev].next = next;
        buf[buffer_index].next = recent;
        buf[buffer_index].prev = -1;
        buf_header->head = buffer_index;
    }

    else
    {
        buf[prev].next = next;
        buf[buffer_index].next = recent;
        buf[buffer_index].prev = -1;
        buf_header->head = buffer_index;
        buf_header->tail = prev;
    }

    return 0;
}

int buf_init(int buffer_index)
{
    // buf[buffer_index].prev = -1;
    // buf[buffer_index].next = -1;
    buf[buffer_index].table_id = 0;
    buf[buffer_index].page_num = 0;
    buf[buffer_index].is_dirty = 0;
    buf[buffer_index].is_pinned = 0;

    return 0;
}

// file에서 buffer로 가져오는 함수
int buf_get_page(int buffer_index, int table_id, pagenum_t pagenum)
{
    if (pagenum == 0)
    {
        buf[buffer_index].header_page = (header_page_t *)malloc(page_size);
        buf_init(buffer_index);
        file_read_page(table_id, pagenum, (page_t *)buf[buffer_index].header_page);
        buf[buffer_index].table_id = table_id;
        buf[buffer_index].page_num = pagenum;
    }
    else
    {
        buf[buffer_index].page = (page_t *)malloc(page_size);
        buf_init(buffer_index);
        file_read_page(table_id, pagenum, (page_t *)buf[buffer_index].page);
        buf[buffer_index].table_id = table_id;
        buf[buffer_index].page_num = pagenum;
    }

    return 0;
}

//buf에서 file에 쓰는 함수
int buf_put_page(int index)
{
    if (buf[index].page_num == 0)
    {
        file_write_page(buf[index].table_id, buf[index].page_num, (page_t *)&buf[index].header_page);
        free(buf[index].header_page);
        buf_init(index);
        buf[index].header_page = NULL;
    }
    else
    {
        file_write_page(buf[index].table_id, buf[index].page_num, &buf[index].page);
        free(buf[index].header_page);
        buf_init(index);
        buf[index].page = NULL;
    }

    return 0;
}

// buffer 사용이 끝났음을 알리는 함수 pin을 뽑고 LRU 처리도 하는 함수
int buf_write_page(int buffer_index)
{
    buf[buffer_index].is_pinned--;
    LRU_func(buffer_index);

    return 0;
}

// pagenum, table_id에 해당하는 page를 읽어들이라고 요청해주는 함수.
int buf_read_page(int table_id, pagenum_t pagenum)
{
    int buffer_index = buf_header->head;

    // buffer에 찾는 page가 존재하는 경우
    while (buffer_index != -1)
    {
        if (buf[buffer_index].table_id == table_id && buf[buffer_index].page_num == pagenum)
        {
            LRU_func(buffer_index);
            buf[buffer_index].is_pinned++;
            return buffer_index;
        }
        buffer_index = buf[buffer_index].next;
    }

    // 없는 경우 buffer를 file에서 불러온다.
    // buffer is full
    if (buf_header->used_size == buf_header->buf_size)
    {
        buffer_index = buf_header->tail;
        while (true)
        {
            // 모든 buffer에 pin이 있을 경우 종료한다.
            if (buffer_index == -1)
            {
                exit(EXIT_FAILURE);
            }
            // 사용 중인 경우 prev로 이동한다.
            if (buf[buffer_index].is_pinned)
            {
                buffer_index = buf[buffer_index].prev;
            }
            // 찾은 경우 반복을 종료한다.
            else
            {
                break;
            }
        }
        // 위치의 page가 dirty page 일 경우 page를 file에 write한다.
        if (buf[buffer_index].is_dirty)
        {
            buf_put_page(buffer_index);
        }
        buf_get_page(buffer_index, table_id, pagenum);
        LRU_func(buffer_index);
    }

    // buffer에 공간이 남아있을 경우
    // free_idx linked list에서 맨 앞을 빼온다.
    else
    {
        free_buf_t *temp = buf_header->free;
        buffer_index = temp->buf_index;
        buf_header->free = temp->next;
        free(temp);

        buf_get_page(buffer_index, table_id, pagenum);

        buf[buffer_index].prev = -1;
        buf[buffer_index].next = -1;

        // 처음 buffer에 쓰는 경우는 head와 tail을 현재 buffer index로 만들어준다.
        if (buf_header->used_size == 0)
        {
            buf_header->head = buffer_index;
            buf_header->tail = buffer_index;
        }
        // 처음은 아닌 경우, head만 바꿔주고 doubly linked list 형태로 맞게 수정한다.
        else
        {
            buf[buf_header->head].prev = buffer_index;
            buf[buffer_index].next = buf_header->head;
            buf_header->head = buffer_index;
        }

        // 사용중인 버퍼의 크기를 늘린다.
        buf_header->used_size++;
    }
    buf[buffer_index].is_pinned++;
    return buffer_index;
}

// free page로 만드는 함수이다.
void buf_free_page(int table_id, pagenum_t pagenum)
{
    // table id에 해당하는 header와 target page가 있는 buffer index를 가져오고 pin을 활성화 한다.
    int header_idx = buf_read_page(table_id, 0);
    int target_idx = buf_read_page(table_id, pagenum);

    memset(buf[target_idx].page, 0, page_size);

    buf[target_idx].page->header.parent_page_number = buf[header_idx].header_page->free_page_number;
    buf[target_idx].is_dirty = 1;

    buf[header_idx].header_page->free_page_number = pagenum;
    buf[header_idx].is_dirty = 1;

    // 함수가 끝나기 전에 pin을 뽑는다.
    buf_write_page(header_idx);
    buf_write_page(target_idx);
}

// 새 pagenum를 가져오는 함수이다.
int buf_alloc_page(int table_id)
{
    int header_idx = buf_read_page(table_id, 0);
    int free_idx;
    pagenum_t free_num = buf[header_idx].header_page->free_page_number;

    // free page가 있을 경우
    if (free_num)
    {
        free_idx = buf_read_page(table_id, free_num);
        buf[header_idx].header_page->free_page_number = buf[free_idx].page->header.parent_page_number;

        buf[free_idx].table_id = table_id;
        buf[free_idx].page_num = free_num;
    }

    // 없는 경우
    else
    {
        free_num = buf[header_idx].header_page->number_of_pages;
        buf[header_idx].header_page->number_of_pages++;

        if (buf_header->used_size == buf_header->buf_size)
        {
            free_idx = buf_header->tail;
            while (true)
            {
                // 모든 buffer에 pin이 있을 경우 종료한다.
                if (free_idx == -1)
                {
                    exit(EXIT_FAILURE);
                }
                // 사용 중인 경우 prev로 이동한다.
                if (buf[free_idx].is_pinned)
                {
                    free_idx = buf[free_idx].prev;
                }
                // 찾은 경우 반복을 종료한다.
                else
                {
                    break;
                }
            }
            // 위치의 page가 dirty page 일 경우 page를 file에 write한다.
            if (buf[free_idx].is_dirty)
            {
                buf_put_page(free_idx);
            }

            buf_init(free_idx);
            buf[free_idx].page_num = free_num;
            buf[free_idx].table_id = table_id;
            buf[free_idx].page = (page_t *)malloc(page_size);
            memcpy(buf[free_idx].page, 0, page_size);
            // buf_get_page(free_idx, table_id, pagenum);
            LRU_func(free_idx);
        }

        // buffer에 공간이 남아있을 경우
        // free_idx linked list에서 맨 앞을 빼온다.
        else
        {
            free_buf_t *temp = buf_header->free;
            free_idx = temp->buf_index;
            buf_header->free = temp->next;
            free(temp);

            buf_init(free_idx);
            buf[free_idx].page_num = free_num;
            buf[free_idx].table_id = table_id;
            buf[free_idx].page = (page_t *)malloc(page_size);
            memcpy(buf[free_idx].page, 0, page_size);
            // buf_get_page(free_idx, table_id, pagenum);

            buf[free_idx].prev = -1;
            buf[free_idx].next = -1;

            // 처음 buffer에 쓰는 경우는 head와 tail을 현재 buffer index로 만들어준다.
            if (buf_header->used_size == 0)
            {
                buf_header->head = free_idx;
                buf_header->tail = free_idx;
            }
            // 처음은 아닌 경우, head만 바꿔주고 doubly linked list 형태로 맞게 수정한다.
            else
            {
                buf[buf_header->head].prev = free_idx;
                buf[free_idx].next = buf_header->head;
                buf_header->head = free_idx;
            }

            // 사용중인 버퍼의 크기를 늘린다.
            buf_header->used_size++;
        }
        buf[free_idx].is_pinned++;
    }

    buf[header_idx].is_dirty = 1;
    buf[free_idx].is_dirty = 1;
    buf_write_page(header_idx);
    buf_write_page(free_idx);

    return free_idx;
}