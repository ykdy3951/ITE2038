#include "trx.h"
#include "buf.h"
#include <vector>
#include <algorithm>
#include <string.h>
#include <iostream>

#define fail 1
#define success 0

bool trx_init_flag;
int global_trx_id;
pthread_mutex_t trx_mgr_latch = PTHREAD_MUTEX_INITIALIZER;
unordered_map<int, trx_t *> trx_table;

// success log

int trx_begin(void)
{
    // if (!trx_init_flag)
    // {
    //     trx_mgr_latch = PTHREAD_MUTEX_INITIALIZER;
    //     trx_init_flag = true;
    // }

    pthread_mutex_lock(&trx_mgr_latch);

    global_trx_id++;
    trx_t *entry = new trx_t(global_trx_id);
    trx_table.insert({global_trx_id, entry});

    pthread_mutex_unlock(&trx_mgr_latch);
    return global_trx_id;
}

int trx_commit(int trx_id)
{
    pthread_mutex_lock(&trx_mgr_latch);

    // cout << "COMMIT" << endl;

    auto trx_entry = trx_table.find(trx_id);

    // if trx_table doesn't has trx_id, then return 0
    if (trx_entry == trx_table.end())
    {
        pthread_mutex_unlock(&trx_mgr_latch);
        return 0;
    }

    // trx_id에 해당하는 lock들이 존재하지 않은 경우 종료한다.
    // if ((*trx_entry).second->trx_head == NULL)
    // {
    //     pthread_mutex_unlock(&trx_mgr_latch);
    //     return 0;
    // }

    lock_t *trx_lock = (*trx_entry).second->trx_head;

    while (trx_lock != NULL)
    {
        lock_t *temp = trx_lock->trx_next_lock;
        lock_release(trx_lock);
        trx_lock = temp;
    }

    delete trx_table[trx_id];
    trx_table.erase(trx_id);
    pthread_mutex_unlock(&trx_mgr_latch);

    return trx_id;
}

bool dfs(int start, vector<int> *graph, int vst[])
{
    if (vst[start])
    {
        if (vst[start] == -1)
        {
            return true;
        }
        return false;
    }
    vst[start] = -1;
    for (int node : graph[start])
    {
        if (dfs(node, graph, vst))
        {
            return true;
        }
    }
    vst[start] = 1;
    return false;
}

bool deadlock_detect(int trx_id)
{
    pthread_mutex_lock(&trx_mgr_latch);

    if (trx_table.find(trx_id) == trx_table.end())
    {
        pthread_mutex_unlock(&trx_mgr_latch);
        return false;
    }

    auto temp_entry = trx_table[trx_id];
    lock_t *temp = temp_entry->trx_tail;
    // unordered_map<int, vector<int>> graph;
    vector<int> *graph = new vector<int>[global_trx_id + 1];

    int *vst = new int[global_trx_id + 1];
    bool result = false;

    // 앞에 자기자신이 있는 경우 예외 처리
    while (true)
    {
        lock_t *prev = temp->prev;
        if (prev == NULL)
            break;
        else
        {
            if (prev->owner_trx_id == temp->owner_trx_id)
            {
                temp = prev;
            }
            else
            {
                break;
            }
        }
    }
    temp = temp->prev;
    while (temp)
    {
        lock_t *prev = temp->prev;
        graph[trx_id].push_back(temp->owner_trx_id);
        temp = prev;
    }

    temp = temp_entry->trx_head;
    while (temp != temp_entry->trx_tail)
    {
        lock_t *loop = temp->sentinel->tail;
        while (loop->owner_trx_id != trx_id)
        {
            lock_t *prev = temp->prev;
            graph[loop->owner_trx_id].push_back(trx_id);
            loop = prev;
        }
        temp = temp->trx_next_lock;
    }

    result = dfs(trx_id, graph, vst);

    delete[] vst;
    delete[] graph;
    pthread_mutex_unlock(&trx_mgr_latch);
    return result;
}

int db_find(int table_id, int64_t key, char *ret_val, int trx_id)
{
    if (trx_id == 0)
    {
        // 닫혀있거나 열리지 않은 테이블이면 종료
        if (table->fd_table[table_id] == -1)
        {
            printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
            return fail;
        }

        // buffer가 없는 경우 종료
        if (buf == NULL)
        {
            printf("[ERROR] NO BUFFER.\n");
            return fail;
        }

        page_t *page;

        pagenum_t pagenum = find_leaf(table_id, key);
        if (pagenum == 0)
        {
            return fail;
        }

        int page_idx = buf_read_page(table_id, pagenum);
        page = buf[page_idx].page;
        int i;
        for (i = 0; i < page->header.number_of_keys; i++)
        {
            if (page->records[i].key == key)
            {
                strcpy(ret_val, page->records[i].value);
                buf_write_page(page_idx);
                return success;
            }
            else if (page->records[i].key > key)
            {
                break;
            }
        }
        buf_write_page(page_idx);
        return fail;
    }

    // 닫혀있거나 열리지 않은 테이블이면 종료
    if (table->fd_table[table_id] == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return fail;
    }

    // buffer가 없는 경우 종료
    if (buf == NULL)
    {
        printf("[ERROR] NO BUFFER.\n");
        return fail;
    }

    pthread_mutex_lock(&trx_mgr_latch);
    if (trx_table.find(trx_id) == trx_table.end())
    {
        pthread_mutex_unlock(&trx_mgr_latch);
        return ABORTED;
    }
    pthread_mutex_unlock(&trx_mgr_latch);

    pagenum_t pagenum = find_leaf(table_id, key);
    if (!pagenum)
    {
        return fail;
    }
    int buffer_idx = buf_read_page(table_id, pagenum);
    buf_write_page(buffer_idx);

    lock_t *lock = lock_acquire(table_id, key, trx_id, SHARED);
    undo_log_t *log = new undo_log_t(key);
    log->prev = ret_val;
    lock->log = log;

    if (lock == nullptr)
    {
        ret_val = NULL;
        delete log;
        return fail;
    }

    if (lock->state == ABORT)
    {
        // cout << "ABORT" << endl;
        trx_abort(trx_id);
        return ABORTED;
    }

    buffer_idx = buf_read_page(table_id, pagenum);
    page_t *page = buf[buffer_idx].page;
    for (int i = 0; i < page->header.number_of_keys; i++)
    {
        if (page->records[i].key == key)
        {
            strcpy(ret_val, page->records[i].value);
            pthread_mutex_unlock(&buf[buffer_idx].page_latch);
            return success;
        }
        else if (page->records[i].key > key)
        {
            break;
        }
    }
    buf_write_page(buffer_idx);
    return fail;
}

int db_update(int table_id, int64_t key, char *values, int trx_id)
{

    // 닫혀있거나 열리지 않은 테이블이면 종료
    if (table->fd_table[table_id] == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return fail;
    }

    // buffer가 없는 경우 종료
    if (buf == NULL)
    {
        printf("[ERROR] NO BUFFER.\n");
        return fail;
    }

    pthread_mutex_lock(&trx_mgr_latch);
    if (trx_table.find(trx_id) == trx_table.end())
    {
        pthread_mutex_unlock(&trx_mgr_latch);
        return ABORTED;
    }
    pthread_mutex_unlock(&trx_mgr_latch);

    // int ret = db_find(table_id, key, rollback);
    pagenum_t pagenum = find_leaf(table_id, key);
    undo_log_t *log = NULL;
    //

    if (!pagenum)
    {
        // pthread_mutex_unlock(&trx_mgr_latch);
        return fail;
    }
    // if (table_id == 0)
    // {
    //     printf("111111111111111111111111111\n");
    // }
    int buffer_idx = buf_read_page(table_id, pagenum);
    page_t *page = buf[buffer_idx].page;

    int i;
    for (i = 0; i < page->header.number_of_keys; i++)
    {
        if (page->records[i].key == key)
        {
            log = new undo_log_t(key, page->records[i].value);
            break;
        }
        else if (page->records[i].key > key)
        {
            buf_write_page(buffer_idx);
            return fail;
        }
    }
    buf_write_page(buffer_idx);

    lock_t *lock = lock_acquire(table_id, key, trx_id, EXCLUSIVE);

    if (lock == nullptr)
    {
        free(log->prev);
        delete log;
        return fail;
    }

    lock->log = log;

    if (lock->state == ABORT)
    {
        // cout << "ABORT" << endl;
        trx_abort(trx_id);
        return ABORTED;
    }
    // if (table_id == 0)
    // {
    //     printf("111111111111111111111111111\n");
    // }
    buffer_idx = buf_read_page(table_id, pagenum);
    // if (table_id == 0)
    // {
    //     printf("111111111111111111111111111\n");
    // }
    page = buf[buffer_idx].page;

    for (i = 0; i < page->header.number_of_keys; i++)
    {
        if (page->records[i].key == key)
        {
            break;
        }
    }
    strcpy(page->records[i].value, values);
    buf[buffer_idx].is_dirty = 1;
    buf_write_page(buffer_idx);

    return success;
}

void trx_abort(int trx_id)
{
    pthread_mutex_lock(&trx_mgr_latch);
    pthread_mutex_lock(&lock_table_latch);

    if (trx_table.find(trx_id) == trx_table.end())
    {
        pthread_mutex_unlock(&lock_table_latch);
        pthread_mutex_unlock(&trx_mgr_latch);
        return;
    }

    lock_t *lock = trx_table[trx_id]->trx_head;
    while (lock != NULL)
    {
        lock_t *temp = lock->trx_next_lock;

        if (lock->log != NULL)
        {
            if (lock->lock_mode == SHARED)
            {
                memset(lock->log->prev, 0, sizeof(char) * 120);
            }
            else
            {
                table_entry_t *entry = lock->sentinel;

                pagenum_t pagenum = find_leaf(entry->table_id, entry->key);
                int buff_idx = buf_read_page(entry->table_id, pagenum);
                page_t *page = buf[buff_idx].page;

                for (int i = 0; i < page->header.number_of_keys; i++)
                {
                    if (page->records[i].key == entry->key)
                    {
                        strcpy(page->records[i].value, lock->log->prev);
                        break;
                    }
                }
                buf_write_page(buff_idx);
            }
        }
        if (lock->state == ACQUIRED)
        {
            pthread_mutex_unlock(&lock_table_latch);
            lock_release(lock);
            pthread_mutex_lock(&lock_table_latch);
        }
        else
        {
            lock_t *prev = lock->prev;
            lock_t *next = lock->next;

            if (prev != NULL)
            {
                prev->next = next;
            }
            else
            {
                if (next != NULL)
                {
                    lock->sentinel->head = next;
                }
                else
                {
                    lock->sentinel->head = lock->sentinel->tail = NULL;
                }
            }
            if (next != NULL)
            {
                next->prev = prev;
            }
            else
            {
                lock->sentinel->tail = prev;
            }
            delete lock;
        }
        lock = temp;
    }
    pthread_mutex_unlock(&lock_table_latch);

    delete trx_table[trx_id];
    trx_table.erase(trx_id);

    pthread_mutex_unlock(&trx_mgr_latch);
}