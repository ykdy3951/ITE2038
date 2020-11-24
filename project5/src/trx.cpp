#include "trx.h"
#include "buf.h"
#include <vector>
#include <algorithm>
#include <string.h>

#define fail 1
#define success 0

bool trx_init_flag;
int global_trx_id;
pthread_mutex_t trx_mgr_latch;
unordered_map<int, trx_t *> trx_table;

int trx_begin(void)
{
    if (!trx_init_flag)
    {
        trx_mgr_latch = PTHREAD_MUTEX_INITIALIZER;
        trx_init_flag = true;
    }

    pthread_mutex_lock(&trx_mgr_latch);

    global_trx_id++;
    trx_t *entry = new trx_t(global_trx_id);
    trx_table[global_trx_id] = entry;

    pthread_mutex_unlock(&trx_mgr_latch);
    return global_trx_id;
}

int trx_commit(int trx_id)
{
    pthread_mutex_lock(&trx_mgr_latch);

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
        if (trx_lock->lock_mode == EXCLUSIVE)
        {
            free(trx_lock->log->prev);
        }
        delete trx_lock->log;
        lock_release(trx_lock);
        trx_lock = temp;
    }

    delete trx_table[trx_id];
    trx_table.erase(trx_id);
    pthread_mutex_unlock(&trx_mgr_latch);

    return trx_id;
}

bool dfs(int start, vector<int> *graph, bool *vst)
{
    vst[start] = true;
    for (auto i : graph[start])
    {
        if (!vst[i] && dfs(i, graph, vst))
        {
            return true;
        }
        else if (vst[i])
        {
            return true;
        }
        vst[start] = false;
        return false;
    }
    return false;
}

bool deadlock_detect(int trx_id)
{
    pthread_mutex_lock(&trx_mgr_latch);

    auto temp_entry = trx_table[trx_id];
    lock_t *temp = temp_entry->trx_tail;
    // unordered_map<int, vector<int>> graph;
    vector<int> *graph = new vector<int>[global_trx_id + 1];
    vector<int> idx;

    bool *chk = new bool[global_trx_id + 1];
    bool result = false;

    while (temp)
    {
        lock_t *prev = temp->prev;
        graph[trx_id].push_back(prev->owner_trx_id);
        temp = prev;
    }
    idx.push_back(trx_id);

    temp = temp_entry->trx_head;
    while (temp != temp_entry->trx_tail)
    {
        lock_t *loop = temp->sentinel->tail;
        while (loop->owner_trx_id != trx_id)
        {
            lock_t *prev = temp->prev;
            idx.push_back(loop->owner_trx_id);
            graph[loop->owner_trx_id].push_back(prev->owner_trx_id);
            loop = prev;
        }
        temp = temp->trx_next_lock;
    }

    // erase duplicate
    sort(idx.begin(), idx.end());
    idx.erase(unique(idx.begin(), idx.end()), idx.end());

    for (int i : idx)
    {
        result = dfs(i, graph, chk);
        if (result)
        {
            break;
        }
    }
    delete chk;
    delete graph;
    pthread_mutex_unlock(&trx_mgr_latch);
    return result;
}

int db_find(int table_id, int64_t key, char *ret_val, int trx_id)
{
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
        trx_abort(trx_id);
        return ABORTED;
    }

    pthread_mutex_lock(&buf[buffer_idx].page_latch);
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
    pthread_mutex_unlock(&buf[buffer_idx].page_latch);
    return fail;
}

int db_update(int table_id, int64_t key, char *values, int trx_id)
{
    pthread_mutex_lock(&trx_mgr_latch);
    if (trx_table.find(trx_id) == trx_table.end())
    {
        pthread_mutex_unlock(&trx_mgr_latch);
        return ABORT;
    }
    pthread_mutex_unlock(&trx_mgr_latch);

    // int ret = db_find(table_id, key, rollback);
    pagenum_t pagenum = find_leaf(table_id, key);

    undo_log_t *log = NULL;
    //

    if (!pagenum)
    {
        pthread_mutex_unlock(&trx_mgr_latch);
        return fail;
    }

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
    lock->log = log;

    if (lock->state == ABORT)
    {
        trx_abort(trx_id);
        return ABORTED;
    }

    buffer_idx = buf_read_page(table_id, pagenum);

    page = buf[buffer_idx].page;

    for (i = 0; i < page->header.number_of_keys; i++)
    {
        if (page->records[i].key == key)
        {
            break;
        }
    }
    strcpy(page->records[i].value, values);
    buf_write_page(buffer_idx);

    return success;
}

void trx_abort(int trx_id)
{
    pthread_mutex_lock(&trx_mgr_latch);
    pthread_mutex_lock(&lock_table_latch);
    lock_t *lock = trx_table[trx_id]->trx_head;
    while (lock != NULL)
    {
        lock_t *temp = lock->trx_next_lock;

        if (lock->lock_mode == SHARED)
        {
            lock->log->prev = NULL;
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

            prev->next = next;
            if (next != NULL)
            {
                next->prev = prev;
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