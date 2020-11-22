#include "trx.h"
#include "buf.h"
#include <vector>
#include <algorithm>
#include <string.h>

#define fail 1
#define success 0

int trx_begin(void)
{
    pthread_mutex_lock(&trx_mgr_latch);

    global_trx_id++;
    trx_t entry(global_trx_id);
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
    if ((*trx_entry).second.trx_head == NULL)
    {
        pthread_mutex_unlock(&trx_mgr_latch);
        return 0;
    }

    lock_t *trx_lock = (*trx_entry).second.trx_head;

    while (trx_lock != NULL)
    {
        lock_t *temp = trx_lock->trx_next_lock;
        lock_release(trx_lock);
        trx_lock = temp;
    }

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
}

bool deadlock_detect(int trx_id)
{
    pthread_mutex_lock(&trx_mgr_latch);

    auto temp_entry = trx_table[trx_id];
    lock_t *temp = temp_entry.trx_tail;
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

    temp = temp_entry.trx_head;
    while (temp != temp_entry.trx_tail)
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

    pagenum_t pagenum = find_leaf(table_id, key);
    int buffer_idx = buf_read_page(table_id, pagenum);

    lock_t *lock = lock_acquire(table_id, key, trx_id, SHARED);
    if (lock == nullptr)
    {
        return ABORTED;
    }

    pthread_mutex_unlock(&trx_mgr_latch);
    return 0;
}

int db_update(int table_id, int64_t key, char *values, int trx_id)
{
    pthread_mutex_lock(&trx_mgr_latch);

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

    char *rollback = (char *)malloc(val_size);
    int ret = db_find(table_id, key, rollback);
    pagenum_t pagenum = find_leaf(table_id, key);

    //

    if (ret || !pagenum)
    {
        free(rollback);
        pthread_mutex_unlock(&trx_mgr_latch);
        return fail;
    }

    int buffer_idx = buf_read_page(table_id, pagenum);
    page_t *page = buf[buffer_idx].page;
    buf_write_page(buffer_idx);

    lock_t *lock = lock_acquire(table_id, key, trx_id, EXCLUSIVE);
    if (lock == nullptr)
    {
        pthread_mutex_unlock(&trx_mgr_latch);
        return ABORTED;
    }

    int i;
    for (i = 0; i < page->header.number_of_keys; i++)
    {
        if (page->records[i].key == key)
        {
            break;
        }
    }
    // for rollback
    strcpy(rollback, page->records[i].value);

    // overwrite
    strcpy(page->records[i].value, values);

    // commit 될 때까지 기다림
    free(rollback);
    pthread_mutex_unlock(&trx_mgr_latch);
    return 0;
}