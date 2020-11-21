#include "trx.h"
#include <vector>
#include <algorithm>

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
        graph[trx_id].push_back(prev->trx_ptr->trx_id);
        temp = prev;
    }
    idx.push_back(trx_id);

    temp = temp_entry.trx_head;
    while (temp != temp_entry.trx_tail)
    {
        lock_t *loop = temp->sentinel->tail;
        while (loop->trx_ptr->trx_id != trx_id)
        {
            lock_t *prev = temp->prev;
            idx.push_back(loop->trx_ptr->trx_id);
            graph[loop->trx_ptr->trx_id].push_back(prev->trx_ptr->trx_id);
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