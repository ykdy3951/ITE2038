#ifndef __LOCK_MGR_H__
#define __LOCK_MGR_H__

#include <stdint.h>
#include <pthread.h>
#include <stdio.h>
#include <unordered_map>
#include "trx.h"

#define SHARED 0
#define EXCLUSIVE 1

using namespace std;

enum lock_state
{
    WAITING,
    ACQUIRED
};

class table_entry_t;
class lock_t;

class table_entry_t
{
    /* NO PAIN, NO GAIN. */
public:
    int table_id;
    int64_t key;
    lock_t *tail;
    lock_t *head;
    table_entry_t(int table_id, int64_t key)
    {
        table_id = table_id;
        key = key;
        head = NULL;
        tail = NULL;
    }
};

class lock_t
{
public:
    lock_t *prev;
    lock_t *next;
    table_entry_t *sentinel;
    lock_state state;
    pthread_cond_t cond;
    int lock_mode; // 0 is SHARED option, 1 is EXCLUSIVE option.
    lock_t *trx_next_lock;
    trx_t *trx_ptr;
    lock_t()
    {
        prev = NULL;
        next = NULL;
        sentinel = NULL;
        cond = PTHREAD_COND_INITIALIZER;
        trx_next_lock = NULL;
    }
};

class HashFunction
{
public:
    size_t operator()(const pair<int, int64_t> &p) const
    {
        int table_id = p.first;
        int64_t key = p.second;
        long long res = table_id + key * 100;
        return res;
    }
};

/* APIs for lock table */
int init_lock_table();
lock_t *lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode);
int lock_release(lock_t *lock_obj);

#endif /* __LOCK_MGR_H__ */
