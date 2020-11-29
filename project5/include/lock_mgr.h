#ifndef __LOCK_MGR_H__
#define __LOCK_MGR_H__

#include <stdint.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unordered_map>

#define SHARED 0
#define EXCLUSIVE 1

using namespace std;

enum lock_state
{
    WAITING,
    ACQUIRED,
    ABORT
};

class table_entry_t;
class lock_t;
class undo_log_t;

class undo_log_t
{
public:
    int64_t key;
    char *prev;
    undo_log_t(int64_t key)
    {
        this->key = key;
        prev = NULL;
    }
    undo_log_t(int64_t key, char *value)
    {
        this->key = key;
        prev = (char *)malloc(120);
        strcpy(prev, value);
    }
};

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
        this->table_id = table_id;
        this->key = key;
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
    int owner_trx_id;
    undo_log_t *log;
    lock_t()
    {
        prev = NULL;
        next = NULL;
        sentinel = NULL;
        cond = PTHREAD_COND_INITIALIZER;
        trx_next_lock = NULL;
        log = NULL;
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

extern unordered_map<pair<int, int64_t>, table_entry_t *, HashFunction> lock_table;
extern pthread_mutex_t lock_table_latch;

#endif /* __LOCK_MGR_H__ */
