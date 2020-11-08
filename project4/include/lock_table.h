#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>
#include <list>
#include <unordered_map>

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
    lock_t()
    {
        prev = NULL;
        next = NULL;
        sentinel = NULL;
        cond = PTHREAD_COND_INITIALIZER;
    }
};

class HashFunction
{
public:
    size_t operator()(const pair<int, int64_t> &p) const
    {
        int table_id = p.first;
        int64_t key = p.second % 1000000007;
        long long res = table_id + key * 10;
        res %= 1000000007;
        return res;
    }
};

/* APIs for lock table */
int init_lock_table();
lock_t *lock_acquire(int table_id, int64_t key);
int lock_release(lock_t *lock_obj);

#endif /* __LOCK_TABLE_H__ */
