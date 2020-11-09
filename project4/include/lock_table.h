#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#include <stdint.h>
#include <pthread.h>
#include <stdio.h>
#include <unordered_map>

using namespace std;

enum lock_state
{
    WAITING,
    ACQUIRED
};

typedef struct table_entry_t table_entry_t;
typedef struct lock_t lock_t;

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
