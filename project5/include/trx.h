#ifndef __TRX_H__
#define __TRX_H__

#include "lock_mgr.h"
#include "bpt.h"
#include <unordered_map> // hash map
#include <pthread.h>
#include <vector>

#define ABORTED -1

using namespace std;

class trx_t;

class trx_t
{
public:
    int trx_id;
    // vector<lock_t *> trx_list;
    lock_t *trx_head;
    lock_t *trx_tail;
    bool is_aborted;
    trx_t()
    {
        trx_id = 0;
        trx_head = NULL;
        trx_tail = NULL;
        is_aborted = false;
    }
    trx_t(int trx_id)
    {
        trx_id = trx_id;
        trx_head = NULL;
        trx_tail = NULL;
        is_aborted = false;
    }
};

int trx_begin(void);
int trx_commit(int trx_id);
bool deadlock_detect(int trx_id);
void trx_abort(int trx_id);
int db_find(int table_id, int64_t key, char *ret_val, int trx_id = 0);
int db_update(int table_id, int64_t key, char *values, int trx_id);
// global variables for transaction
extern int global_trx_id;
extern pthread_mutex_t trx_mgr_latch;
extern unordered_map<int, trx_t *> trx_table;

#endif