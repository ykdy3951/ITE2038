#ifndef __TRX_H__
#define __TRX_H__

#include "lock_mgr.h"
#include <unordered_map> // hash map
#include <pthread.h>

using namespace std;

class trx_t;

class trx_t
{
public:
    int trx_id;
    lock_t *trx_head;
    lock_t *trx_tail;
    trx_t(int trx_id)
    {
        trx_id;
        trx_head = NULL;
        trx_tail = NULL;
    }
};

int trx_begin(void);
int trx_commit(int trx_id);
bool deadlock_detect(int trx_id);

// global variables for transaction
int global_trx_id = 0;
pthread_mutex_t trx_mgr_latch = PTHREAD_MUTEX_INITIALIZER;
unordered_map<int, trx_t> trx_table;

#endif