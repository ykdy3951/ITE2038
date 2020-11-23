#include "lock_mgr.h"
#include "trx.h"
#include <stdlib.h>
#include <iostream>

unordered_map<pair<int, int64_t>, table_entry_t, HashFunction> lock_table;
pthread_mutex_t lock_table_latch;

int init_lock_table()
{
	/* DO IMPLEMENT YOUR ART !!!!! */
	lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
	return 0;
}

// trx_table에 lock들을 추가하는 코드를 작성해야함
lock_t *lock_acquire(int table_id, int64_t key, int trx_id, int lock_mode)
{
	// cout << "ACQUIRE\n";
	/* ENJOY CODING !!!! */
	pthread_mutex_lock(&lock_table_latch);

	if (trx_table.find(trx_id) == trx_table.end())
	{
		pthread_mutex_unlock(&lock_table_latch);
		return nullptr;
	}

	auto location = lock_table.find({table_id, key});

	if (location == lock_table.end())
	{
		table_entry_t new_entry(table_id, key);
		lock_t *new_lock = new lock_t();
		new_lock->state = ACQUIRED;
		/////////
		new_lock->lock_mode = lock_mode;
		new_lock->owner_trx_id = trx_id;
		pthread_mutex_lock(&trx_mgr_latch);
		if (trx_table[table_id].trx_head == NULL)
		{
			trx_table[trx_id].trx_head = trx_table[trx_id].trx_tail = new_lock;
		}
		else
		{
			lock_t *tail = trx_table[trx_id].trx_tail;
			tail->trx_next_lock = new_lock;
			trx_table[trx_id].trx_tail = new_lock;
		}
		pthread_mutex_unlock(&trx_mgr_latch);
		/////////
		new_entry.head = new_entry.tail = new_lock;
		lock_table.insert({{table_id, key}, new_entry});
		new_lock->sentinel = &(*lock_table.find({table_id, key})).second;
		pthread_mutex_unlock(&lock_table_latch);
		return new_lock;
	}
	else
	{
		if ((*location).second.head == NULL)
		{
			lock_t *new_lock = new lock_t();
			new_lock->state = ACQUIRED;
			new_lock->sentinel = &(*location).second;
			(*location).second.head = (*location).second.tail = new_lock;

			/////////
			new_lock->lock_mode = lock_mode;
			new_lock->owner_trx_id = trx_id;
			pthread_mutex_lock(&trx_mgr_latch);
			if (trx_table[table_id].trx_head == NULL)
			{
				trx_table[trx_id].trx_head = trx_table[trx_id].trx_tail = new_lock;
			}
			else
			{
				lock_t *tail = trx_table[trx_id].trx_tail;
				tail->trx_next_lock = new_lock;
				trx_table[trx_id].trx_tail = new_lock;
			}
			pthread_mutex_unlock(&trx_mgr_latch);
			/////////

			pthread_mutex_unlock(&lock_table_latch);
			return new_lock;
		}
		// deadlock 탐지 하는 code 작성해야함
		else
		{
			table_entry_t *temp = &(*location).second;
			lock_t *new_lock = new lock_t();
			new_lock->state = WAITING;
			new_lock->sentinel = temp;

			lock_t *tmp_tail = temp->tail;
			tmp_tail->next = new_lock;
			new_lock->prev = temp->tail;

			temp->tail = new_lock;

			/////////
			new_lock->lock_mode = lock_mode;
			new_lock->owner_trx_id = trx_id;

			pthread_mutex_lock(&trx_mgr_latch);
			if (trx_table[table_id].trx_head == NULL)
			{
				trx_table[trx_id].trx_head = trx_table[trx_id].trx_tail = new_lock;
			}
			else
			{
				lock_t *tail = trx_table[trx_id].trx_tail;
				tail->trx_next_lock = new_lock;
				trx_table[trx_id].trx_tail = new_lock;
			}
			pthread_mutex_unlock(&trx_mgr_latch);
			/////////

			if (deadlock_detect(trx_id))
			{
				// abort function 구현 및 넣기
				pthread_mutex_lock(&trx_mgr_latch);
				trx_table[trx_id].is_aborted = true;
				pthread_mutex_unlock(&trx_mgr_latch);
				pthread_mutex_unlock(&lock_table_latch);
				return nullptr;
			}
			// if prev lock pointer's mode is SHARED and its state is acquired state, new_lock's state makes acquire.
			if (new_lock->prev->lock_mode == SHARED && new_lock->prev->state == ACQUIRED)
			{
				new_lock->state = ACQUIRED;
				pthread_mutex_unlock(&lock_table_latch);
				return new_lock;
			}

			while (temp->head != new_lock)
			{
				pthread_cond_wait(&new_lock->cond, &lock_table_latch);
			}
			// new_lock->state = ACQUIRED;
			pthread_mutex_unlock(&lock_table_latch);
			return new_lock;
		}
	}
	pthread_mutex_unlock(&lock_table_latch);
	return nullptr;
}

// 중간 것을 해제할 수 있음
int lock_release(lock_t *lock_obj)
{
	/* GOOD LUCK !!! */
	pthread_mutex_lock(&lock_table_latch);
	if (lock_obj->prev != NULL)
	{
		while (lock_obj->state != ACQUIRED)
		{
			pthread_cond_wait(&lock_obj->cond, &lock_table_latch);
		}

		// prev lock of lock object exists and is also ACQUIRED
		if (lock_obj->prev != NULL)
		{
			table_entry_t *temp = lock_obj->sentinel;
			lock_t *prev = lock_obj->prev;
			prev->next = lock_obj->next;

			delete lock_obj;
		}

		// lock object is head
		else
		{
			table_entry_t *temp = lock_obj->sentinel;
			temp->head = lock_obj->next;

			delete lock_obj;
			if (temp->head != NULL)
			{
				temp->head->prev = NULL;
				if (temp->head->state != ACQUIRED)
				{
					// If lock mode of Head is EXCLUSIVE, then unlocking this lock
					if (temp->head->lock_mode == EXCLUSIVE)
					{
						temp->head->state = ACQUIRED;
						pthread_cond_signal(&temp->head->cond);
					}
					// If lock mode of head is shared, then unlock sequential shared lock
					else
					{
						lock_t *lock = temp->head;
						while (lock != NULL && lock->lock_mode == SHARED)
						{
							lock->state = ACQUIRED;
							pthread_cond_signal(&lock->cond);
						}
					}
				}
			}
			// lock list is empty
			else
			{
				temp->tail = NULL;
				//lock_table.erase({temp->table_id, temp->key});
			}
		}
	}
	else
	{
		table_entry_t *temp = lock_obj->sentinel;
		temp->head = lock_obj->next;

		delete lock_obj;

		if (temp->head != NULL)
		{
			temp->head->prev = NULL;
			if (temp->head->state != ACQUIRED)
			{
				// If lock mode of Head is EXCLUSIVE, then unlocking this lock
				if (temp->head->lock_mode == EXCLUSIVE)
				{
					temp->head->state = ACQUIRED;
					pthread_cond_signal(&temp->head->cond);
				}
				// If lock mode of head is shared, then unlock sequential shared lock
				else
				{
					lock_t *lock = temp->head;
					while (lock != NULL && lock->lock_mode == SHARED)
					{
						lock->state = ACQUIRED;
						pthread_cond_signal(&lock->cond);
					}
				}
			}
		}
		else
		{
			temp->tail = NULL;
			//lock_table.erase({temp->table_id, temp->key});
		}
	}
	pthread_mutex_unlock(&lock_table_latch);
	return 0;
}