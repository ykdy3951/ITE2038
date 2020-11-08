#include <lock_table.h>
#include <iostream>

unordered_map<pair<int, int64_t>, table_entry_t, HashFunction> lock_table;
pthread_mutex_t lock_table_latch;
pthread_cond_t cond;

int init_lock_table()
{
	/* DO IMPLEMENT YOUR ART !!!!! */
	lock_table_latch = PTHREAD_MUTEX_INITIALIZER;
	cond = PTHREAD_COND_INITIALIZER;
	return 0;
}

lock_t *lock_acquire(int table_id, int64_t key)
{
	// cout << "ACQUIRE\n";
	/* ENJOY CODING !!!! */
	pthread_mutex_lock(&lock_table_latch);

	auto location = lock_table.find({table_id, key});

	if (location == lock_table.end())
	{
		table_entry_t new_entry(table_id, key);
		lock_t *new_lock = new lock_t();
		new_lock->state = ACQUIRED;
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
			pthread_mutex_unlock(&lock_table_latch);
			return new_lock;
		}
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
			while (temp->head != new_lock)
			{
				pthread_cond_wait(&cond, &lock_table_latch);
			}
			// new_lock->state = ACQUIRED;
			pthread_mutex_unlock(&lock_table_latch);
			return new_lock;
		}
	}
	pthread_mutex_unlock(&lock_table_latch);
	return nullptr;
}

int lock_release(lock_t *lock_obj)
{
	// cout << "RELEASE\n";
	/* GOOD LUCK !!! */
	pthread_mutex_lock(&lock_table_latch);
	if (lock_obj->prev != NULL)
	{
		while (lock_obj->state != ACQUIRED)
		{
			pthread_cond_wait(&cond, &lock_table_latch);
		}
		table_entry_t *temp = lock_obj->sentinel;
		temp->head = lock_obj->next;

		delete lock_obj;
		if (temp->head != NULL)
		{
			temp->head->state = ACQUIRED;
			temp->head->prev = NULL;
			pthread_cond_signal(&cond);
		}
		else
		{
			temp->tail = NULL;
			//lock_table.erase({temp->table_id, temp->key});
		}
	}
	else
	{
		table_entry_t *temp = lock_obj->sentinel;
		temp->head = lock_obj->next;

		delete lock_obj;
		if (temp->head != NULL)
		{
			temp->head->state = ACQUIRED;
			temp->head->prev = NULL;
			pthread_cond_signal(&cond);
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
