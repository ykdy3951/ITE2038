#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "file.h"

#ifdef WINDOWS
#endif

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */
typedef struct data
{
    pagenum_t pagenum;
    int depth;
} data;

typedef struct Node
{
    data d;
    struct Node *next;
} Node;

typedef struct Queue
{
    Node *front;
    Node *rear;
    int count;
} Queue;

typedef struct Direction_data
{
    int data;
    int direction;
} Direction_data;

/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */
void usage(void);
// FUNCTION PROTOTYPES.
void InitQueue(Queue *q);
data dequeue(Queue *q);
void enqueue(Queue *q, pagenum_t p, int depth);
int IsEmpty(Queue *q);
void printAll(int table_id);
void print_leaf(int table_id);
void free_print(int table_id);

int db_insert(int table_id, int64_t key, char *value);
int db_find(int table_id, int64_t key, char *ret_val);
int db_delete(int table_id, int64_t key);

pagenum_t find_leaf(int table_id, int64_t key);
int cut(int length);
// Insertion.

record_t *make_record(int64_t key, char *value);
page_t *make_node(void);
page_t *make_leaf(void);
int get_left_index(page_t *parent, pagenum_t left_pagenum);
int insert_into_leaf(int table_id, int leaf_idx, record_t *new_record);
int insert_into_leaf_after_splitting(int table_id, int leaf_idx,
                                     record_t *new_record);
int insert_into_node(int table_id, int parent_idx,
                     int left_index, int64_t key, pagenum_t right_pagenum);
int insert_into_node_after_splitting(int table_id, int old_idx, int left_index,
                                     int64_t key, pagenum_t right_pagenum);
int insert_into_parent(int table_id, int leaf_idx, int64_t key, int right_idx);
int insert_into_new_root(int table_id, int left_idx, int64_t key, int right_idx);
int start_new_tree(int table_id, record_t *record);

// Deletion.

Direction_data get_neighbor_index(int table_id, page_t *page, pagenum_t pagenum);
pagenum_t get_neighbor_pagenum(int table_id, page_t *page, int neighbor_index);
int64_t get_parent_key(int table_id, page_t *child, int key_index);
int adjust_root(int table_id, page_t *root, pagenum_t pagenum, int page_idx);
page_t *remove_entry_from_node(int table_id, page_t *page, pagenum_t pagenum, int64_t key);
int coalesce_nodes(int table_id, int page_idx, int neighbor_idx, int neighbor_index, int direction, int64_t k_prime);
int redistribute_nodes(int table_id, int page_idx, int neighbor_idx, int neighbor_index, int direction,
                       int k_prime_index, int64_t k_prime);
int delete_entry(int table_id, pagenum_t pagenum, int64_t key);

#endif /* __BPT_H__*/
