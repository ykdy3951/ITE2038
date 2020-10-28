/*
 *  bpt.c  
 */
#define Version "1.14"

#include <string.h>
#include <stdlib.h>
#include "bpt.h"
#include "file.h"
// GLOBALS.
#define success 0
#define fail 1
/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
int leaf_order = LEAF_ORDER;
int branch_order = BRANCH_ORDER;
/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
void InitQueue(Queue *q)
{
    q->front = NULL;
    q->rear = NULL;
    q->count = 0;
}

data dequeue(Queue *q)
{
    Node *node = q->front;
    data d = node->d;
    q->front = node->next;
    q->count--;
    free(node);
    return d;
}

void enqueue(Queue *q, pagenum_t p, int depth)
{
    Node *new_node = (Node *)malloc(sizeof(Node));
    new_node->next = NULL;
    new_node->d.depth = depth;
    new_node->d.pagenum = p;
    if (IsEmpty(q))
    {
        q->front = q->rear = new_node;
    }
    else
    {
        q->rear->next = new_node;
        q->rear = new_node;
    }
    q->count++;
}

int IsEmpty(Queue *q)
{
    return q->count == 0;
}

void printAll()
{
    header_read();
    // no page
    if (header_page->root_page_number == 0)
    {
        printf("no page\n");
        return;
    }
    Queue q;
    InitQueue(&q);
    page_t *page = (page_t *)malloc(page_size);
    enqueue(&q, header_page->root_page_number, 0);
    int depth = 0;
    while (!IsEmpty(&q))
    {
        data d = dequeue(&q);
        file_read_page(d.pagenum, page);

        if (depth != d.depth)
        {
            depth++;
            printf("\n");
        }
        if (!page->header.isLeaf)
        {
            enqueue(&q, page->one_more_page_number, d.depth + 1);
            for (int i = 0; i < page->header.number_of_keys; i++)
            {
                enqueue(&q, page->entries[i].page_number, d.depth + 1);
                printf("%ld ", page->entries[i].key);
            }
            printf("(%ld %ld) ", d.pagenum, page->header.parent_page_number);
            printf("| ");
        }
        else
        {
            for (int i = 0; i < page->header.number_of_keys; i++)
            {
                printf("(%ld %s) ", page->records[i].key, page->records[i].value);
            }
            printf("(%ld %ld) ", d.pagenum, page->header.parent_page_number);
            printf("| ");
        }
    }
    printf("\n");
    free(page);
}

void print_leaf()
{
    header_read();
    if (header_page->root_page_number == 0)
    {
        printf("no page\n");
        return;
    }
    page_t *page = (page_t *)malloc(page_size);
    file_read_page(header_page->root_page_number, page);
    while (!page->header.isLeaf)
    {
        file_read_page(page->one_more_page_number, page);
    }
    while (true)
    {

        for (int i = 0; i < page->header.number_of_keys; i++)
        {
            printf("%ld ", page->records[i].key);
        }
        if (!page->right_sibling_number)
        {
            break;
        }
        file_read_page(page->right_sibling_number, page);
    }
    free(page);
    printf("\n");
}

void free_print()
{
    header_read();
    if (header_page->free_page_number == 0)
    {
        printf("no free page\n");
        return;
    }
    page_t *page = (page_t *)malloc(page_size);
    file_read_page(header_page->free_page_number, page);
    printf("%llu ", header_page->free_page_number);
    while (page->header.parent_page_number)
    {
        printf("%llu ", page->header.parent_page_number);
        file_read_page(page->header.parent_page_number, page);
    }
    printf("\n");
    free(page);
}

int db_insert(int64_t key, char *value)
{
    if (fd == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return fail;
    }
    header_read();

    char *tmp_val = (char *)malloc(val_size);
    if (db_find(key, tmp_val) == success)
    {
        free(tmp_val);
        return fail;
    }
    free(tmp_val);

    record_t *new_record = make_record(key, value);

    if (header_page->root_page_number == 0)
    {
        int ret = start_new_tree(new_record);
        return ret;
    }

    pagenum_t pagenum = find_leaf(key);

    // tree 내부에 저장하는 방법
    // 1. 일단
    page_t *leaf_page = (page_t *)malloc(page_size);
    file_read_page(pagenum, leaf_page);
    int ret;
    if (leaf_page->header.number_of_keys < leaf_order - 1)
    {
        ret = insert_into_leaf(leaf_page, pagenum, new_record);
    }

    // 2. splitting 구현
    else
    {
        ret = insert_into_leaf_after_splitting(leaf_page, pagenum, new_record);
    }
    return ret;
}

int db_find(int64_t key, char *ret_val)
{
    if (fd == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return fail;
    }
    page_t *page = (page_t *)malloc(page_size);
    pagenum_t pagenum = find_leaf(key);
    if (pagenum == 0)
    {
        free(page);
        return fail;
    }
    file_read_page(pagenum, page);

    int i;
    for (i = 0; i < page->header.number_of_keys; i++)
    {
        if (page->records[i].key == key)
        {
            strcpy(ret_val, page->records[i].value);
            free(page);
            return success;
        }
        else if (page->records[i].key > key)
        {
            break;
        }
    }
    free(page);
    return fail;
}

int db_delete(int64_t key)
{
    if (fd == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return fail;
    }
    header_read();

    char *tmp_val = (char *)malloc(val_size);
    int ret = db_find(key, tmp_val);
    pagenum_t key_pagenum = find_leaf(key);
    free(tmp_val);

    if (!ret && key_pagenum)
    {
        int ret = delete_entry(key_pagenum, key);
        return ret;
    }

    return fail;
}

/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
pagenum_t find_leaf(int64_t key)
{
    header_read();
    if (header_page->number_of_pages <= 1 || header_page->root_page_number == 0)
    {
        return 0;
    }
    page_t *page = (page_t *)malloc(page_size);
    pagenum_t pagenum = header_page->root_page_number;
    file_read_page(pagenum, page);
    while (!page->header.isLeaf)
    {
        int i;
        for (i = 0; i < page->header.number_of_keys; i++)
        {
            if (key < page->entries[i].key)
            {
                break;
            }
        }
        if (i == 0)
        {
            pagenum = page->one_more_page_number;
            file_read_page(pagenum, page);
        }
        else
        {
            pagenum = page->entries[i - 1].page_number;
            file_read_page(pagenum, page);
        }
    }
    free(page);
    return pagenum;
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut(int length)
{
    if (length % 2 == 0)
        return length / 2;
    else
        return length / 2 + 1;
}

// INSERTION
record_t *make_record(int64_t key, char *value)
{
    record_t *new_record = (record_t *)malloc(sizeof(record_t));
    if (new_record == NULL)
    {
        perror("Record creation.");
        exit(EXIT_FAILURE);
    }
    else
    {
        new_record->key = key;
        strcpy(new_record->value, value);
    }
    return new_record;
}

page_t *make_node(void)
{
    page_t *new_node;
    new_node = (page_t *)malloc(page_size);
    memset(new_node, 0, page_size);
    return new_node;
}

/* Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
page_t *make_leaf(void)
{
    page_t *leaf = make_node();
    leaf->header.isLeaf = true;
    return leaf;
}
/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
int get_left_index(page_t *parent, pagenum_t left_pagenum)
{

    int left_index = 0;
    while (left_index < parent->header.number_of_keys)
    {
        if (parent->entries[left_index].page_number == left_pagenum)
        {
            return left_index;
        }
        left_index++;
    }
    return -1;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
int insert_into_leaf(page_t *leaf, pagenum_t pagenum, record_t *new_record)
{

    int i, insertion_point;

    insertion_point = 0;
    while (insertion_point < leaf->header.number_of_keys && leaf->records[insertion_point].key < new_record->key)
        insertion_point++;

    for (i = leaf->header.number_of_keys; i > insertion_point; i--)
    {
        leaf->records[i].key = leaf->records[i - 1].key;
        strcpy(leaf->records[i].value, leaf->records[i - 1].value);
    }
    leaf->records[insertion_point].key = new_record->key;
    strcpy(leaf->records[insertion_point].value, new_record->value);
    leaf->header.number_of_keys++;

    file_write_page(pagenum, leaf);
    free(leaf);
    free(new_record);
    return 0;
}

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
int insert_into_leaf_after_splitting(page_t *leaf, pagenum_t pagenum, record_t *new_record)
{
    int ret;
    page_t *new_leaf;
    int insertion_index, split, i, j;
    int64_t new_key;

    new_leaf = make_leaf();

    record_t *temp_records = (record_t *)malloc(leaf_order * sizeof(record_t));

    insertion_index = 0;
    while (insertion_index < leaf_order - 1 && leaf->records[insertion_index].key < new_record->key)
        insertion_index++;

    for (i = 0, j = 0; i < leaf->header.number_of_keys; i++, j++)
    {
        if (j == insertion_index)
            j++;
        temp_records[j].key = leaf->records[i].key;
        strcpy(temp_records[j].value, leaf->records[i].value);
    }

    temp_records[insertion_index].key = new_record->key;
    strcpy(temp_records[insertion_index].value, new_record->value);

    leaf->header.number_of_keys = 0;

    split = cut(leaf_order - 1);

    for (i = 0; i < split; i++)
    {
        leaf->records[i].key = temp_records[i].key;
        strcpy(leaf->records[i].value, temp_records[i].value);
        leaf->header.number_of_keys++;
    }

    for (i = split, j = 0; i < leaf_order; i++, j++)
    {
        new_leaf->records[j].key = temp_records[i].key;
        strcpy(new_leaf->records[j].value, temp_records[i].value);
        new_leaf->header.number_of_keys++;
    }
    free(new_record);
    free(temp_records);

    new_leaf->right_sibling_number = leaf->right_sibling_number;
    new_leaf->header.parent_page_number = leaf->header.parent_page_number;

    pagenum_t new_pagenum = file_alloc_page();
    leaf->right_sibling_number = new_pagenum;

    new_key = new_leaf->records[0].key;

    ret = insert_into_parent(leaf, pagenum, new_key, new_leaf, new_pagenum);

    return ret;
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
int insert_into_node(page_t *parent, pagenum_t parent_pagenum,
                     int left_index, int64_t key, pagenum_t right_pagenum)
{
    int i;
    for (i = parent->header.number_of_keys - 1; i > left_index; i--)
    {
        parent->entries[i + 1].key = parent->entries[i].key;
        parent->entries[i + 1].page_number = parent->entries[i].page_number;
    }
    parent->entries[left_index + 1].key = key;
    parent->entries[left_index + 1].page_number = right_pagenum;
    parent->header.number_of_keys++;

    file_write_page(parent_pagenum, parent);
    free(parent);
    return 0;
}

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
int insert_into_node_after_splitting(page_t *old_page, pagenum_t old_pagenum, int left_index,
                                     int64_t key, pagenum_t right_pagenum)
{
    int ret;
    int i, j, split, k_prime;
    branch_factor_t *temp_branch;
    page_t *new_page;
    page_t *temp_page;
    pagenum_t new_pagenum = file_alloc_page();

    temp_page = (page_t *)malloc(page_size);
    temp_branch = (branch_factor_t *)malloc(branch_order * sizeof(branch_factor_t));

    for (i = 0, j = 0; i < old_page->header.number_of_keys; i++, j++)
    {
        if (j == left_index + 1)
            j++;
        temp_branch[j].key = old_page->entries[i].key;
        temp_branch[j].page_number = old_page->entries[i].page_number;
    }

    temp_branch[left_index + 1].key = key;
    temp_branch[left_index + 1].page_number = right_pagenum;

    /* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */
    split = cut(branch_order);
    new_page = make_node();
    old_page->header.number_of_keys = 0;
    for (i = 0; i < split - 1; i++)
    {
        old_page->entries[i].key = temp_branch[i].key;
        old_page->entries[i].page_number = temp_branch[i].page_number;
        old_page->header.number_of_keys++;
    }
    k_prime = temp_branch[split - 1].key;
    new_page->one_more_page_number = temp_branch[split - 1].page_number;
    for (++i, j = 0; i < branch_order; i++, j++)
    {
        new_page->entries[j].key = temp_branch[i].key;
        new_page->entries[j].page_number = temp_branch[i].page_number;
        new_page->header.number_of_keys++;
    }

    for (int i = 0; i < old_page->header.number_of_keys; i++)
    {
        file_read_page(old_page->entries[i].page_number, temp_page);
        temp_page->header.parent_page_number = old_pagenum;
        file_write_page(old_page->entries[i].page_number, temp_page);
    }

    file_read_page(new_page->one_more_page_number, temp_page);
    temp_page->header.parent_page_number = new_pagenum;
    file_write_page(new_page->one_more_page_number, temp_page);

    for (int i = 0; i < new_page->header.number_of_keys; i++)
    {
        file_read_page(new_page->entries[i].page_number, temp_page);
        temp_page->header.parent_page_number = new_pagenum;
        file_write_page(new_page->entries[i].page_number, temp_page);
    }
    free(temp_page);
    free(temp_branch);
    new_page->header.parent_page_number = old_page->header.parent_page_number;

    ret = insert_into_parent(old_page, old_pagenum, k_prime, new_page, new_pagenum);
    return ret;
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
int insert_into_parent(page_t *left, pagenum_t left_pagenum, int64_t key, page_t *right, pagenum_t right_pagenum)
{

    int left_index, ret;
    pagenum_t parent_pagenum = left->header.parent_page_number;

    /* Case: new root. */
    if (parent_pagenum == 0)
    {
        ret = insert_into_new_root(left, left_pagenum, key, right, right_pagenum);
        return ret;
    }

    /* Case: leaf or node. (Remainder of
     * function body.)  
     */
    page_t *parent = (page_t *)malloc(page_size);
    file_read_page(left->header.parent_page_number, parent);

    /* Find the parent's pointer to the left 
     * node.
     */
    /* Simple case: the new key fits into the node. 
     */

    left_index = get_left_index(parent, left_pagenum);
    file_write_page(right_pagenum, right);
    file_write_page(left_pagenum, left);
    free(left);
    free(right);

    if (parent->header.number_of_keys < branch_order - 1)
    {
        ret = insert_into_node(parent, parent_pagenum, left_index, key, right_pagenum);
    }

    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */
    else
    {
        ret = insert_into_node_after_splitting(parent, parent_pagenum, left_index, key, right_pagenum);
    }
    return ret;
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
int insert_into_new_root(page_t *left, pagenum_t left_pagenum, int64_t key, page_t *right, pagenum_t right_pagenum)
{
    header_read();

    page_t *root = make_node();

    pagenum_t root_pagenum = file_alloc_page();
    root->one_more_page_number = left_pagenum;

    root->entries[0].key = key;
    root->entries[0].page_number = right_pagenum;
    root->header.number_of_keys++;
    root->header.parent_page_number = 0;

    left->header.parent_page_number = root_pagenum;
    right->header.parent_page_number = root_pagenum;

    header_page->root_page_number = root_pagenum;

    header_write();
    file_write_page(right_pagenum, right);
    file_write_page(left_pagenum, left);
    file_write_page(root_pagenum, root);
    free(left);
    free(right);
    free(root);

    return 0;
}

/* First insertion:
 * start a new tree.
 */
int start_new_tree(record_t *record)
{
    header_read();
    page_t *root = make_leaf();
    pagenum_t pagenum = file_alloc_page();
    root->records[0].key = record->key;
    strcpy(root->records[0].value, record->value);
    root->header.number_of_keys++;
    header_page->root_page_number = pagenum;
    file_write_page(pagenum, root);
    header_write();
    free(record);
    free(root);
    return 0;
}

// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
Direction_data get_neighbor_index(page_t *page, pagenum_t pagenum)
{

    int i;

    Direction_data d;
    page_t *parent = (page_t *)malloc(page_size);
    file_read_page(page->header.parent_page_number, parent);

    for (i = 0; i < parent->header.number_of_keys; i++)
    {
        if (parent->entries[i].page_number == pagenum)
        {
            free(parent);
            d.data = i - 1;
            d.direction = 1;
            return d;
        }
    }
    free(parent);
    d.data = 0;
    d.direction = -1;
    return d;
}

pagenum_t get_neighbor_pagenum(page_t *page, int neighbor_index)
{
    page_t *parent = (page_t *)malloc(page_size);
    pagenum_t pagenum = page->header.parent_page_number;
    file_read_page(pagenum, parent);
    //printf("%d %d\n", parent->entries[0].key, parent->entries[0].page_number);
    pagenum_t ret_pagenum;
    if (neighbor_index == -1)
    {
        ret_pagenum = parent->one_more_page_number;
    }
    else
    {
        ret_pagenum = parent->entries[neighbor_index].page_number;
    }

    free(parent);
    return ret_pagenum;
}

int64_t get_parent_key(page_t *child, int key_index)
{
    page_t *parent = (page_t *)malloc(page_size);
    file_read_page(child->header.parent_page_number, parent);
    int64_t ret_key = parent->entries[key_index].key;
    free(parent);
    return ret_key;
}

page_t *remove_entry_from_node(page_t *page, pagenum_t pagenum, int64_t key)
{
    // Remove the key and shift other keys accordingly.
    int i = 0;

    if (page->header.isLeaf)
    {
        while (page->records[i].key != key)
            i++;
        for (++i; i < page->header.number_of_keys; i++)
        {
            page->records[i - 1].key = page->records[i].key;
            strcpy(page->records[i - 1].value, page->records[i].value);
        }
        page->records[i].key = 0;
        // strcpy(page->records[i].value, 0);
        page->header.number_of_keys--;
    }
    // Remove the pointer and shift other pointers accordingly.
    // First determine number of pointers.
    else
    {
        while (page->entries[i].key != key)
            i++;
        for (++i; i < page->header.number_of_keys; i++)
        {
            page->entries[i - 1].key = page->entries[i].key;
            page->entries[i - 1].page_number = page->entries[i].page_number;
        }
        page->entries[i].key = 0;
        page->entries[i].page_number = 0;
        page->header.number_of_keys--;
    }
    return page;
}

int adjust_root(page_t *root, pagenum_t pagenum)
{

    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    if (root->header.number_of_keys > 0)
    {
        file_write_page(pagenum, root);
        return 0;
    }
    /* Case: empty root. 
     */

    // If it has a child, promote
    // the first (only) child
    // as the new root.
    header_read();
    page_t *new_root = make_node();

    header_page->root_page_number = 0;
    if (!root->header.isLeaf)
    {
        pagenum_t new_root_pagenum = root->one_more_page_number;

        file_read_page(new_root_pagenum, new_root);
        new_root->header.parent_page_number = 0;
        file_write_page(new_root_pagenum, new_root);
        header_page->root_page_number = new_root_pagenum;
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.
    free(new_root);
    header_write();
    file_free_page(pagenum);
    return 0;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_nodes(page_t *page, pagenum_t pagenum, page_t *neighbor, pagenum_t neighbor_pagenum, int neighbor_index, int direction, int64_t k_prime)
{
    int i, j, neighbor_insertion_index, n_end;
    pagenum_t parent_pagenum = page->header.parent_page_number;

    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     */

    /* Case:  nonleaf node.
     * Append k_prime and the following pointer.
     * Append all pointers and keys from the neighbor.
     */

    if (!page->header.isLeaf)
    {
        page_t *child = (page_t *)malloc(page_size);
        if (direction == -1)
        {
            n_end = page->header.number_of_keys;
            neighbor_insertion_index = neighbor->header.number_of_keys;

            for (i = n_end + 1, j = 0; j < neighbor_insertion_index; i++, j++)
            {
                page->entries[i].key = neighbor->entries[j].key;
                page->entries[i].page_number = neighbor->entries[j].page_number;
                page->header.number_of_keys++;
            }
            page->entries[n_end].key = k_prime;
            page->entries[n_end].page_number = neighbor->one_more_page_number;
            page->header.number_of_keys++;

            file_read_page(page->one_more_page_number, child);
            child->header.parent_page_number = pagenum;
            file_write_page(page->one_more_page_number, child);
            for (int i = 0; i < page->header.number_of_keys; i++)
            {
                file_read_page(page->entries[i].page_number, child);
                child->header.parent_page_number = pagenum;
                file_write_page(page->entries[i].page_number, child);
            }

            file_free_page(neighbor_pagenum);
            file_write_page(pagenum, page);
        }
        else
        {
            n_end = page->header.number_of_keys;
            neighbor_insertion_index = neighbor->header.number_of_keys;

            for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++)
            {
                neighbor->entries[i].key = page->entries[j].key;
                neighbor->entries[i].page_number = page->entries[j].page_number;
                neighbor->header.number_of_keys++;
            }
            neighbor->entries[neighbor_insertion_index].key = k_prime;
            neighbor->entries[neighbor_insertion_index].page_number = page->one_more_page_number;
            neighbor->header.number_of_keys++;

            file_read_page(neighbor->one_more_page_number, child);
            child->header.parent_page_number = neighbor_pagenum;
            file_write_page(neighbor->one_more_page_number, child);
            for (int i = 0; i < neighbor->header.number_of_keys; i++)
            {
                file_read_page(neighbor->entries[i].page_number, child);
                child->header.parent_page_number = neighbor_pagenum;
                file_write_page(neighbor->entries[i].page_number, child);
            }

            file_free_page(pagenum);
            file_write_page(neighbor_pagenum, neighbor);
        }
        free(child);
    }

    else
    {
        if (direction == -1)
        {
            n_end = page->header.number_of_keys;
            for (int i = 0, j = n_end; i < neighbor->header.number_of_keys; i++, j++)
            {
                page->records[j].key = neighbor->records[i].key;
                strcpy(page->records[j].value, neighbor->records[i].value);
                page->header.number_of_keys++;
            }
            page->right_sibling_number = neighbor->right_sibling_number;
            file_free_page(neighbor_pagenum);
            file_write_page(pagenum, page);
        }
        else
        {
            n_end = neighbor->header.number_of_keys;
            for (int i = 0, j = n_end; i < page->header.number_of_keys; i++, j++)
            {
                neighbor->records[j].key = page->records[j].key;
                strcpy(neighbor->records[j].value, page->records[i].value);
                neighbor->header.number_of_keys++;
            }
            neighbor->right_sibling_number = page->right_sibling_number;
            file_free_page(pagenum);
            file_write_page(neighbor_pagenum, neighbor);
        }
    }
    free(page);
    free(neighbor);
    int ret = delete_entry(parent_pagenum, k_prime);
    return ret;
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
int redistribute_nodes(page_t *page, pagenum_t pagenum, page_t *neighbor, pagenum_t neighbor_pagenum, int neighbor_index, int direction, int k_prime_index, int64_t k_prime)
{

    int i;
    page_t *parent = (page_t *)malloc(page_size);
    page_t *temp_page = (page_t *)malloc(page_size);
    file_read_page(page->header.parent_page_number, parent);
    /* Case: n has a neighbor to the left. 
     * Pull the neighbor's last key-pointer pair over
     * from the neighbor's right end to n's left end.
     */

    if (direction != -1)
    {
        if (!page->header.isLeaf)
        {
            for (i = page->header.number_of_keys; i > 0; i--)
            {
                page->entries[i].key = page->entries[i - 1].key;
                page->entries[i].page_number = page->entries[i - 1].page_number;
            }
            page->entries[0].key = k_prime;
            page->entries[0].page_number = page->one_more_page_number;

            page->one_more_page_number = neighbor->entries[neighbor->header.number_of_keys - 1].page_number;
            parent->entries[k_prime_index].key = neighbor->entries[neighbor->header.number_of_keys - 1].key;

            file_read_page(page->one_more_page_number, temp_page);
            temp_page->header.parent_page_number = pagenum;
            file_write_page(page->one_more_page_number, temp_page);

            neighbor->entries[neighbor->header.number_of_keys - 1].page_number = 0;
            neighbor->entries[neighbor->header.number_of_keys - 1].key = 0;
        }
        else
        {
            for (i = page->header.number_of_keys; i > 0; i--)
            {
                page->records[i].key = page->records[i - 1].key;
                strcpy(page->records[i].value, page->records[i - 1].value);
            }

            page->records[0].key = neighbor->records[neighbor->header.number_of_keys - 1].key;
            strcpy(page->records[0].value, neighbor->records[neighbor->header.number_of_keys - 1].value);

            neighbor->records[neighbor->header.number_of_keys - 1].key = 0;

            parent->entries[k_prime_index].key = page->records[0].key;
        }
    }

    /* Case: n is the leftmost child.
     * Take a key-pointer pair from the neighbor to the right.
     * Move the neighbor's leftmost key-pointer pair
     * to n's rightmost position.
     */

    else
    {
        if (!page->header.isLeaf)
        {
            page->entries[page->header.number_of_keys].key = k_prime;
            page->entries[page->header.number_of_keys].page_number = neighbor->one_more_page_number;

            file_read_page(page->entries[page->header.number_of_keys].page_number, temp_page);
            temp_page->header.parent_page_number = pagenum;
            file_write_page(page->entries[page->header.number_of_keys].page_number, temp_page);

            parent->entries[k_prime_index].key = neighbor->entries[0].key;
            neighbor->one_more_page_number = neighbor->entries[0].page_number;

            for (i = 0; i < neighbor->header.number_of_keys - 1; i++)
            {
                neighbor->entries[i].key = neighbor->entries[i + 1].key;
                neighbor->entries[i].page_number = neighbor->entries[i + 1].page_number;
            }
        }
        else
        {
            page->records[page->header.number_of_keys].key = neighbor->records[0].key;
            strcpy(page->records[page->header.number_of_keys].value, neighbor->records[0].value);
            parent->entries[k_prime_index].key = neighbor->records[1].key;
            for (i = 0; i < neighbor->header.number_of_keys - 1; i++)
            {
                neighbor->records[i].key = neighbor->records[i + 1].key;
                strcpy(neighbor->records[i].value, neighbor->records[i + 1].value);
            }
        }
    }

    /* n now has one more key and one more pointer;
     * the neighbor has one fewer of each.
     */
    page->header.number_of_keys++;
    neighbor->header.number_of_keys--;
    file_write_page(pagenum, page);
    file_write_page(neighbor_pagenum, neighbor);
    file_write_page(page->header.parent_page_number, parent);
    free(parent);
    free(page);
    free(neighbor);
    return 0;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
int delete_entry(pagenum_t pagenum, int64_t key)
{
    // Remove key and pointer from node.
    page_t *page = (page_t *)malloc(page_size);
    int ret;
    file_read_page(pagenum, page);
    header_read();

    page = remove_entry_from_node(page, pagenum, key);

    /* Case:  deletion from the root. 
     */

    if (header_page->root_page_number == pagenum)
    {
        ret = adjust_root(page, pagenum);
        free(page);
        return ret;
    }

    /* Case:  deletion from a node below the root.
     * (Rest of function body.)
     */

    /* Case:  node stays at or above minimum.
     * (The simple case.)
     */

    if (page->header.number_of_keys > 0)
    {
        file_write_page(pagenum, page);
        free(page);
        return 0;
    }

    /* Case:  node falls below minimum.
     * Either coalescence or redistribution
     * is needed.
     */

    /* Find the appropriate neighbor node with which
     * to coalesce.
     * Also find the key (k_prime) in the parent
     * between the pointer to node n and the pointer
     * to the neighbor.
     */
    Direction_data d = get_neighbor_index(page, pagenum);
    int neighbor_index = d.data;
    int direction = d.direction;
    // printf("[direction] %d %d\n", d.data, d.direction);
    int k_prime_index = direction == -1 ? 0 : neighbor_index + 1;
    int64_t k_prime = get_parent_key(page, k_prime_index);
    // printf("[k_prime] %ld\n", k_prime);
    pagenum_t neighbor_pagenum = get_neighbor_pagenum(page, neighbor_index);
    // printf("[neighbor pagenum] %lu\n", neighbor_pagenum);
    page_t *neighbor = (page_t *)malloc(page_size);

    file_read_page(neighbor_pagenum, neighbor);

    int size = page->header.isLeaf ? leaf_order : branch_order - 1;

    /* Coalescence. */
    if (neighbor->header.number_of_keys < size)
        return coalesce_nodes(page, pagenum, neighbor, neighbor_pagenum, neighbor_index, direction, k_prime);

    /* Redistribution. */

    else
        return redistribute_nodes(page, pagenum, neighbor, neighbor_pagenum, neighbor_index, direction, k_prime_index, k_prime);
}