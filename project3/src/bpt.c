/*
 *  bpt.c  
 */
#define Version "1.14"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "bpt.h"
#include "file.h"
#include "buf.h"
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
void usage(void)
{
    printf("Enter any of the following commands after the prompt > :\n"
           "\topen <pname>  -- pname에 해당하는 file을 열거나 생성\n"
           "\tinit <size>  -- buffer를 size만큼의 크기로 생성\n"
           "\tinsert <tid> <k> <v> -- tid에 해당하는 tree에 key와 value를 넣는다.\n"
           "\tdelete <tid> <k>  -- tid에 해당하는 tree에서 k 값을 지운다.\n"
           "\tprint <tid> -- tid에 해당하는 B+tree 출력\n"
           "\tleaf <tid> -- tid에 해당하는 tree의 leaf출력\n"
           "\tfree <tid> -- tid에 해당하는 tree의 free page들 출력\n"
           "\tclose <tid> -- tid에 해당하는 table을 닫는다.\n"
           "\tquit -- Quit.\n"
           "\thelp -- Print this help message.\n");
}

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

void printAll(int table_id)
{
    // 닫혀있거나 열리지 않은 테이블이면 종료
    if (table->fd_table[table_id] == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return;
    }

    // buffer가 없는 경우 종료
    if (buf == NULL)
    {
        printf("[ERROR] NO BUFFER.\n");
        return;
    }
    header_page_t *header_page;
    int header_idx = buf_read_page(table_id, 0);
    header_page = buf[header_idx].header_page;
    pagenum_t root_pagenum = header_page->root_page_number;
    buf_write_page(header_idx);

    // page 가 존재하지 않으면 종료한다.
    if (root_pagenum == 0)
    {
        printf("[ERROR] NO PAGE\n");
        return;
    }
    Queue q;
    InitQueue(&q);
    enqueue(&q, root_pagenum, 0);
    int depth = 0;
    int page_idx;

    while (!IsEmpty(&q))
    {
        data d = dequeue(&q);

        page_idx = buf_read_page(table_id, d.pagenum);
        page_t *page = buf[page_idx].page;

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

        buf_write_page(page_idx);
    }
    printf("\n");
}

void print_leaf(int table_id)
{
    // 닫혀있거나 열리지 않은 테이블이면 종료
    if (table->fd_table[table_id] == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return;
    }

    // buffer가 없는 경우 종료
    if (buf == NULL)
    {
        printf("[ERROR] NO BUFFER.\n");
        return;
    }
    // header에서 root pagenum을 가져오고 unpin시킨다.
    header_page_t *header_page;
    int header_idx = buf_read_page(table_id, 0);
    header_page = buf[header_idx].header_page;
    pagenum_t root_pagenum = header_page->root_page_number;
    buf_write_page(header_idx);

    // page가 존재하지 않으면 종료한다.
    if (root_pagenum == 0)
    {
        printf("[ERROR] NO PAGE\n");
        return;
    }

    page_t *page;
    int page_idx = buf_read_page(table_id, root_pagenum);
    page = buf[page_idx].page;
    while (!page->header.isLeaf)
    {
        pagenum_t temp_pagenum = page->one_more_page_number;
        buf_write_page(page_idx);
        page_idx = buf_read_page(table_id, temp_pagenum);
        page = buf[page_idx].page;
    }
    while (true)
    {
        pagenum_t temp_pagenum = page->right_sibling_number;
        for (int i = 0; i < page->header.number_of_keys; i++)
        {
            printf("%ld ", page->records[i].key);
        }
        if (!page->right_sibling_number)
        {
            break;
        }
        buf_write_page(page_idx);
        page_idx = buf_read_page(table_id, temp_pagenum);
        page = buf[page_idx].page;
    }
    buf_write_page(page_idx);
    printf("\n");
}

void free_print(int table_id)
{
    // 닫혀있거나 열리지 않은 테이블이면 종료
    if (table->fd_table[table_id] == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return;
    }

    // buffer가 없는 경우 종료
    if (buf == NULL)
    {
        printf("[ERROR] NO BUFFER.\n");
        return;
    }
    // header 처리
    header_page_t *header_page;
    int header_idx = buf_read_page(table_id, 0);
    header_page = buf[header_idx].header_page;
    pagenum_t free_pagenum = header_page->free_page_number;
    buf_write_page(header_idx);

    // free page가 존재하지 않으면 종료한다.
    if (free_pagenum == 0)
    {
        printf("[ERROR] NO FREE PAGE\n");
        return;
    }

    page_t *page;
    int page_idx = buf_read_page(table_id, free_pagenum);
    page = buf[page_idx].page;

    printf("%lu ", header_page->free_page_number);
    while (page->header.parent_page_number)
    {
        pagenum_t temp_pagenum = page->header.parent_page_number;
        printf("%lu ", page->header.parent_page_number);
        buf_write_page(page_idx);
        page_idx = buf_read_page(table_id, temp_pagenum);
        page = buf[page_idx].page;
    }
    printf("\n");
    buf_write_page(page_idx);
}

int db_insert(int table_id, int64_t key, char *value)
{
    // 닫혀있거나 열리지 않은 테이블이면 종료
    if (table->fd_table[table_id] == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return fail;
    }

    // buffer가 없는 경우 종료
    if (buf == NULL)
    {
        printf("[ERROR] NO BUFFER.\n");
        return fail;
    }

    // 해당 키를 찾아서 있으면 종료
    char *tmp_val = (char *)malloc(val_size);
    if (db_find(table_id, key, tmp_val) == success)
    {
        free(tmp_val);
        return fail;
    }
    free(tmp_val);

    record_t *new_record = make_record(key, value);

    header_page_t *header_page;
    int header_idx = buf_read_page(table_id, 0);
    header_page = buf[header_idx].header_page;

    pagenum_t root_pagenum = header_page->root_page_number;
    buf_write_page(header_idx);

    // printf("HEADER PAGE READ COMPLETE\n");

    if (root_pagenum == 0)
    {
        int ret = start_new_tree(table_id, new_record);
        return ret;
    }

    pagenum_t pagenum = find_leaf(table_id, key);

    // tree 내부에 저장하는 방법
    // 1. 일단
    page_t *leaf_page;
    int leaf_page_idx = buf_read_page(table_id, pagenum);
    leaf_page = buf[leaf_page_idx].page;

    int ret;
    if (leaf_page->header.number_of_keys < leaf_order - 1)
    {
        ret = insert_into_leaf(table_id, leaf_page_idx, new_record);
    }

    // 2. splitting 구현
    else
    {
        ret = insert_into_leaf_after_splitting(table_id, leaf_page_idx, new_record);
    }
    return ret;
}

int db_find(int table_id, int64_t key, char *ret_val)
{
    // 닫혀있거나 열리지 않은 테이블이면 종료
    if (table->fd_table[table_id] == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return fail;
    }

    // buffer가 없는 경우 종료
    if (buf == NULL)
    {
        printf("[ERROR] NO BUFFER.\n");
        return fail;
    }

    page_t *page;

    pagenum_t pagenum = find_leaf(table_id, key);
    if (pagenum == 0)
    {
        free(page);
        return fail;
    }

    int page_idx = buf_read_page(table_id, pagenum);
    page = buf[page_idx].page;
    int i;
    for (i = 0; i < page->header.number_of_keys; i++)
    {
        if (page->records[i].key == key)
        {
            strcpy(ret_val, page->records[i].value);
            buf_write_page(page_idx);
            return success;
        }
        else if (page->records[i].key > key)
        {
            break;
        }
    }
    buf_write_page(page_idx);
    return fail;
}

int db_delete(int table_id, int64_t key)
{
    // 닫혀있거나 열리지 않은 테이블이면 종료
    if (table->fd_table[table_id] == -1)
    {
        printf("[ERROR] YOU HAVE TO OPEN THE EXISTING DATA FILE.\n");
        return fail;
    }

    // buffer가 없는 경우 종료
    if (buf == NULL)
    {
        printf("[ERROR] NO BUFFER.\n");
        return fail;
    }

    // header_page_t *header_page = (header_page_t *)malloc(page_size);
    // buf_read_page(table_id, 0, header_page);

    char *tmp_val = (char *)malloc(val_size);
    int ret = db_find(table_id, key, tmp_val);
    pagenum_t key_pagenum = find_leaf(table_id, key);
    free(tmp_val);
    if (!ret && key_pagenum)
    {
        int ret = delete_entry(table_id, key_pagenum, key);
        return ret;
    }

    return fail;
}

/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
pagenum_t find_leaf(int table_id, int64_t key)
{
    header_page_t *header_page;
    int header_idx = buf_read_page(table_id, 0);
    header_page = buf[header_idx].header_page;

    if (header_page->number_of_pages <= 1 || header_page->root_page_number == 0)
    {
        buf_write_page(header_idx);
        return 0;
    }
    page_t *page;
    pagenum_t pagenum = header_page->root_page_number;

    int page_idx = buf_read_page(table_id, pagenum);
    page = buf[page_idx].page;

    buf_write_page(header_idx);

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

            // 사용이 끝났으므로 pin을 내려주고 LRU를 변경한다.
            buf_write_page(page_idx);

            page_idx = buf_read_page(table_id, pagenum);
            page = buf[page_idx].page;
        }
        else
        {
            pagenum = page->entries[i - 1].page_number;

            // 사용이 끝났으므로 pin을 내려주고 LRU를 변경한다.
            buf_write_page(page_idx);

            page_idx = buf_read_page(table_id, pagenum);
            page = buf[page_idx].page;
        }
    }
    buf_write_page(page_idx);
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
int insert_into_leaf(int table_id, int leaf_idx, record_t *new_record)
{

    int i, insertion_point;
    page_t *leaf = buf[leaf_idx].page;
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

    buf[leaf_idx].is_dirty = 1;

    buf_write_page(leaf_idx);
    free(new_record);
    return 0;
}

/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
int insert_into_leaf_after_splitting(int table_id, int leaf_idx, record_t *new_record)
{
    // 필요한 변수들 생성
    int ret;
    page_t *leaf, *new_leaf;
    int insertion_index, split, i, j;
    int64_t new_key;

    // new_leaf 생성
    new_leaf = make_leaf();

    // 임시 record 생성
    record_t *temp_records = (record_t *)malloc(leaf_order * sizeof(record_t));
    leaf = buf[leaf_idx].page;

    // 하나에 전부 넣음
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

    // 재분배
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

    // leaf 끼리 연결하기
    new_leaf->right_sibling_number = leaf->right_sibling_number;
    new_leaf->header.parent_page_number = leaf->header.parent_page_number;

    int new_leaf_idx = buf_alloc_page(table_id);
    memcpy(buf[new_leaf_idx].page, new_leaf, page_size);
    leaf->right_sibling_number = buf[new_leaf_idx].page_num;

    new_key = new_leaf->records[0].key;

    buf[leaf_idx].is_dirty = 1;
    buf[new_leaf_idx].is_dirty = 1;

    free(new_leaf);

    ret = insert_into_parent(table_id, leaf_idx, new_key, new_leaf_idx);

    return ret;
}

/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
int insert_into_node(int table_id, int parent_idx,
                     int left_index, int64_t key, pagenum_t right_pagenum)
{
    int i;
    page_t *parent = buf[parent_idx].page;
    for (i = parent->header.number_of_keys - 1; i > left_index; i--)
    {
        parent->entries[i + 1].key = parent->entries[i].key;
        parent->entries[i + 1].page_number = parent->entries[i].page_number;
    }
    parent->entries[left_index + 1].key = key;
    parent->entries[left_index + 1].page_number = right_pagenum;
    parent->header.number_of_keys++;

    buf[parent_idx].is_dirty = 1;

    buf_write_page(parent_idx);
    return 0;
}

/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
int insert_into_node_after_splitting(int table_id, int old_idx, int left_index,
                                     int64_t key, pagenum_t right_pagenum)
{
    int ret;
    int i, j, split, k_prime;
    int temp_idx;
    branch_factor_t *temp_branch;
    page_t *old_page;
    page_t *new_page;
    int new_page_idx = buf_alloc_page(table_id);
    pagenum_t old_pagenum = buf[old_idx].page_num;
    pagenum_t new_pagenum = buf[new_page_idx].page_num;

    old_page = buf[old_idx].page;
    temp_branch = (branch_factor_t *)malloc(branch_order * sizeof(branch_factor_t));

    // 한곳으로 모두 모은다.
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

    // child의 parent 바꿔주는 코드
    // 기존의 page
    for (int i = 0; i < old_page->header.number_of_keys; i++)
    {
        temp_idx = buf_read_page(table_id, old_page->entries[i].page_number);
        buf[temp_idx].page->header.parent_page_number = old_pagenum;

        buf[temp_idx].is_dirty = 1;
        buf_write_page(temp_idx);
    }

    // 새로운 page
    temp_idx = buf_read_page(table_id, new_page->one_more_page_number);
    buf[temp_idx].page->header.parent_page_number = new_pagenum;
    buf[temp_idx].is_dirty = 1;
    buf_write_page(temp_idx);

    for (int i = 0; i < new_page->header.number_of_keys; i++)
    {
        int temp_idx = buf_read_page(table_id, new_page->entries[i].page_number);
        buf[temp_idx].page->header.parent_page_number = new_pagenum;

        buf[temp_idx].is_dirty = 1;
        buf_write_page(temp_idx);
    }
    new_page->header.parent_page_number = old_page->header.parent_page_number;

    // 그후 buffer에 넣어주고 free 시켜준다.
    memcpy(buf[new_page_idx].page, new_page, page_size);

    // free 처리
    free(temp_branch);
    free(new_page);

    ret = insert_into_parent(table_id, old_idx, k_prime, new_page_idx);
    return ret;
}

/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
int insert_into_parent(int table_id, int left_idx, int64_t key, int right_idx)
{

    int left_index, ret;
    pagenum_t parent_pagenum = buf[left_idx].page->header.parent_page_number;
    pagenum_t right_pagenum = buf[right_idx].page_num;
    /* Case: new root. */
    if (parent_pagenum == 0)
    {
        ret = insert_into_new_root(table_id, left_idx, key, right_idx);
        return ret;
    }

    /* Case: leaf or node. (Remainder of
     * function body.)  
     */
    int parent_idx = buf_read_page(table_id, parent_pagenum);
    page_t *parent = buf[parent_idx].page;

    /* Find the parent's pointer to the left 
     * node.
     */
    /* Simple case: the new key fits into the node. 
     */

    left_index = get_left_index(parent, buf[left_idx].page_num);
    buf_write_page(left_idx);
    buf_write_page(right_idx);

    if (parent->header.number_of_keys < branch_order - 1)
    {
        ret = insert_into_node(table_id, parent_idx, left_index, key, right_pagenum);
    }

    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */
    else
    {
        ret = insert_into_node_after_splitting(table_id, parent_idx, left_index, key, right_pagenum);
    }
    return ret;
}

/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
int insert_into_new_root(int table_id, int left_idx, int64_t key, int right_idx)
{
    header_page_t *header_page;
    int header_idx = buf_read_page(table_id, 0);
    header_page = buf[header_idx].header_page;

    page_t *root = make_node();
    int root_idx = buf_alloc_page(table_id);
    pagenum_t root_pagenum = buf[root_idx].page_num;

    // root 처리
    root->one_more_page_number = buf[left_idx].page_num;
    root->entries[0].key = key;
    root->entries[0].page_number = buf[right_idx].page_num;
    root->header.number_of_keys++;
    root->header.parent_page_number = 0;
    memcpy(buf[root_idx].page, root, page_size);
    buf[root_idx].is_dirty = 1;

    // header 처리
    header_page->root_page_number = root_pagenum;
    buf[header_idx].is_dirty = 1;

    // child들 처리
    buf[left_idx].page->header.parent_page_number = root_pagenum;
    buf[right_idx].page->header.parent_page_number = root_pagenum;
    buf[left_idx].is_dirty = 1;
    buf[right_idx].is_dirty = 1;

    buf_write_page(header_idx);
    buf_write_page(root_idx);
    buf_write_page(left_idx);
    buf_write_page(right_idx);

    free(root);

    return 0;
}

/* First insertion:
 * start a new tree.
 */
int start_new_tree(int table_id, record_t *record)
{
    header_page_t *header_page;
    int header_idx = buf_read_page(table_id, 0);
    header_page = buf[header_idx].header_page;

    // printf("HEADER PAGE READ COMPLETE\n");

    page_t *root = make_leaf();
    int root_idx = buf_alloc_page(table_id);

    // printf("NEW PAGE ALLOC COMPLETE\n");

    root->records[0].key = record->key;
    strcpy(root->records[0].value, record->value);

    root->header.number_of_keys++;
    header_page->root_page_number = buf[root_idx].page_num;

    memcpy(buf[root_idx].page, root, page_size);

    buf[header_idx].is_dirty = 1;
    buf[root_idx].is_dirty = 1;

    buf_write_page(root_idx);
    buf_write_page(header_idx);
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
Direction_data get_neighbor_index(int table_id, page_t *page, pagenum_t pagenum)
{
    int i;

    Direction_data d;
    int parent_idx = buf_read_page(table_id, page->header.parent_page_number);
    page_t *parent = buf[parent_idx].page;

    for (i = 0; i < parent->header.number_of_keys; i++)
    {
        if (parent->entries[i].page_number == pagenum)
        {
            buf_put_page(parent_idx);
            d.data = i - 1;
            d.direction = 1;
            return d;
        }
    }
    buf_put_page(parent_idx);
    d.data = 0;
    d.direction = -1;
    return d;
}

pagenum_t get_neighbor_pagenum(int table_id, page_t *page, int neighbor_index)
{
    page_t *parent;
    pagenum_t pagenum = page->header.parent_page_number;

    int parent_idx = buf_read_page(table_id, pagenum);
    parent = buf[parent_idx].page;

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

    buf_write_page(parent_idx);
    return ret_pagenum;
}

int64_t get_parent_key(int table_id, page_t *child, int key_index)
{
    page_t *parent;
    int parent_idx = buf_read_page(table_id, child->header.parent_page_number);
    parent = buf[parent_idx].page;
    int64_t ret_key = parent->entries[key_index].key;
    buf_write_page(parent_idx);
    return ret_key;
}

page_t *remove_entry_from_node(int table_id, page_t *page, pagenum_t pagenum, int64_t key)
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

int adjust_root(int table_id, page_t *root, pagenum_t pagenum)
{

    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    if (root->header.number_of_keys > 0)
    {
        return 0;
    }
    /* Case: empty root. 
     */

    // If it has a child, promote
    // the first (only) child
    // as the new root.
    header_page_t *header_page;
    int header_idx = buf_read_page(table_id, 0);
    header_page = buf[header_idx].header_page;
    header_page->root_page_number = 0;

    if (!root->header.isLeaf)
    {
        // root page 재설정
        pagenum_t new_root_pagenum = root->one_more_page_number;

        int new_root_idx = buf_read_page(table_id, new_root_pagenum);
        buf[new_root_idx].page->header.parent_page_number = 0;

        // dirty 설정
        buf[new_root_idx].is_dirty = 1;
        buf_write_page(new_root_idx);

        header_page->root_page_number = new_root_pagenum;
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.
    buf_write_page(header_idx);
    buf_free_page(table_id, pagenum);
    return 0;
}

/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
int coalesce_nodes(int table_id, int page_idx, int neighbor_idx, int neighbor_index, int direction, int64_t k_prime)
{
    int i, j, neighbor_insertion_index, n_end;
    int temp_idx;
    page_t *page = buf[page_idx].page;
    page_t *neighbor = buf[neighbor_idx].page;
    pagenum_t pagenum = buf[page_idx].page_num;
    pagenum_t neighbor_pagenum = buf[neighbor_idx].page_num;
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

            // child 재설정
            temp_idx = buf_read_page(table_id, page->one_more_page_number);
            buf[temp_idx].page->header.parent_page_number = pagenum;
            buf[temp_idx].is_dirty = 1;
            buf_write_page(temp_idx);

            for (int i = 0; i < page->header.number_of_keys; i++)
            {
                temp_idx = buf_read_page(table_id, page->entries[i].page_number);
                buf[temp_idx].page->header.parent_page_number = pagenum;
                buf[temp_idx].is_dirty = 1;
                buf_write_page(temp_idx);
            }

            // page들 dirty 설정
            buf[page_idx].is_dirty = 1;
            buf[neighbor_idx].is_dirty = 1;

            buf_free_page(table_id, neighbor_pagenum);
            buf_write_page(page_idx);
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

            // child 재설정
            temp_idx = buf_read_page(table_id, neighbor->one_more_page_number);
            buf[temp_idx].page->header.parent_page_number = neighbor_pagenum;
            buf[temp_idx].is_dirty = 1;
            buf_write_page(temp_idx);

            for (int i = 0; i < neighbor->header.number_of_keys; i++)
            {
                temp_idx = buf_read_page(table_id, neighbor->entries[i].page_number);
                buf[temp_idx].page->header.parent_page_number = neighbor_pagenum;
                buf[temp_idx].is_dirty = 1;
                buf_write_page(temp_idx);
            }

            // page들 dirty 설정
            buf[page_idx].is_dirty = 1;
            buf[neighbor_idx].is_dirty = 1;

            buf_free_page(table_id, pagenum);
            buf_write_page(neighbor_idx);
        }
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

            // page들 dirty 설정
            buf[page_idx].is_dirty = 1;
            buf[neighbor_idx].is_dirty = 1;

            buf_free_page(table_id, neighbor_pagenum);
            buf_write_page(page_idx);
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

            // page들 dirty 설정
            buf[page_idx].is_dirty = 1;
            buf[neighbor_idx].is_dirty = 1;

            buf_free_page(table_id, pagenum);
            buf_write_page(neighbor_idx);
        }
    }
    int ret = delete_entry(table_id, parent_pagenum, k_prime);
    return ret;
}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
int redistribute_nodes(int table_id, int page_idx, int neighbor_idx, int neighbor_index, int direction, int k_prime_index, int64_t k_prime)
{
    // temp_idx는 child의 부모를 바꿔 주기 위해서 필요한 local variable이다.
    int i, temp_idx, parent_idx;
    page_t *page = buf[page_idx].page;
    page_t *neighbor = buf[neighbor_idx].page;
    pagenum_t pagenum = buf[page_idx].page_num;
    pagenum_t neighbor_pagenum = buf[neighbor_idx].page_num;
    parent_idx = buf_read_page(table_id, page->header.parent_page_number);
    page_t *parent = buf[parent_idx].page;

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

            // 자식들의 부모를 재설정 해준 뒤 dirty 설정을 해준다.
            temp_idx = buf_read_page(table_id, page->one_more_page_number);
            buf[temp_idx].page->header.parent_page_number = pagenum;
            buf[temp_idx].is_dirty = 1;
            buf_write_page(temp_idx);

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

            temp_idx = buf_read_page(table_id, page->entries[page->header.number_of_keys].page_number);
            buf[temp_idx].page->header.parent_page_number = pagenum;
            buf[temp_idx].is_dirty = 1;
            buf_write_page(temp_idx);

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

    // dirty 설정
    buf[page_idx].is_dirty = 1;
    buf[neighbor_idx].is_dirty = 1;
    buf[parent_idx].is_dirty = 1;

    // unpin
    buf_write_page(page_idx);
    buf_write_page(neighbor_idx);
    buf_write_page(parent_idx);
    return 0;
}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
int delete_entry(int table_id, pagenum_t pagenum, int64_t key)
{
    // Remove key and pointer from node.
    int ret;
    page_t *page;
    int page_idx = buf_read_page(table_id, pagenum);
    page = buf[page_idx].page;

    int header_idx = buf_read_page(table_id, 0);
    pagenum_t root_pagenum = buf[header_idx].header_page->root_page_number;
    buf_write_page(header_idx);

    page = remove_entry_from_node(table_id, page, pagenum, key);
    buf[page_idx].is_dirty = 1;
    /* Case:  deletion from the root. 
     */

    if (root_pagenum == pagenum)
    {
        ret = adjust_root(table_id, page, pagenum);
        buf_write_page(page_idx);
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
        buf_write_page(page_idx);
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
    Direction_data d = get_neighbor_index(table_id, page, pagenum);
    int neighbor_index = d.data;
    int direction = d.direction;
    // printf("[direction] %d %d\n", d.data, d.direction);
    int k_prime_index = direction == -1 ? 0 : neighbor_index + 1;
    int64_t k_prime = get_parent_key(table_id, page, k_prime_index);
    // printf("[k_prime] %ld\n", k_prime);
    pagenum_t neighbor_pagenum = get_neighbor_pagenum(table_id, page, neighbor_index);
    // printf("[neighbor pagenum] %lu\n", neighbor_pagenum);
    int neighbor_idx = buf_read_page(table_id, neighbor_pagenum);
    int size = page->header.isLeaf ? leaf_order : branch_order - 1;

    /* Coalescence. */
    if (buf[neighbor_idx].page->header.number_of_keys < size)
        return coalesce_nodes(table_id, page_idx, neighbor_idx, neighbor_index, direction, k_prime);

    /* Redistribution. */

    else
        return redistribute_nodes(table_id, page_idx, neighbor_idx, neighbor_index, direction, k_prime_index, k_prime);
}