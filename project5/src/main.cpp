#include "bpt.h"
#include "buf.h"
#include "file.h"
#include "trx.h"
#include <pthread.h>
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <unistd.h>

using namespace std;
// MAIN

pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

int table_id = 1;
int flag = 0;

void *trx1(void *arg)
{
    pthread_mutex_lock(&mutex);
    int t1 = trx_begin();
    // int t3 = trx_begin();
    // int t4 = trx_begin();
    cout << 1 << endl;
    int ret;
    char a[120];
    char val[120] = "aaaa";
    ret = db_update(table_id, 1, val, t1);
    cout << ret << val << endl;
    while (flag != 1)
    {
        pthread_cond_wait(&cond, &mutex);
    }
    ret = db_update(table_id, 2, val, t1);
    cout << ret << val << endl;
    pthread_mutex_unlock(&mutex);
}

void *trx2(void *arg)
{
    pthread_mutex_lock(&mutex);
    int t2 = trx_begin();
    cout << 1 << endl;
    // int t3 = trx_begin();
    // int t4 = trx_begin();

    int ret;
    char a[120];
    char val[120] = "aaaa";
    ret = db_update(table_id, 1, val, t2);
    cout << ret << val << endl;
    ret = db_update(table_id, 2, val, t2);
    cout << ret << val << endl;
    flag = 1;
    pthread_cond_signal(&cond);

    pthread_mutex_unlock(&mutex);
}

int main(void)
{
    init_db(1000);
    init_lock_table();
    open_table(const_cast<char *>("test.db"));
    for (int i = 1; i <= 10000; i++)
    {
        db_insert(1, i, const_cast<char *>("aa"));
    }
    // printAll(1);

    pthread_t tx1;
    pthread_t tx2;

    pthread_create(&tx1, 0, trx1, NULL);
    pthread_create(&tx2, 0, trx2, NULL);
    pthread_join(tx1, NULL);
    pthread_join(tx2, NULL);

    // printAll(1);
    // shutdown_db();
}

// int main(int argc, char **argv)
// {
//     char command[10];
//     int table_id;
//     int64_t key;
//     char value[120];
//     char pathname[20];
//     //
//     //FILE *fp = fopen("sol.txt", "r");
//     //
//     usage();
//     while (true)
//     {
//         printf("> ");

//         scanf("%s", command);
//         //fscanf(fp, "%s", command);
//         if (!strcmp(command, "open"))
//         {
//             scanf("%s", pathname);
//             //fscanf(fp, "%s", pathname);
//             int ret = open_table(pathname);
//             if (ret < 0)
//             {
//                 printf("OPEN FAIL\n");
//             }
//             else
//             {
//                 printf("OPEN SUCCEED [table id : %d]\n", ret);
//             }
//         }
//         else if (!strcmp(command, "insert"))
//         {
//             scanf("%d %ld %s", &table_id, &key, value);
//             //fscanf(fp, "%d %ld %s", &table_id, &key, value);
//             if (!db_insert(table_id, key, value))
//             {
//                 printf("INSERTION SUCCEED [%d : %ld : %s]\n", table_id, key, value);
//             }
//             else
//             {
//                 printf("INSERTION FAIL [%d : %ld]\n", table_id, key);
//             }
//         }
//         else if (!strcmp(command, "find"))
//         {
//             scanf("%d %ld", &table_id, &key);
//             //fscanf(fp, "%d %ld", &table_id, &key);
//             char *ret_val = (char *)malloc(val_size);
//             if (!db_find(table_id, key, ret_val))
//             {
//                 printf("THIS KEY EXISTS [%d : %ld : %s]\n", table_id, key, ret_val);
//             }
//             else
//             {
//                 printf("THIS KEY IS NOT FOUNDED ON DISK [%d : %ld]\n", table_id, key);
//             }
//             free(ret_val);
//         }
//         else if (!strcmp(command, "delete"))
//         {
//             scanf("%d %ld", &table_id, &key);
//             //fscanf(fp, "%d %ld", &table_id, &key);
//             if (!db_delete(table_id, key))
//             {
//                 printf("DELETION SUCCEED [%d : %ld]\n", table_id, key);
//             }
//             else
//             {
//                 printf("DELETION FAIL [%d : %ld]\n", table_id, key);
//             }
//         }
//         else if (!strcmp(command, "print"))
//         {
//             scanf("%d", &table_id);
//             //fscanf(fp, "%d", &table_id);
//             printAll(table_id);
//         }
//         else if (!strcmp(command, "leaf"))
//         {
//             scanf("%d", &table_id);
//             //fscanf(fp, "%d", &table_id);
//             print_leaf(table_id);
//         }
//         else if (!strcmp(command, "free"))
//         {
//             scanf("%d", &table_id);
//             //fscanf(fp, "%d", &table_id);
//             free_print(table_id);
//         }
//         else if (!strcmp(command, "init"))
//         {
//             int buf_size;
//             scanf("%d", &buf_size);
//             //fscanf(fp, "%d", &buf_size);
//             if (!init_db(buf_size))
//             {
//                 printf("INIT SUCCEED [SIZE : %d]\n", buf_size);
//             }
//             else
//             {
//                 printf("INIT FAIL\n");
//             }
//         }
//         else if (!strcmp(command, "close"))
//         {
//             scanf("%d", &table_id);
//             //fscanf(fp, "%d", &table_id);
//             if (!close_table(table_id))
//             {
//                 printf("CLOSE TABLE SUCCEED [TABLE ID : %d]\n", table_id);
//             }
//             else
//             {
//                 printf("CLOSE TABLE FAIL [TABLE ID : %d]\n", table_id);
//             }
//         }
//         else if (!strcmp(command, "quit"))
//         {
//             shutdown_db();
//             //fclose(fp);
//             return 0;
//         }
//         else if (!strcmp(command, "help"))
//         {
//             usage();
//         }
//         else
//         {
//             printf("Please enter the correct command\n");
//         }
//     }
//     return 0;
// }

// int main(void)
// {
//     open_table("1.db");
//     init_db(4);
//     for (int64_t i = 1; i <= 10000; i++)
//     {
//         db_insert(1, i, "aa");
//         printf("%ld\n", i);
//     }
//     shutdown_db();
//     return 0;
// }