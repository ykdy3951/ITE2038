#include "bpt.h"
#include "file.h"
#include <inttypes.h>
#include <string.h>
#include <stdio.h>
// MAIN

int main(int argc, char **argv)
{
    header_page = (header_page_t *)malloc(page_size);
    fd = -1;
    char command[10];
    int64_t key;
    char value[120];

    while (true)
    {
        printf("> ");
        scanf("%s", command);
        if (!strcmp(command, "open"))
        {
            scanf("%s", value);
            if (open_table(value) < 0)
            {
                printf("OPEN FAIL\n");
            }
            else
            {
                printf("OPEN SUCCEED\n");
            }
        }
        else if (!strcmp(command, "insert"))
        {
            scanf("%ld %s", &key, value);
            if (!db_insert(key, value))
            {
                printf("INSERTION SUCCEED [%ld : %s]\n", key, value);
            }
            else
            {
                printf("INSERTION FAIL [%ld]\n", key);
            }
        }
        else if (!strcmp(command, "find"))
        {
            scanf("%ld", &key);
            char *ret_val = (char *)malloc(val_size);
            if (!db_find(key, ret_val))
            {
                printf("THIS KEY EXISTS [%ld : %s]\n", key, ret_val);
            }
            else
            {
                printf("THIS KEY IS NOT FOUNDED ON DISK [%ld]\n", key);
            }
            free(ret_val);
        }
        else if (!strcmp(command, "delete"))
        {
            scanf("%ld", &key);
            if (!db_delete(key))
            {
                printf("DELETION SUCCEED [%ld]\n", key);
            }
            else
            {
                printf("DELETION FAIL [%ld]\n", key);
            }
        }
        else if (!strcmp(command, "print"))
        {
            printAll();
        }
        else if (!strcmp(command, "leaf"))
        {
            print_leaf();
        }
        else if (!strcmp(command, "free"))
        {
            free_print();
        }
        else if (!strcmp(command, "quit"))
        {
            free(header_page);
            return 0;
        }
        else
        {
            printf("Please enter the correct command\n");
        }
    }
    return 0;
}

// int main(void)
// {

//     header_page = (header_page_t *)malloc(page_size);
//     char value[120] = "aa";
//     open_table("aag.db");
//     for (int64_t i = 1; i <= 10000; i++)
//     {
//         db_insert(i, value);
//         printf("insert %ld\n", i);
//     }
//     // print_leaf();
//     // printAll();
//     for (int64_t i = 11; i <= 10000; i++)
//     {
//         if (db_find(10000, value))
//         {
//             printf("%d\n\n\n", i);
//             break;
//         }
//         if (db_delete(i))
//         {
//             printf("1\n");
//         }
//         printf("delete %ld\n", i);
//     }
//     printAll();
//     print_leaf();
//     free_print();
//     free(header_page);
//     return 0;
// }