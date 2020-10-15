#include "bpt.h"
#include "file.h"
#include <inttypes.h>
#include <stdio.h>
// MAIN

int main(int argc, char **argv)
{
    header_page = (header_page_t *)malloc(page_size);
    char command;
    int64_t key;
    char value[120];

    printf("> ");
    while (scanf("%c", &command) != EOF)
    {
        switch (command)
        {
        case 'o':
            scanf("%s", value);
            open_table(value);
            break;
        case 'f':
            scanf("%lld", &key);
            char *ret_val = (char *)malloc(val_size);
            if (!db_find(key, ret_val))
            {
                printf("THIS KEY EXISTS [%lld : %s]\n", key, value);
            }
            else
            {
                printf("THIS KEY IS NOT FOUNDED ON DISK [%lld]\n", key);
            }
            free(ret_val);
            break;
        case 'i':
            scanf("%lld %s", &key, value);
            if (!db_insert(key, value))
            {
                printf("INSERTION SUCCEED [%lld : %s]\n", key, value);
            }
            else
            {
                printf("INSERTION FAIL [%lld]\n", key);
            }
            break;
        case 'd':
            scanf("%lld", &key);
            if (!db_delete(key))
            {
                printf("DELETION SUCCEED [%lld]\n", key);
            }
            else
            {
                printf("DELETION FAIL [%lld]\n", key);
            }
            break;
        case 'p':
            printAll();
            break;
        case 'l':
            print_leaf();
            break;
        case 'q':
            free(header_page);
            return 0;
        }
        while (getchar() != (int)'\n')
            ;
        printf("> ");
    }
}

// int main(void)
// {

//     header_page = (header_page_t *)malloc(page_size);
//     char value[120] = "aa";
//     open_table("aaa.db");
//     for (uint64_t i = 1; i <= 10000; i++)
//     {
//         db_insert(i, value);
//         printf("%lld\n", i);
//     }
//     print_leaf();
//     printAll();
//     free(header_page);
// }