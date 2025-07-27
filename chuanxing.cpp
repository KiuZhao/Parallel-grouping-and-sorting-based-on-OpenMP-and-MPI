#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_KEY_LEN 33
#define HASH_CAPACITY (1 << 20)

typedef struct Node {
    char key[MAX_KEY_LEN];
    int count;
    struct Node* next;
} Node;

typedef struct {
    Node** buckets;
    int capacity;
} HashMap;

unsigned long hash_string(const char* str) {
    unsigned long h = 5381;
    int c;
    while ((c = *str++)) {
        h = ((h << 5) + h) ^ c;
    }
    return h;
}

HashMap* create_hashmap(int capacity) {
    HashMap* map = (HashMap*)malloc(sizeof(HashMap));
    map->capacity = capacity;
    map->buckets = (Node**)calloc(capacity, sizeof(Node*));
    return map;
}

void hashmap_put(HashMap* map, const char* key) {
    unsigned long h = hash_string(key) % map->capacity;
    Node* cur = map->buckets[h];
    
    while (cur != NULL) {
        if (strcmp(cur->key, key) == 0) {
            cur->count++;
            return;
        }
        cur = cur->next;
    }
    
    Node* newNode = (Node*)malloc(sizeof(Node));
    strncpy(newNode->key, key, MAX_KEY_LEN - 1);
    newNode->key[MAX_KEY_LEN - 1] = '\0';
    newNode->count = 1;
    newNode->next = map->buckets[h];
    map->buckets[h] = newNode;
}

void free_hashmap(HashMap* map) {
    for (int i = 0; i < map->capacity; i++) {
        Node* cur = map->buckets[i];
        while (cur != NULL) {
            Node* next = cur->next;
            free(cur);
            cur = next;
        }
    }
    free(map->buckets);
    free(map);
}

typedef struct {
    char key[MAX_KEY_LEN];
    int count;
} Entry;

int compare_entries(const void* a, const void* b) {
    const Entry* ea = (const Entry*)a;
    const Entry* eb = (const Entry*)b;
    
    if (ea->count != eb->count) {
        return eb->count - ea->count; // 降序
    }
    return strcmp(ea->key, eb->key); // 升序
}

void process_file(const char* input_file, const char* output_file) {
    FILE* file = fopen(input_file, "r");
    if (!file) {
        perror("Cannot open input file");
        exit(1);
    }
    
    HashMap* map = create_hashmap(HASH_CAPACITY);
    char line[MAX_KEY_LEN];
    
    while (fgets(line, sizeof(line), file)) {
        line[strcspn(line, "\n")] = '\0';
        hashmap_put(map, line);
    }
    fclose(file);
    
    // 收集所有条目
    int unique_count = 0;
    for (int i = 0; i < map->capacity; i++) {
        Node* cur = map->buckets[i];
        while (cur) {
            unique_count++;
            cur = cur->next;
        }
    }
    
    Entry* entries = (Entry*)malloc(unique_count * sizeof(Entry));
    int index = 0;
    for (int i = 0; i < map->capacity; i++) {
        Node* cur = map->buckets[i];
        while (cur) {
            strncpy(entries[index].key, cur->key, MAX_KEY_LEN);
            entries[index].count = cur->count;
            index++;
            cur = cur->next;
        }
    }
    
    free_hashmap(map);
    
    // 排序：频率降序，字典序升序
    qsort(entries, unique_count, sizeof(Entry), compare_entries);
    
    // 写入输出文件
    FILE* out = fopen(output_file, "w");
    if (!out) {
        perror("Cannot open output file");
        free(entries);
        exit(1);
    }
    
    fprintf(out, "%d\n", unique_count);
    for (int i = 0; i < unique_count; i++) {
        fprintf(out, "%s %d\n", entries[i].key, entries[i].count);
    }
    fclose(out);
    free(entries);
}

int main() {
    const char* file_pairs[][2] = {
        {"dataset/data_8_1M.txt", "output/result8-1M.txt"},
        {"dataset/data_8_10M.txt", "output/result8-10M.txt"},
        {"dataset/data_8_40M.txt", "output/result8-40M.txt"},
        {"dataset/data_16_1M.txt", "output/result16-1M.txt"},
        {"dataset/data_16_10M.txt", "output/result16-10M.txt"},
        {"dataset/data_16_40M.txt", "output/result16-40M.txt"},
        {"dataset/data_24_1M.txt", "output/result24-1M.txt"},
        {"dataset/data_24_10M.txt", "output/result24-10M.txt"},
        {"dataset/data_24_40M.txt", "output/result24-40M.txt"}
    };
    
    double start_time = (double)clock() / CLOCKS_PER_SEC;
    
    for (int i = 0; i < 9; i++) {
        printf("Processing: %s -> %s\n", file_pairs[i][0], file_pairs[i][1]);
        double file_start = (double)clock() / CLOCKS_PER_SEC;
        process_file(file_pairs[i][0], file_pairs[i][1]);
        double file_end = (double)clock() / CLOCKS_PER_SEC;
        printf("  Time: %.3f seconds\n", file_end - file_start);
    }
    
    double end_time = (double)clock() / CLOCKS_PER_SEC;
    printf("Total processing time: %.2f seconds\n", end_time - start_time);
    
    return 0;
}