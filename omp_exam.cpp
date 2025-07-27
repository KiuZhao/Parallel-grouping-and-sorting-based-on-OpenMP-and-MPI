#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <dirent.h>

typedef struct {
    char** data;
    int size;
    int capacity;
} StringList;

void initStringList(StringList* list) {
    list->size = 0;
    list->capacity = 1024;
    list->data = (char**)malloc(sizeof(char*) * list->capacity);
}

void pushString(StringList* list, const char* str) {
    if (list->size >= list->capacity) {
        list->capacity *= 2;
        list->data = (char**)realloc(list->data, sizeof(char*) * list->capacity);
    }
    list->data[list->size++] = strdup(str);
}

void freeStringList(StringList* list) {
    for (int i = 0; i < list->size; i++) {
        free(list->data[i]);
    }
    free(list->data);
}

typedef struct Node {
    char* key;
    int count;
    struct Node* next;
} Node;

typedef struct {
    Node** buckets;
    omp_lock_t* locks;
    int capacity;
} HashMap;

unsigned long hash_string(const char* str) {
    unsigned long h = 5381;
    while (*str) h = ((h << 5) + h) + *str++;
    return h;
}

HashMap* create_hashmap(int cap) {
    HashMap* m = (HashMap*)malloc(sizeof(HashMap));
    m->capacity = cap;
    m->buckets = (Node**)calloc(cap, sizeof(Node*));
    m->locks = (omp_lock_t*)malloc(sizeof(omp_lock_t) * cap);
    for (int i = 0; i < cap; i++) omp_init_lock(&m->locks[i]);
    return m;
}

void hashmap_add(HashMap* m, const char* key, int cnt) {
    unsigned long h = hash_string(key) % m->capacity;
    omp_set_lock(&m->locks[h]);
    Node* cur = m->buckets[h];
    while (cur) {
        if (strcmp(cur->key, key) == 0) {
            cur->count += cnt;
            omp_unset_lock(&m->locks[h]);
            return;
        }
        cur = cur->next;
    }
    Node* n = (Node*)malloc(sizeof(Node));
    n->key = strdup(key);
    n->count = cnt;
    n->next = m->buckets[h];
    m->buckets[h] = n;
    omp_unset_lock(&m->locks[h]);
}

void destroy_hashmap(HashMap* m) {
    for (int i = 0; i < m->capacity; i++) {
        Node* n = m->buckets[i];
        while (n) {
            Node* next = n->next;
            free(n->key);
            free(n);
            n = next;
        }
        omp_destroy_lock(&m->locks[i]);
    }
    free(m->buckets);
    free(m->locks);
    free(m);
}

typedef struct {
    char* key;
    int count;
} Entry;

typedef struct {
    Entry* data;
    int size;
    int capacity;
} EntryList;

void initEntryList(EntryList* list) {
    list->size = 0;
    list->capacity = 1024;
    list->data = (Entry*)malloc(sizeof(Entry) * list->capacity);
}

void pushEntryRaw(EntryList* list, char* key, int count) {
    if (list->size >= list->capacity) {
        list->capacity *= 2;
        list->data = (Entry*)realloc(list->data, sizeof(Entry) * list->capacity);
    }
    list->data[list->size].key = key;
    list->data[list->size].count = count;
    list->size++;
}

void collect_from_hashmap(HashMap* m, EntryList* list) {
    for (int i = 0; i < m->capacity; i++) {
        Node* n = m->buckets[i];
        while (n) {
            pushEntryRaw(list, n->key, n->count);
            n = n->next;
        }
    }
}

void merge(Entry* arr, int l, int m, int r, Entry* temp) {
    int i = l, j = m+1, k = l;
    while (i <= m && j <= r) {
        if (arr[i].count > arr[j].count || 
           (arr[i].count == arr[j].count && strcmp(arr[i].key, arr[j].key) < 0)) {
            temp[k++] = arr[i++];
        } else {
            temp[k++] = arr[j++];
        }
    }
    while (i <= m) temp[k++] = arr[i++];
    while (j <= r) temp[k++] = arr[j++];
    for (i = l; i <= r; i++) arr[i] = temp[i];
}

void parallel_merge_sort(Entry* arr, int l, int r, Entry* temp) {
    if (l >= r) return;
    int m = (l + r) / 2;
    #pragma omp task shared(arr, temp) if (r-l > 1000)
    parallel_merge_sort(arr, l, m, temp);
    #pragma omp task shared(arr, temp) if (r-l > 1000)
    parallel_merge_sort(arr, m+1, r, temp);
    #pragma omp taskwait
    merge(arr, l, m, r, temp);
}

int main(int argc, char* argv[]) {
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

    double t0 = omp_get_wtime();

    for (int i = 0; i < 9; ++i) {
        const char* input = file_pairs[i][0];
        const char* output = file_pairs[i][1];

        printf("Processing file: %s -> %s\n", input, output);

        FILE* f = fopen(input, "r");
        if (!f) {
            fprintf(stderr, "Cannot open %s\n", input);
            continue;
        }

        StringList lines;
        initStringList(&lines);
        char buf[128];
        while (fgets(buf, sizeof(buf), f)) {
            buf[strcspn(buf, "\n")] = 0;
            pushString(&lines, buf);
        }
        fclose(f);

        int n = lines.size;
        int threads = omp_get_max_threads();
        omp_set_num_threads(threads);

        HashMap** locals = (HashMap**)malloc(sizeof(HashMap*) * threads);
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            locals[tid] = create_hashmap(1 << 18);
        }

        #pragma omp parallel for schedule(dynamic)
        for (int i = 0; i < n; i++) {
            int tid = omp_get_thread_num();
            hashmap_add(locals[tid], lines.data[i], 1);
        }

        HashMap* global = create_hashmap(1 << 20);
        #pragma omp parallel for schedule(dynamic)
        for (int t = 0; t < threads; t++) {
            EntryList tmp;
            initEntryList(&tmp);
            collect_from_hashmap(locals[t], &tmp);
            for (int i = 0; i < tmp.size; i++) {
                hashmap_add(global, tmp.data[i].key, tmp.data[i].count);
            }
            free(tmp.data);
            destroy_hashmap(locals[t]);
        }
        free(locals);

        EntryList result;
        initEntryList(&result);
        collect_from_hashmap(global, &result);

        Entry* temp = (Entry*)malloc(sizeof(Entry) * result.size);
        #pragma omp parallel
        {
            #pragma omp single nowait
            parallel_merge_sort(result.data, 0, result.size - 1, temp);
        }
        free(temp);

        FILE* fout = fopen(output, "w");
        if (fout) {
            fprintf(fout, "%d\n", result.size);
            for (int i = 0; i < result.size; i++) {
                fprintf(fout, "%s %d\n", result.data[i].key, result.data[i].count);
            }
            fclose(fout);
        }

        destroy_hashmap(global);
        free(result.data);
        freeStringList(&lines);
    }

    double t1 = omp_get_wtime();
    printf("OMP parallel processing completed in %.2f seconds.\n", t1 - t0);
    return 0;
}
