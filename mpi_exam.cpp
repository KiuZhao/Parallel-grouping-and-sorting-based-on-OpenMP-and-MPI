#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mpi.h>

#define BUCKET_SIZE (1 << 24)
#define MAX_KEY_LEN 33
#define INITIAL_POOL_SIZE (1 << 27)

struct HashNode {
    char key[MAX_KEY_LEN];
    int value;
    HashNode* next;
};

inline void safe_strcpy(char* dest, const char* src) {
    int i = 0;
    while (src[i] && i < MAX_KEY_LEN - 1) {
        dest[i] = src[i];
        i++;
    }
    dest[i] = '\0';
}

class NodePool {
private:
    HashNode* pool;
    int index, size;

public:
    NodePool() : index(0), size(INITIAL_POOL_SIZE) {
        pool = (HashNode*)malloc(sizeof(HashNode) * size);
        if (!pool) {
            fprintf(stderr, "NodePool alloc failed\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    ~NodePool() { free(pool); }

    HashNode* alloc(const char* key) {
        if (index >= size) {
            size_t new_size = size * 2;
            HashNode* new_pool = (HashNode*)realloc(pool, sizeof(HashNode) * new_size);
            if (!new_pool) {
                fprintf(stderr, "NodePool realloc failed\n");
                MPI_Abort(MPI_COMM_WORLD, 1);
            }
            pool = new_pool;
            size = new_size;
        }
        HashNode* node = &pool[index++];
        safe_strcpy(node->key, key);
        node->value = 1;
        node->next = NULL;
        return node;
    }
};

class HashTable {
private:
    HashNode** buckets;
    NodePool* pool;
    int capacity;
    unsigned int mask;

    inline unsigned int hash(const char* str) const {
        unsigned long h = 5381;
        int c;
        while ((c = *str++))
            h = ((h << 5) + h) ^ c;
        return h & mask;
    }

public:
    HashTable(NodePool* p, int cap = BUCKET_SIZE) : pool(p), capacity(cap), mask(cap - 1) {
        buckets = (HashNode**)calloc(capacity, sizeof(HashNode*));
        if (!buckets) {
            fprintf(stderr, "HashTable alloc failed\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    ~HashTable() { free(buckets); }

    void insert(const char* key) {
        unsigned int idx = hash(key);
        HashNode* node = buckets[idx];
        while (node) {
            if (strcmp(node->key, key) == 0) {
                node->value++;
                return;
            }
            node = node->next;
        }
        HashNode* new_node = pool->alloc(key);
        new_node->next = buckets[idx];
        buckets[idx] = new_node;
    }

    int flatten(HashNode** array) {
        int count = 0;
        for (int i = 0; i < capacity; ++i) {
            HashNode* node = buckets[i];
            while (node) {
                if (array) array[count] = node;
                count++;
                node = node->next;
            }
        }
        return count;
    }
};

struct Entry {
    char key[MAX_KEY_LEN];
    int value;
};

int cmp_key(const Entry* a, const Entry* b) {
    return strcmp(a->key, b->key);
}

int cmp_value(const Entry* a, const Entry* b) {
    if (a->value != b->value) {
        return b->value - a->value;
    }
    return strcmp(a->key, b->key);
}

void merge_entries(Entry* arr, int left, int mid, int right, int (*cmp)(const Entry*, const Entry*)) {
    int n1 = mid - left + 1;
    int n2 = right - mid;

    Entry* L = (Entry*)malloc(n1 * sizeof(Entry));
    Entry* R = (Entry*)malloc(n2 * sizeof(Entry));

    for (int i = 0; i < n1; i++)
        L[i] = arr[left + i];
    for (int j = 0; j < n2; j++)
        R[j] = arr[mid + 1 + j];

    int i = 0, j = 0, k = left;
    while (i < n1 && j < n2) {
        if (cmp(&L[i], &R[j]) <= 0) {
            arr[k++] = L[i++];
        } else {
            arr[k++] = R[j++];
        }
    }

    while (i < n1) arr[k++] = L[i++];
    while (j < n2) arr[k++] = R[j++];

    free(L);
    free(R);
}

void merge_sort(Entry* arr, int l, int r, int (*cmp)(const Entry*, const Entry*)) {
    if (l < r) {
        int m = l + (r - l) / 2;
        merge_sort(arr, l, m, cmp);
        merge_sort(arr, m + 1, r, cmp);
        merge_entries(arr, l, m, r, cmp);
    }
}

Entry* merge_sorted_entries(Entry* arr1, int n1, Entry* arr2, int n2, int* merged_size) {
    *merged_size = n1 + n2;
    Entry* merged = (Entry*)malloc((*merged_size) * sizeof(Entry));
    int i = 0, j = 0, k = 0;
    
    while (i < n1 && j < n2) {
        if (cmp_key(&arr1[i], &arr2[j]) <= 0) {
            merged[k++] = arr1[i++];
        } else {
            merged[k++] = arr2[j++];
        }
    }
    
    while (i < n1) merged[k++] = arr1[i++];
    while (j < n2) merged[k++] = arr2[j++];
    
    return merged;
}

void merge_same_keys(Entry* entries, int* count) {
    if (*count <= 1) return;
    
    int unique_count = 0;
    for (int i = 1; i < *count; i++) {
        if (strcmp(entries[unique_count].key, entries[i].key) == 0) {
            entries[unique_count].value += entries[i].value;
        } else {
            unique_count++;
            entries[unique_count] = entries[i];
        }
    }
    unique_count++;
    *count = unique_count;
}

void group_by_mpi(const char* input_file, const char* output_file) {
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    MPI_File fh;
    if (MPI_File_open(MPI_COMM_WORLD, input_file, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh) != MPI_SUCCESS) {
        if (rank == 0) fprintf(stderr, "Cannot open input file: %s\n", input_file);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Offset file_size;
    MPI_File_get_size(fh, &file_size);

    MPI_Offset chunk_size = file_size / size;
    MPI_Offset remainder = file_size % size;
    MPI_Offset start = rank * chunk_size + (rank < remainder ? rank : remainder);
    MPI_Offset end = start + chunk_size - 1;
    if (rank < remainder) end += 1;

    if (rank != 0 && start > 0) {
        char prev_char;
        MPI_File_read_at(fh, start - 1, &prev_char, 1, MPI_CHAR, MPI_STATUS_IGNORE);
        if (prev_char != '\n') {
            MPI_Offset pos = start - 1;
            while (pos >= 0) {
                MPI_File_read_at(fh, pos, &prev_char, 1, MPI_CHAR, MPI_STATUS_IGNORE);
                if (prev_char == '\n') {
                    start = pos + 1;
                    break;
                }
                if (pos == 0) break;
                pos--;
            }
        }
    }

    if (rank == size - 1) end = file_size - 1;

    MPI_Offset read_size = end - start + 1;
    if (read_size <= 0) read_size = 0;

    char* local_buf = NULL;
    if (read_size > 0) {
        local_buf = (char*)malloc(read_size + 1);
        MPI_File_read_at(fh, start, local_buf, read_size, MPI_CHAR, MPI_STATUS_IGNORE);
        local_buf[read_size] = '\0';
    }
    MPI_File_close(&fh);

    NodePool pool;
    HashTable table(&pool, BUCKET_SIZE);
    
    if (local_buf) {
        char* ptr = local_buf;
        while (*ptr) {
            char* end_ptr = strchr(ptr, '\n');
            if (!end_ptr) break;
            
            if (end_ptr - ptr < MAX_KEY_LEN) {
                char temp = *end_ptr;
                *end_ptr = '\0';
                table.insert(ptr);
                *end_ptr = temp;
            }
            ptr = end_ptr + 1;
        }
        free(local_buf);
    }

    int local_count = table.flatten(NULL);
    HashNode** nodes = (HashNode**)malloc(local_count * sizeof(HashNode*));
    table.flatten(nodes);
    
    Entry* local_entries = (Entry*)malloc(local_count * sizeof(Entry));
    for (int i = 0; i < local_count; ++i) {
        safe_strcpy(local_entries[i].key, nodes[i]->key);
        local_entries[i].value = nodes[i]->value;
    }
    free(nodes);

    if (local_count > 1) {
        merge_sort(local_entries, 0, local_count - 1, cmp_key);
    }

    merge_same_keys(local_entries, &local_count);

    int step = 1;
    while (step < size) {
        if (rank % (2 * step) == 0) {
            int src_rank = rank + step;
            if (src_rank < size) {
                int src_count;
                MPI_Recv(&src_count, 1, MPI_INT, src_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                Entry* src_entries = NULL;
                if (src_count > 0) {
                    src_entries = (Entry*)malloc(src_count * sizeof(Entry));
                    MPI_Recv(src_entries, src_count * sizeof(Entry), MPI_BYTE, 
                            src_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }

                int merged_count;
                Entry* merged_entries = merge_sorted_entries(
                    local_entries, local_count, 
                    src_entries, src_count, 
                    &merged_count
                );

                merge_same_keys(merged_entries, &merged_count);

                free(local_entries);
                if (src_entries) free(src_entries);
                local_entries = merged_entries;
                local_count = merged_count;
            }
        } else {
            int dst_rank = rank - step;
            MPI_Send(&local_count, 1, MPI_INT, dst_rank, 0, MPI_COMM_WORLD);
            if (local_count > 0) {
                MPI_Send(local_entries, local_count * sizeof(Entry), MPI_BYTE, dst_rank, 0, MPI_COMM_WORLD);
            }
            free(local_entries);
            local_entries = NULL;
            local_count = 0;
            break;
        }
        step *= 2;
    }

    if (rank == 0 && local_entries) {
        if (local_count > 1) {
            merge_sort(local_entries, 0, local_count - 1, cmp_value);
        }

        FILE* out = fopen(output_file, "w");
        if (!out) {
            fprintf(stderr, "Cannot open output file: %s\n", output_file);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
        
        fprintf(out, "%d\n", local_count);
        for (int i = 0; i < local_count; ++i) {
            fprintf(out, "%s %d\n", local_entries[i].key, local_entries[i].value);
        }
        fclose(out);
    }

    if (local_entries) free(local_entries);
}

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

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

    double total_start = MPI_Wtime();
    for (int i = 0; i < 9; ++i) {
        if (rank == 0) {
            printf("Processing file: %s -> %s\n", file_pairs[i][0], file_pairs[i][1]);
        }
        double file_start = MPI_Wtime();
        group_by_mpi(file_pairs[i][0], file_pairs[i][1]);
        double file_end = MPI_Wtime();
        if (rank == 0) {
            printf("File processed in %.3f seconds\n", file_end - file_start);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }
    double total_end = MPI_Wtime();
    if (rank == 0) {
        printf("MPI parallel processing completed in %.2f seconds.\n", total_end - total_start);
    }

    MPI_Finalize();
    return 0;
}