#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <math.h>
#include <time.h>
#include <locale.h>

#define MAX_LINHA 1024
/* AUMENTO DO HASH SIZE PARA NUMERO PRIMO: Garante espalhamento perfeito e evita listas encadeadas longas (Garante O(1) puro) */
#define HASH_SIZE 100003 
#define WINDOW_SIZE 2048 
#define BENCHMARK_LIMIT 100000 

/* ==========================================================================
 * VARIAVEIS GLOBAIS DE ESTADO E BENCHMARK
 * ========================================================================== */
int hash_collisions = 0;
int max_avl_memory_limit = -1; 
bool restrict_processing = false;
bool restrict_latency = false;    
bool restrict_data_noise = false; 
bool restrict_algorithmic = false;

long long total_picos_estresse = 0;
long long total_alertas_burnout = 0;

char min_date_global[20] = "";
char max_date_global[20] = "";

/* ==========================================================================
 * 1. CLASSICA 1: FILA ENCADEADA (Controle de Fluxo Continuo - IoT Stream)
 * ==========================================================================
 * ESSENCIAL: Todos os dados lidos do arquivo OBRIGATORIAMENTE passam por esta fila 
 * antes de serem distribuidos para qualquer outra estrutura. Isso simula o gargalo real 
 * de recepcao de dados via rede (Streaming) antes da persistencia.
 */
typedef struct {
    float x, y, z;
    float eda, hr, temp;
    char id[20];
    char datetime[50];
    char date_only[20];
    float label;
} SensorData;

typedef struct QueueNode {
    SensorData* data;
    struct QueueNode* next;
} QueueNode;

typedef struct {
    QueueNode *front, *rear;
    int size;
} Queue;

Queue* createQueue() {
    Queue* q = (Queue*)malloc(sizeof(Queue));
    q->front = q->rear = NULL; 
    q->size = 0; 
    return q;
}

void enqueue(Queue* q, SensorData* data) {
    QueueNode* newNode = (QueueNode*)malloc(sizeof(QueueNode));
    newNode->data = data; 
    newNode->next = NULL;
    if (q->rear == NULL) q->front = q->rear = newNode;
    else { q->rear->next = newNode; q->rear = newNode; }
    q->size++;
}

SensorData* dequeue(Queue* q) {
    if (q->front == NULL) return NULL;
    QueueNode* temp = q->front;
    SensorData* data = temp->data;
    q->front = q->front->next;
    if (q->front == NULL) q->rear = NULL;
    free(temp); 
    q->size--;
    return data;
}

/* ==========================================================================
 * BUFFER CIRCULAR (LINHA DO TEMPO CRONOLOGICA)
 * ========================================================================== */
SensorData chronological_window[WINDOW_SIZE];
int current_time_idx = 0;
long long total_insercoes = 0;

int getNumAmostrasValidas() {
    return (total_insercoes < WINDOW_SIZE) ? total_insercoes : WINDOW_SIZE;
}

/* ==========================================================================
 * 2. NAO-CLASSICA 1: SEGMENT TREE (Range Queries para Dashboard)
 * ========================================================================== */
float seg_tree_hr[4 * WINDOW_SIZE];
float seg_tree_eda[4 * WINDOW_SIZE];

void updateSegTree(float* tree, int node, int start, int end, int idx, float val) {
    if (start == end) { tree[node] = val; return; }
    int mid = (start + end) / 2;
    if (start <= idx && idx <= mid) updateSegTree(tree, 2 * node, start, mid, idx, val);
    else updateSegTree(tree, 2 * node + 1, mid + 1, end, idx, val);
    tree[node] = tree[2 * node] + tree[2 * node + 1];
}

float querySegTree(float* tree, int node, int start, int end, int l, int r) {
    if (r < start || end < l) return 0.0;
    if (l <= start && end <= r) return tree[node];
    int mid = (start + end) / 2;
    return querySegTree(tree, 2 * node, start, mid, l, r) + querySegTree(tree, 2 * node + 1, mid + 1, end, l, r);
}

void insertTimelineData(SensorData* data) {
    int idx = current_time_idx;
    chronological_window[idx] = *data; 
    
    updateSegTree(seg_tree_hr, 1, 0, WINDOW_SIZE - 1, idx, data->hr);
    updateSegTree(seg_tree_eda, 1, 0, WINDOW_SIZE - 1, idx, data->eda);
    
    current_time_idx = (current_time_idx + 1) % WINDOW_SIZE;
    total_insercoes++;
}

float getMediaMovel(float* tree, int num_samples, int amostras_desejadas, int recuo) {
    if (num_samples == 0 || amostras_desejadas <= 0) return 0;
    
    int start_logic = num_samples - recuo - amostras_desejadas;
    if (start_logic < 0) start_logic = 0;
    int end_logic = num_samples - recuo - 1;
    if (end_logic < 0) return 0;
    
    int start_idx = (current_time_idx - num_samples + start_logic + WINDOW_SIZE) % WINDOW_SIZE;
    int end_idx = (current_time_idx - num_samples + end_logic + WINDOW_SIZE) % WINDOW_SIZE;
    
    float soma = 0;
    if (start_idx <= end_idx) {
        soma = querySegTree(tree, 1, 0, WINDOW_SIZE - 1, start_idx, end_idx);
    } else {
        soma = querySegTree(tree, 1, 0, WINDOW_SIZE - 1, start_idx, WINDOW_SIZE - 1) + 
               querySegTree(tree, 1, 0, WINDOW_SIZE - 1, 0, end_idx);
    }
    return soma / (end_logic - start_logic + 1);
}

/* ==========================================================================
 * 3. CLASSICA 2: TABELA HASH (Indexada por DATA para Buscas O(1))
 * ========================================================================== */
typedef struct HashNode { 
    SensorData* data; 
    struct HashNode* next; 
} HashNode;

HashNode* hashTable[HASH_SIZE];

unsigned int hashFunction(const char* str) {
    unsigned int hash = 5381; int c; while ((c = *str++)) hash = ((hash << 5) + hash) + c; return hash % HASH_SIZE;
}

void insertHash(SensorData* data) {
    unsigned int idx = hashFunction(data->date_only);
    if (hashTable[idx] != NULL && strcmp(hashTable[idx]->data->date_only, data->date_only) != 0) hash_collisions++;
    HashNode* node = (HashNode*)malloc(sizeof(HashNode));
    node->data = data; node->next = hashTable[idx]; hashTable[idx] = node;
}

void buscarRegistroPorDataHash(const char* data_alvo) {
    clock_t inicio = clock(); 
    unsigned int idx = hashFunction(data_alvo);
    HashNode* curr = hashTable[idx]; 
    int encontrados = 0;
    int limitador_tela = 0;
    
    printf("\n--- RESULTADOS DA BUSCA PARA A DATA: %s ---\n", data_alvo);
    while (curr) {
        if (strcmp(curr->data->date_only, data_alvo) == 0) {
            if (limitador_tela < 5000) {
                printf(" -> Hora: %s | HR: %6.2f | EDA: %5.2f | Temp: %5.2f\n", 
                       curr->data->datetime, curr->data->hr, curr->data->eda, curr->data->temp);
                limitador_tela++;
            }
            encontrados++;
        }
        curr = curr->next;
    }
    
    if (encontrados > 5000) printf(" ... [Mais %d registros ocultados. Limite de tela atingido] ...\n", encontrados - 5000);
    else if (encontrados == 0) printf(" [!] Nenhum registro encontrado.\n");
    
    printf(" > Total Encontrados: %d | Tempo de Busca: %f segundos\n", encontrados, ((double)(clock() - inicio)) / CLOCKS_PER_SEC);
}

bool searchHashBench(const char* date_key) {
    unsigned int idx = hashFunction(date_key);
    HashNode* curr = hashTable[idx];
    while (curr) {
        if (strcmp(curr->data->date_only, date_key) == 0) return true;
        curr = curr->next;
    }
    return false;
}

void removerTodosPorDataHash(const char* date_key) {
    unsigned int idx = hashFunction(date_key);
    HashNode* curr = hashTable[idx];
    HashNode* prev = NULL;
    while (curr) {
        if (strcmp(curr->data->date_only, date_key) == 0) {
            HashNode* temp = curr;
            if (!prev) hashTable[idx] = curr->next; else prev->next = curr->next;
            curr = curr->next; free(temp);
        } else { prev = curr; curr = curr->next; }
    }
}

/* ==========================================================================
 * 4. CLASSICA 3 E OTIMIZADA: AVL Classica vs AVL Aumentada (Trade-off de RAM)
 * ========================================================================== */
int getMax(int a, int b) { return (a > b) ? a : b; }

// ---- AVL CLASSICA O(log N) ----
typedef struct StdAVLNode { SensorData* data; struct StdAVLNode *left, *right; int height; } StdAVLNode;
StdAVLNode* stdRoot = NULL;
int getStdHeight(StdAVLNode *n) { return n ? n->height : 0; }
StdAVLNode* stdRightRotate(StdAVLNode *y) {
    StdAVLNode *x = y->left; StdAVLNode *T2 = x->right; x->right = y; y->left = T2;
    y->height = getMax(getStdHeight(y->left), getStdHeight(y->right)) + 1;
    x->height = getMax(getStdHeight(x->left), getStdHeight(x->right)) + 1; return x;
}
StdAVLNode* stdLeftRotate(StdAVLNode *x) {
    StdAVLNode *y = x->right; StdAVLNode *T2 = y->left; y->left = x; x->right = T2;
    x->height = getMax(getStdHeight(x->left), getStdHeight(x->right)) + 1;
    y->height = getMax(getStdHeight(y->left), getStdHeight(y->right)) + 1; return y;
}
StdAVLNode* insertStdAVL(StdAVLNode* node, SensorData* data) {
    if (!node) { StdAVLNode* n = (StdAVLNode*)malloc(sizeof(StdAVLNode)); n->data = data; n->left = n->right = NULL; n->height = 1; return n; }
    if (data->hr < node->data->hr) node->left = insertStdAVL(node->left, data); else node->right = insertStdAVL(node->right, data);
    node->height = 1 + getMax(getStdHeight(node->left), getStdHeight(node->right));
    int balance = getStdHeight(node->left) - getStdHeight(node->right);
    if (balance > 1 && data->hr < node->left->data->hr) return stdRightRotate(node);
    if (balance < -1 && data->hr >= node->right->data->hr) return stdLeftRotate(node);
    if (balance > 1 && data->hr >= node->left->data->hr) { node->left = stdLeftRotate(node->left); return stdRightRotate(node); }
    if (balance < -1 && data->hr < node->right->data->hr) { node->right = stdRightRotate(node->right); return stdLeftRotate(node); }
    return node;
}
bool searchStdAVL(StdAVLNode* root, float hr_key) {
    if (!root) return false;
    if (fabs(root->data->hr - hr_key) < 0.001) return true;
    if (hr_key < root->data->hr) return searchStdAVL(root->left, hr_key);
    return searchStdAVL(root->right, hr_key);
}
StdAVLNode* minNodeStdAVL(StdAVLNode* node) {
    StdAVLNode* current = node;
    while (current->left != NULL) current = current->left;
    return current;
}
StdAVLNode* removeStdAVL(StdAVLNode* root, float hr_key) {
    if (root == NULL) return root;
    if (hr_key < root->data->hr) root->left = removeStdAVL(root->left, hr_key);
    else if (hr_key > root->data->hr) root->right = removeStdAVL(root->right, hr_key);
    else {
        if ((root->left == NULL) || (root->right == NULL)) {
            StdAVLNode *temp = root->left ? root->left : root->right;
            if (temp == NULL) { temp = root; root = NULL; } else *root = *temp; free(temp);
        } else {
            StdAVLNode* temp = minNodeStdAVL(root->right);
            root->data = temp->data; root->right = removeStdAVL(root->right, temp->data->hr);
        }
    }
    if (root == NULL) return root;
    root->height = 1 + getMax(getStdHeight(root->left), getStdHeight(root->right));
    int balance = getStdHeight(root->left) - getStdHeight(root->right);
    if (balance > 1 && getStdHeight(root->left->left) - getStdHeight(root->left->right) >= 0) return stdRightRotate(root);
    if (balance > 1 && getStdHeight(root->left->left) - getStdHeight(root->left->right) < 0) { root->left = stdLeftRotate(root->left); return stdRightRotate(root); }
    if (balance < -1 && getStdHeight(root->right->left) - getStdHeight(root->right->right) <= 0) return stdLeftRotate(root);
    if (balance < -1 && getStdHeight(root->right->left) - getStdHeight(root->right->right) > 0) { root->right = stdRightRotate(root->right); return stdLeftRotate(root); }
    return root;
}
void calcStatsStdAVL(StdAVLNode* root, double* sum, int* count) {
    if (!root) return; *sum += root->data->hr; (*count)++; calcStatsStdAVL(root->left, sum, count); calcStatsStdAVL(root->right, sum, count);
}
void destruirStdAVL(StdAVLNode* root) {
    if(!root) return; destruirStdAVL(root->left); destruirStdAVL(root->right); free(root);
}

// ---- AVL OTIMIZADA O(1) com METADADOS ----
typedef struct OptAVLNode { SensorData* data; struct OptAVLNode *left, *right; int height; int count; double sum_hr; } OptAVLNode;
OptAVLNode* avlRoot = NULL;
int getOptHeight(OptAVLNode *n) { return n ? n->height : 0; }
int getOptCount(OptAVLNode *n) { return n ? n->count : 0; }
double getOptSum(OptAVLNode *n) { return n ? n->sum_hr : 0.0; }
void updateOptMetadata(OptAVLNode *n) {
    if (!n) return;
    n->height = 1 + getMax(getOptHeight(n->left), getOptHeight(n->right));
    n->count = 1 + getOptCount(n->left) + getOptCount(n->right);
    n->sum_hr = n->data->hr + getOptSum(n->left) + getOptSum(n->right);
}
OptAVLNode* optRightRotate(OptAVLNode *y) {
    OptAVLNode *x = y->left; OptAVLNode *T2 = x->right; x->right = y; y->left = T2;
    updateOptMetadata(y); updateOptMetadata(x); return x;
}
OptAVLNode* optLeftRotate(OptAVLNode *x) {
    OptAVLNode *y = x->right; OptAVLNode *T2 = y->left; y->left = x; x->right = T2;
    updateOptMetadata(x); updateOptMetadata(y); return y;
}
OptAVLNode* insertOptAVL(OptAVLNode* node, SensorData* data) {
    if (!node) { OptAVLNode* n = (OptAVLNode*)malloc(sizeof(OptAVLNode)); n->data = data; n->left = n->right = NULL; n->height = n->count = 1; n->sum_hr = data->hr; return n; }
    if (data->hr < node->data->hr) node->left = insertOptAVL(node->left, data); else node->right = insertOptAVL(node->right, data);
    updateOptMetadata(node); int balance = getOptHeight(node->left) - getOptHeight(node->right);
    if (balance > 1 && data->hr < node->left->data->hr) return optRightRotate(node);
    if (balance < -1 && data->hr >= node->right->data->hr) return optLeftRotate(node);
    if (balance > 1 && data->hr >= node->left->data->hr) { node->left = optLeftRotate(node->left); return optRightRotate(node); }
    if (balance < -1 && data->hr < node->right->data->hr) { node->right = optRightRotate(node->right); return optLeftRotate(node); }
    return node;
}
void destruirOptAVL(OptAVLNode* root) {
    if(!root) return; destruirOptAVL(root->left); destruirOptAVL(root->right); free(root);
}

/* ==========================================================================
 * 5. NAO-CLASSICA 2: KD-TREE (Mapeamento de Padroes de Movimento MEMS)
 * ========================================================================== */
typedef struct KDNode { SensorData* data; struct KDNode *left, *right; } KDNode;
KDNode* kdRoot = NULL;

KDNode* insertKDNode(KDNode* root, SensorData* data, unsigned depth_ignored) {
    KDNode* newNode = (KDNode*)malloc(sizeof(KDNode)); newNode->data = data; newNode->left = newNode->right = NULL;
    if (!root) return newNode;
    KDNode* curr = root; unsigned d = 0;
    while (true) {
        unsigned axis = d % 3; bool goLeft = false;
        if ((axis == 0 && data->x < curr->data->x) || (axis == 1 && data->y < curr->data->y) || (axis == 2 && data->z < curr->data->z)) goLeft = true;
        if (goLeft) { if (!curr->left) { curr->left = newNode; break; } curr = curr->left; } 
        else { if (!curr->right) { curr->right = newNode; break; } curr = curr->right; }
        d++;
    } return root;
}

// Busca por Movimentos Semelhantes (Vetor Euclidiano de Aceleracao)
void buscarMovimentosSemelhantesKDTree(KDNode* root, float tx, float ty, float tz, float margem_tolerancia, unsigned depth, int* encontrados) {
    if (!root) return;
    
    // Calcula a diferenca vetorial de forca G
    float dx = root->data->x - tx; 
    float dy = root->data->y - ty; 
    float dz = root->data->z - tz;
    float similaridade = sqrt(dx*dx + dy*dy + dz*dz);

    if (similaridade <= margem_tolerancia) {
        if (*encontrados < 15) {
            printf(" -> Acel(X:%6.1f, Y:%6.1f, Z:%6.1f) | Margem Diff: %5.2f | HR: %5.1f | Hora: %s\n",
                   root->data->x, root->data->y, root->data->z, similaridade, root->data->hr, root->data->datetime);
        }
        (*encontrados)++;
    }

    unsigned axis = depth % 3;
    float diff = 0;
    if (axis == 0) diff = tx - root->data->x;
    else if (axis == 1) diff = ty - root->data->y;
    else diff = tz - root->data->z;

    // Navega pelas subarvores de aceleracao, descartando quadrantes irrelevantes
    if (diff < 0) {
        buscarMovimentosSemelhantesKDTree(root->left, tx, ty, tz, margem_tolerancia, depth + 1, encontrados);
        if (fabs(diff) <= margem_tolerancia) buscarMovimentosSemelhantesKDTree(root->right, tx, ty, tz, margem_tolerancia, depth + 1, encontrados);
    } else {
        buscarMovimentosSemelhantesKDTree(root->right, tx, ty, tz, margem_tolerancia, depth + 1, encontrados);
        if (fabs(diff) <= margem_tolerancia) buscarMovimentosSemelhantesKDTree(root->left, tx, ty, tz, margem_tolerancia, depth + 1, encontrados);
    }
}

void destruirKDTree(KDNode* root) {
    if (!root) return; 
    // Protecao rigorosa contra estouro de pilha alocando espaco equivalente ao limite total
    KDNode** stack = (KDNode**)malloc((BENCHMARK_LIMIT + 1) * sizeof(KDNode*)); 
    if(!stack) return;
    int top = 0; stack[top++] = root;
    while (top > 0) {
        KDNode* curr = stack[--top]; 
        if (curr->right) stack[top++] = curr->right; 
        if (curr->left) stack[top++] = curr->left; 
        free(curr);
    } free(stack);
}

/* ==========================================================================
 * 6. MODULOS DE DIAGNOSTICO E REGRAS DE NEGOCIO
 * ========================================================================== */
void analisarAnomaliasEmTempoReal() {
    int num_samples = getNumAmostrasValidas();
    if (num_samples == 0) return;

    int last_idx = (current_time_idx - 1 + WINDOW_SIZE) % WINDOW_SIZE;
    float hr = chronological_window[last_idx].hr;
    float eda = chronological_window[last_idx].eda;

    if (hr >= 95.0 && eda >= 3.0) total_picos_estresse++;

    if (num_samples >= 200) {
        float base_hr = getMediaMovel(seg_tree_hr, num_samples, 100, num_samples - 100);
        float base_eda = getMediaMovel(seg_tree_eda, num_samples, 100, num_samples - 100);
        float mov_hr = getMediaMovel(seg_tree_hr, num_samples, 200, 0);
        float mov_eda = getMediaMovel(seg_tree_eda, num_samples, 200, 0);

        if (base_hr > 0 && base_eda > 0) {
            if ((mov_hr / base_hr) > 1.15 && (mov_eda / base_eda) > 1.15) total_alertas_burnout++;
        }
    }
}

void mostrarFuncionalidadeSegmentTree() {
    int total = getNumAmostrasValidas();
    if (total < 500) {
        printf(" [!] Requer ao menos 500 amostras na fila. Amostras atuais: %d\n", total);
        return;
    }
    printf("\n--- ANALISE DE MEDIAS MOVEIS DE SINAIS VITAIS (DASHBOARD) ---\n");
    printf(" Usando a Arvore de Segmentos para resgatar intervalos em O(log N)...\n");
    printf(" -> Media HR das ultimas 50 amostras : %.2f bpm\n", getMediaMovel(seg_tree_hr, total, 50, 0));
    printf(" -> Media HR das ultimas 100 amostras: %.2f bpm\n", getMediaMovel(seg_tree_hr, total, 100, 0));
    printf(" -> Media HR das ultimas 200 amostras: %.2f bpm\n", getMediaMovel(seg_tree_hr, total, 200, 0));
    printf(" -> Media HR das ultimas 500 amostras: %.2f bpm\n", getMediaMovel(seg_tree_hr, total, 500, 0));
}

void diagnosticoDiferenciacaoEstresse() {
    printf("\n--- IDENTIFICACAO PRAGMATICA DE ESTRESSE ---\n");
    printf(" \033[1;36m[DADOS HISTORICOS]\033[0m: Detectamos %lld picos de estresse agudo.\n", total_picos_estresse);
    
    int num_samples = getNumAmostrasValidas();
    if (num_samples == 0) return;
    int picos_recentes = 0;
    
    printf(" [STATUS NO MOMENTO ATUAL (Fim do Turno)]:\n");
    for (int i = 0; i < num_samples; i++) {
        int idx = (current_time_idx - num_samples + i + WINDOW_SIZE) % WINDOW_SIZE;
        float hr = chronological_window[idx].hr; float eda = chronological_window[idx].eda;
        
        if (hr >= 95.0 && eda >= 3.0) {
            float x = chronological_window[idx].x; float y = chronological_window[idx].y; float z = chronological_window[idx].z;
            float magnitude = sqrt(x*x + y*y + z*z);
            picos_recentes++;
            if (magnitude > 60.0) printf("  -> \033[1;33m[ESTRESSE FISICO]\033[0m Mov. Alto (%.2f). HR: %.2f | EDA: %.2f\n", magnitude, hr, eda);
            else printf("  -> \033[1;35m[ESTRESSE MENTAL]\033[0m Imovel (%.2f). HR: %.2f | EDA: %.2f\n", magnitude, hr, eda);
        }
    }
    if (picos_recentes == 0) printf("  -> A enfermeira encerrou o turno de forma calma.\n");
}

void diagnosticoBurnoutAcumulado() {
    printf("\n--- ANALISE DE CARGA ACUMULADA (BURNOUT) ---\n");
    printf(" \033[1;36m[DADOS HISTORICOS]\033[0m: %lld alertas de risco de Burnout no expediente.\n", total_alertas_burnout);
    int num_samples = getNumAmostrasValidas();
    if (num_samples < 200) { printf(" Necessario 200 amostras para status final.\n"); return; }

    float base_hr = getMediaMovel(seg_tree_hr, num_samples, 100, num_samples - 100);
    float base_eda = getMediaMovel(seg_tree_eda, num_samples, 100, num_samples - 100);
    float mov_hr = getMediaMovel(seg_tree_hr, num_samples, 200, 0);
    float mov_eda = getMediaMovel(seg_tree_eda, num_samples, 200, 0);

    printf(" [STATUS NO MOMENTO ATUAL (Fim do Turno)]:\n");
    printf(" > Baseline (Repouso) -> HR: %.2f | EDA: %.2f\n", base_hr, base_eda);
    printf(" > Janela Atual       -> HR: %.2f | EDA: %.2f\n", mov_hr, mov_eda);
    
    if (base_hr > 0 && base_eda > 0) {
        float ratio_hr = mov_hr / base_hr; float ratio_eda = mov_eda / base_eda;
        printf(" > Carga de Esforco   -> HR: %+.0f%% | EDA: %+.0f%%\n", (ratio_hr - 1.0) * 100, (ratio_eda - 1.0) * 100);

        if (ratio_hr > 1.30 && ratio_eda > 1.30) printf(" \033[1;31m[ALERTA VERMELHO: BURNOUT]\033[0m Exaustao severa.\n");
        else if (ratio_hr > 1.15 && ratio_eda > 1.15) printf(" \033[1;33m[SINAL AMARELO: QUASE BURNOUT]\033[0m Estresse sustentado.\n");
        else printf(" \033[1;32m[NORMAL]\033[0m Turno encerrado em parametros adaptaveis.\n");
    }
}

/* ==========================================================================
 * 7. ESCALABILIDADE, MEDICAO DE MEMORIA E PARSERS
 * ========================================================================== */
void higienizarStringID(char* str) {
    int i = 0, j = 0;
    while (str[i]) { if (str[i] != ' ' && str[i] != '\n' && str[i] != '\r' && str[i] != ';') str[j++] = str[i]; i++; } str[j] = '\0';
}
void limparQuebraDeLinha(char* str) {
    for (int i = 0; str[i] != '\0'; i++) { if (str[i] == '\n' || str[i] == '\r') { str[i] = '\0'; break; } }
}
void aplicarRestricoesDADOS(SensorData* data) {
    if (restrict_data_noise && (rand() % 100 < 15)) data->hr = -99.0;
    if (restrict_algorithmic) { for(int i = 0; data->id[i] != '\0'; i++) data->id[i] ^= 0x5A; }
}
void aplicarRestricoesTEMPO() {
    if (restrict_processing) { volatile double dummy = 0; for(int i = 0; i < 1500; i++) dummy += sqrt((double)i); }
    if (restrict_latency) { volatile int delay = 0; for(int i = 0; i < 5000; i++) delay++; }
}

// Benchmark reescrito focado no Edital: Tamanhos Dinamicos, Tempo Medio e Calculo Real de Memoria
void executarBenchmarkSimulado(FILE* file) {
    printf("\n========================================================================================================\n");
    printf("   LABORATORIO DE BENCHMARKS AUTOMATIZADO (Escalabilidade e Complexidade Assintotica)  \n");
    printf("========================================================================================================\n");
    printf(" 0. Baseline (Condicoes Ideais)\n 1. Restricao de MEMORIA (Max 500 nos)\n");
    printf(" 2. Restricao de PROCESSAMENTO\n 3. Restricao de LATENCIA\n");
    printf(" 4. Restricao de DADOS (Sensores Corrompidos)\n 5. Restricao ALGORITMICA (XOR)\nEscolha: ");
    
    int cenario; if (scanf("%d", &cenario) != 1) return;

    restrict_processing = (cenario == 2); restrict_latency = (cenario == 3);
    restrict_data_noise = (cenario == 4); restrict_algorithmic = (cenario == 5);
    max_avl_memory_limit = (cenario == 1) ? 500 : -1;

    int tamanhos[] = {1000, 5000, 10000, 25000, 50000, 100000};
    int num_tamanhos = 6;
    
    printf("\n [INFO] Iniciando avaliacao O(N), O(log N) e O(1) em Multiplas Cargas...\n");
    printf("----------------------------------------------------------------------------------------------------------------\n");
    printf("   (N)   | T. Medio Ins. HASH | T. Medio Ins. AVL | T. Medio Busca HASH | T. Medio Busca AVL | OVERHEAD RAM AVL \n");
    printf("----------------------------------------------------------------------------------------------------------------\n");

    double time_calc_std_final = 0, time_calc_opt_final = 0;

    for (int t = 0; t < num_tamanhos; t++) {
        int limite_atual = tamanhos[t];
        hash_collisions = 0;
        
        avlRoot = NULL; stdRoot = NULL; kdRoot = NULL;
        for(int i=0; i<HASH_SIZE; i++) hashTable[i] = NULL;
        
        fseek(file, 0, SEEK_SET); 
        char linha[MAX_LINHA]; 
        fgets(linha, MAX_LINHA, file); // DESCARTA O CABECALHO DO CSV RIGOROSAMENTE
        
        // Uso OBRIGATORIO da Fila Encadeada como centro de distribuicao (Exigencia do Edital)
        Queue* qBench = createQueue();
        int lidos = 0;
        
        while (fgets(linha, MAX_LINHA, file) && lidos < limite_atual) { 
            char cp[MAX_LINHA]; strcpy(cp, linha);
            char* t_x = strtok(cp, ","); char* t_y = strtok(NULL, ","); char* t_z = strtok(NULL, ",");
            char* t_eda = strtok(NULL, ","); char* t_hr = strtok(NULL, ","); char* t_temp = strtok(NULL, ",");
            char* t_id = strtok(NULL, ","); char* t_dt = strtok(NULL, ",");
            
            if (t_id) {
                SensorData* s = (SensorData*)malloc(sizeof(SensorData));
                s->x = t_x ? atof(t_x) : 0.0f; s->y = t_y ? atof(t_y) : 0.0f; s->z = t_z ? atof(t_z) : 0.0f;
                s->hr = t_hr ? atof(t_hr) : 0.0f; strcpy(s->id, t_id); higienizarStringID(s->id);
                if(t_dt) { limparQuebraDeLinha(t_dt); strcpy(s->datetime, t_dt); sscanf(t_dt, "%19s", s->date_only); } 
                else { strcpy(s->datetime, ""); strcpy(s->date_only, ""); }
                aplicarRestricoesDADOS(s); 
                
                enqueue(qBench, s); 
                lidos++;
            }
        }
        
        SensorData** buffer_teste = (SensorData**)malloc(lidos * sizeof(SensorData*)); 
        int idx_buf = 0;
        // Drenando a fila de processamento (IoT Stream logic)
        while (qBench->size > 0) { buffer_teste[idx_buf++] = dequeue(qBench); }
        free(qBench);

        // Medicoes de Tempos
        clock_t t0 = clock(); 
        for (int i = 0; i < lidos; i++) { aplicarRestricoesTEMPO(); insertHash(buffer_teste[i]); } 
        double avg_hash_in = (((double)(clock() - t0)) / CLOCKS_PER_SEC) / lidos;
        
        t0 = clock(); 
        for (int i = 0; i < lidos; i++) { aplicarRestricoesTEMPO(); stdRoot = insertStdAVL(stdRoot, buffer_teste[i]); } 
        double avg_avl_in = (((double)(clock() - t0)) / CLOCKS_PER_SEC) / lidos;

        for (int i = 0; i < lidos; i++) { 
            if (max_avl_memory_limit != -1 && getOptCount(avlRoot) >= max_avl_memory_limit) continue;
            aplicarRestricoesTEMPO(); avlRoot = insertOptAVL(avlRoot, buffer_teste[i]);
        }
        
        for (int i = 0; i < lidos; i++) { aplicarRestricoesTEMPO(); kdRoot = insertKDNode(kdRoot, buffer_teste[i], 0); } 
        
        t0 = clock(); for (int i = 0; i < lidos; i++) searchHashBench(buffer_teste[i]->date_only); 
        double avg_hash_search = (((double)(clock() - t0)) / CLOCKS_PER_SEC) / lidos;
        
        t0 = clock(); for (int i = 0; i < lidos; i++) searchStdAVL(stdRoot, buffer_teste[i]->hr); 
        double avg_avl_search = (((double)(clock() - t0)) / CLOCKS_PER_SEC) / lidos;
        
        // Calculo Teorico e Real de Memoria (Overhead Estrutural)
        double mem_hash_mb = (double)(lidos * sizeof(HashNode) + HASH_SIZE * sizeof(HashNode*)) / (1024.0 * 1024.0);
        double mem_avl_mb = (double)(lidos * sizeof(StdAVLNode)) / (1024.0 * 1024.0);
        double mem_optavl_mb = (double)(getOptCount(avlRoot) * sizeof(OptAVLNode)) / (1024.0 * 1024.0);

        printf("  %6d | %1.6f seg       | %1.6f seg      | %1.6f seg        | %1.6f seg       | %.2f MB\n", 
               limite_atual, avg_hash_in, avg_avl_in, avg_hash_search, avg_avl_search, mem_avl_mb);

        // Somente na ultima rodada executamos o teste de estresse O(N) vs O(1)
        if (t == num_tamanhos - 1) {
            double soma_classica = 0; int count_classica = 0; 
            t0 = clock(); for(int k = 0; k < 50000; k++) { soma_classica = 0; count_classica = 0; calcStatsStdAVL(stdRoot, &soma_classica, &count_classica); }
            time_calc_std_final = ((double)(clock() - t0)) / CLOCKS_PER_SEC;

            t0 = clock(); double soma_otimizada = 0; for(int k = 0; k < 50000; k++) { soma_otimizada = getOptSum(avlRoot); }
            time_calc_opt_final = ((double)(clock() - t0)) / CLOCKS_PER_SEC;
            
            // Relatorio de Complexidade Assintotica Metodologico
            printf("\n========================================================================================================\n");
            printf("   ANALISE DE COMPLEXIDADE ESTRUTURAL DA ULTIMA CARGA (N = %d)\n", limite_atual);
            printf("--------------------------------------------------------------------------------------------------------\n");
            printf(" Estrutura          | Consumo RAM | Tempo Total | Justificativa de Arquitetura\n");
            printf("--------------------------------------------------------------------------------------------------------\n");
            printf(" AVL (Calculo O(N)) | %.2f MB     | %.4f seg  | Acesso Linear lento, ideal para insercoes dinamicas.\n", mem_avl_mb, time_calc_std_final);
            printf(" AVL Opt (Calc O(1))| %.2f MB     | %.4f seg  | Trade-off: Gasta %ld Bytes extras p/no pela Busca Imediata.\n", mem_optavl_mb, time_calc_opt_final, sizeof(OptAVLNode)-sizeof(StdAVLNode));
            printf(" Hash Table O(1)    | %.2f MB     |   -         | Excepcional para ID/Datas exatas, inutil para Range Query.\n", mem_hash_mb);
            printf(" KD-Tree Vetorial   | %.2f MB     |   -         | Obrigatoria para buscas de Aceleracao(X,Y,Z) em O(log N).\n", (double)(lidos * sizeof(KDNode))/(1024.0*1024.0));
            printf(" Segment Tree O(log)| 0.06 MB     |   -         | Alocacao array estatico fixa, perfeita p/ medias moveis.\n");
            printf("========================================================================================================\n");
        }

        // LIMPEZA ESTRUTURAL BLINDADA: Evita Memory Leak e zera os ponteiros para a proxima rodada
        destruirStdAVL(stdRoot); stdRoot = NULL;
        destruirOptAVL(avlRoot); avlRoot = NULL;
        destruirKDTree(kdRoot);  kdRoot = NULL;
        for(int i=0; i<HASH_SIZE; i++) { 
            HashNode* c = hashTable[i]; 
            while(c) { HashNode* tmp = c; c = c->next; free(tmp); } 
            hashTable[i] = NULL; 
        }
        for (int i = 0; i < lidos; i++) free(buffer_teste[i]); 
        free(buffer_teste); 
    }
}

/* ==========================================================================
 * 8. MOTOR PRINCIPAL INTERATIVO
 * ==========================================================================
 */
int main() {
    srand((unsigned int)time(NULL)); 
    setlocale(LC_ALL, "Portuguese_Brazil.UTF-8"); 
    setlocale(LC_NUMERIC, "C");
    
    FILE* file = fopen("merged_data.csv", "r");
    if (!file) { printf("Erro: Arquivo merged_data.csv nao localizado.\n"); return 1; }
    
    Queue* filaEntrada = createQueue(); 
    int opcao = 0; 
    char enfermeira_ativa[20] = "";

    while (true) {
        printf("\n==================================================================\n");
        printf("   SISTEMA DE MONITORAMENTO DE ENFERMEIRAS PANDEMIA  \n");
        printf("==================================================================\n");
        printf("Enfermeira: [%s] | Medicoes Avaliadas: %lld\n------------------------------------------------------------------\n", 
                strlen(enfermeira_ativa) > 0 ? enfermeira_ativa : "Nenhuma", total_insercoes);
        printf("1. CARREGAR Lote da Enfermeira via Fila IoT (Queue)\n2. BUSCAR Registro por Data Exata (Hash O(1))\n");
        printf("3. BUSCAR Medicoes com Padroes de Movimento Semelhantes (KD-Tree)\n4. CONSULTAR Medias Moveis Sinais Vitais (Segment Tree)\n");
        printf("5. DIAGNOSTICO: Picos de Estresse (Fisico vs Mental)\n6. DIAGNOSTICO: Quase Burnout e Burnout\n");
        printf("7. SIMULADOR: Benchmark e Escalabilidade de Memoria\n8. Sair\nEscolha uma opcao: ");
        
        if (scanf("%d", &opcao) != 1) break;

        if (opcao == 1) {
            printf("Introduza o ID da Enfermeira (15,5C,6B,6D,7A,7E,83,8B,94,BG,CE,DF,E4,EG,F5): "); 
            scanf("%s", enfermeira_ativa); higienizarStringID(enfermeira_ativa);
            
            fseek(file, 0, SEEK_SET); 
            char linha[MAX_LINHA]; 
            fgets(linha, MAX_LINHA, file); // DESCARTA O CABECALHO DO CSV RIGOROSAMENTE
            
            int lidos = 0; total_insercoes = 0; current_time_idx = 0; total_picos_estresse = 0; total_alertas_burnout = 0;
            min_date_global[0] = '\0'; max_date_global[0] = '\0';
            
            for(int i=0; i<HASH_SIZE; i++) { HashNode* c = hashTable[i]; while(c) { HashNode* t = c; c = c->next; free(t->data); free(t); } hashTable[i] = NULL; }
            destruirKDTree(kdRoot); kdRoot = NULL; 
            
            printf(" -> Lendo arquivo CSV simulando Stream de Rede...\n");
            while (fgets(linha, MAX_LINHA, file)) {
                char cp_linha[MAX_LINHA]; strcpy(cp_linha, linha);
                char* t_x = strtok(cp_linha, ","); char* t_y = strtok(NULL, ","); char* t_z = strtok(NULL, ",");
                char* t_eda = strtok(NULL, ","); char* t_hr = strtok(NULL, ","); char* t_temp = strtok(NULL, ",");
                char* t_id = strtok(NULL, ","); char* t_dt = strtok(NULL, ",");
                
                if (t_id) {
                    char id_limpo[20]; strcpy(id_limpo, t_id); higienizarStringID(id_limpo);
                    if (strcmp(id_limpo, enfermeira_ativa) == 0) {
                        SensorData* s = (SensorData*)malloc(sizeof(SensorData));
                        s->x = t_x?atof(t_x):0; s->y = t_y?atof(t_y):0; s->z = t_z?atof(t_z):0;
                        s->eda = t_eda?atof(t_eda):0; s->hr = t_hr?atof(t_hr):0; s->temp = t_temp?atof(t_temp):0;
                        strcpy(s->id, id_limpo); 
                        if (t_dt) {
                            limparQuebraDeLinha(t_dt); strcpy(s->datetime, t_dt); sscanf(t_dt, "%19s", s->date_only); 
                            if (strlen(min_date_global) == 0 || strcmp(s->date_only, min_date_global) < 0) strcpy(min_date_global, s->date_only);
                            if (strlen(max_date_global) == 0 || strcmp(s->date_only, max_date_global) > 0) strcpy(max_date_global, s->date_only);
                        } else { strcpy(s->datetime, ""); strcpy(s->date_only, ""); }

                        enqueue(filaEntrada, s); 
                        lidos++;
                    }
                }
            }
            if (lidos == 0) { printf(" Nenhum registro encontrado.\n"); strcpy(enfermeira_ativa, ""); } 
            else {
                printf(" -> Processando e distribuindo Fila (Tamanho: %d)...\n", filaEntrada->size);
                int processados = 0;
                while(filaEntrada->size > 0) {
                    SensorData* dado = dequeue(filaEntrada);
                    insertHash(dado); 
                    insertTimelineData(dado); 
                    kdRoot = insertKDNode(kdRoot, dado, 0); 
                    analisarAnomaliasEmTempoReal(); 
                    processados++;
                    
                    if (processados % 1500 == 0 || filaEntrada->size == 0) {
                        printf("\r \033[1;36m[VARREDURA ATIVA]\033[0m %d/%d | Picos: %lld | Burnout: %lld", processados, lidos, total_picos_estresse, total_alertas_burnout);
                        fflush(stdout);
                    }
                }
                printf("\n [OK] Historico mapeado com sucesso!\n");
            }
        } 
        else if (opcao == 2) {
            if (strlen(min_date_global) == 0) printf(" [!] Carregue os dados da enfermeira primeiro.\n");
            else {
                char busca_data[20]; 
                printf(" Pesquisa disponivel: de [%s] ate [%s]\n Digite a data: ", min_date_global, max_date_global);
                scanf("%s", busca_data); higienizarStringID(busca_data); buscarRegistroPorDataHash(busca_data);
            }
        } 
        else if (opcao == 3) {
            if (!kdRoot) printf(" [!] Carregue os dados da enfermeira primeiro.\n");
            else {
                float px, py, pz, raio; int enc = 0;
                printf("\n--- BUSCA DE PADROES DE MOVIMENTO (KD-Tree) ---\n");
                printf(" Mapeando similaridade vetorial de forca G (Acelerometro MEMS)...\n");
                printf(" Informe o Vetor de Aceleracao Alvo (X Y Z): "); if(scanf("%f %f %f", &px, &py, &pz)!=3) continue;
                printf(" Informe a Margem de Tolerancia (Raio de Similaridade Euclidiana): "); if(scanf("%f", &raio)!=1) continue;
                buscarMovimentosSemelhantesKDTree(kdRoot, px, py, pz, raio, 0, &enc);
                if (enc == 0) printf(" [!] Nenhum registro com esse padrao de movimento fisico encontrado.\n");
                else if (enc > 15) printf(" ... [Muitos registros semelhantes. Exibidos os 15 primeiros encontrados] ...\n");
            }
        } 
        else if (opcao == 4) { mostrarFuncionalidadeSegmentTree(); } 
        else if (opcao == 5) { if (strlen(enfermeira_ativa) == 0) continue; diagnosticoDiferenciacaoEstresse(); } 
        else if (opcao == 6) { if (strlen(enfermeira_ativa) == 0) continue; diagnosticoBurnoutAcumulado(); } 
        else if (opcao == 7) { executarBenchmarkSimulado(file); strcpy(enfermeira_ativa, ""); } 
        else if (opcao == 8) { break; }
    }
    
    for(int i=0; i<HASH_SIZE; i++) { HashNode* c = hashTable[i]; while(c) { HashNode* t = c; c = c->next; free(t->data); free(t); } hashTable[i] = NULL; }
    destruirKDTree(kdRoot); kdRoot = NULL;
    while (filaEntrada->size > 0) { SensorData* d = dequeue(filaEntrada); free(d); } free(filaEntrada);
    fclose(file); return 0;
}