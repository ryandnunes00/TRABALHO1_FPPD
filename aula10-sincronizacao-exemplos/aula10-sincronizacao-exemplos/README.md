# Aula 10 — Mecanismos de Sincronização: Exemplos em Go

Exemplos completos e executáveis dos três mecanismos de sincronização estudados na Aula 10, aplicados aos três problemas clássicos de concorrência.

## Estrutura

```
aula10-sincronizacao/
├── 01-semaforos/                 # Semáforos (golang.org/x/sync/semaphore)
│   ├── secao-critica/main.go    # Algorithm 6.1 (Ben-Ari)
│   ├── produtor-consumidor/main.go  # Algorithm 6.8
│   └── filosofos/main.go        # Algorithm 6.10 (com quebra de simetria)
│
├── 02-monitores/                 # Monitores (sync.Mutex + sync.Cond)
│   ├── secao-critica/main.go    # Algorithm 7.1
│   ├── produtor-consumidor/main.go  # Algorithm 7.3
│   └── filosofos/main.go        # Algorithm 7.5
│
├── 03-canais/                    # Canais (chan T)
│   ├── secao-critica/main.go    # Algorithm 8.4
│   ├── produtor-consumidor/main.go  # Algorithm 8.1
│   ├── filosofos/main.go        # Variante com canal bufferizado + token
│   └── filosofos-benari/main.go # Algorithm 8.5 — garfo como processo + canal síncrono
│
├── go.mod
└── README.md
```

## Como executar

Cada exemplo é um programa independente. Para executar, navegue até o diretório do exemplo e use `go run`:

```bash
# Exemplo: semáforo — seção crítica
cd 01-semaforos/secao-critica
go run main.go

# Exemplo: monitor — produtor-consumidor
cd 02-monitores/produtor-consumidor
go run main.go

# Exemplo: canais — filósofos
cd 03-canais/filosofos
go run main.go
```

## Resumo dos mecanismos

### 1. Semáforos (Dijkstra, 1965)

Go não possui um tipo `semaphore` na biblioteca padrão. Os exemplos usam o pacote oficial [`golang.org/x/sync/semaphore`](https://pkg.go.dev/golang.org/x/sync/semaphore), que implementa um **semáforo ponderado** (`Weighted`):

- **Wait (P):** `sem.Acquire(ctx, 1)` — adquire 1 permissão (bloqueia se indisponível)
- **Signal (V):** `sem.Release(1)` — libera 1 permissão
- `NewWeighted(n)` cria um semáforo com capacidade máxima `n`, inicialmente com `n` permissões disponíveis

### 2. Monitores (Hoare, 1974)

O padrão de monitor em Go combina `sync.Mutex` (exclusão mútua) com `sync.Cond` (variáveis de condição):

- O mutex garante que apenas uma goroutine execute o código protegido por vez
- `cond.Wait()` libera o lock, bloqueia e readquire ao acordar
- Sempre usar `for` (não `if`) ao testar condição antes de `Wait()` (signal-and-continue)

### 3. Canais

Canais são o mecanismo idiomático de Go — sincronização por comunicação:

- **Canal síncrono** (`make(chan T)`): rendezvous entre emissor e receptor
- **Canal bufferizado** (`make(chan T, N)`): desacopla emissor e receptor até N posições

## Problemas clássicos

| Problema | Descrição |
|----------|-----------|
| **Seção crítica** | Garantir exclusão mútua no acesso a um recurso compartilhado |
| **Produtor-consumidor** | Coordenar threads que produzem e consomem itens via buffer limitado |
| **Jantar dos filósofos** | Alocação de recursos circulares sem deadlock nem starvation |

## Referência

Ben-Ari, M. *Principles of Concurrent and Distributed Programming*. 2nd ed. Addison-Wesley, 2006.
