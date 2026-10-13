# Aula 09 — Exemplos de Código

Exemplos em Go da apresentação **Canais e Memória Compartilhada** (Unidade 3).

## Como executar

```bash
cd aula09-canais-sincronizacao/exemplos
go run ./01-canal-sincrono/
```

Para o exemplo de data race, use a flag `-race`:

```bash
go run -race ./06-data-race/
```

## Exemplos

### Parte 4 — Channels em Go

| # | Pasta | Conceito |
|---|-------|----------|
| 1 | `01-canal-sincrono` | Canal sem buffer — rendez-vous entre goroutines |
| 2 | `02-canal-bufferizado` | Canal com buffer — envio assíncrono e leitura FIFO |
| 3 | `03-canal-direcional` | Channels direcionais (`chan<-` e `<-chan`) |
| 4 | `04-close-range` | Fechamento com `close()` e iteração com `range` |
| 5 | `05-produtor-consumidor` | Múltiplos produtores/consumidores com `WaitGroup` |

### Parte 5 — Memória Compartilhada

| # | Pasta | Conceito |
|---|-------|----------|
| 6 | `06-data-race` | Condição de corrida (executar com `-race`) |
| 7 | `07-mutex` | Correção com `sync.Mutex` |
| 8 | `08-rwmutex` | Leitores/escritores com `sync.RWMutex` |

### Parte 6 — Select

| # | Pasta | Conceito |
|---|-------|----------|
| 9 | `09-select` | `select` básico, `default` e timeout com `time.After` |
