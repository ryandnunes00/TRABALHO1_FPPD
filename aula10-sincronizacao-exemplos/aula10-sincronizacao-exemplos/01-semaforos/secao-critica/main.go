// Seção Crítica com Semáforos
//
// Demonstra exclusão mútua usando golang.org/x/sync/semaphore.
// Baseado no Algorithm 6.1 do Ben-Ari.
//
// O semáforo ponderado (Weighted) com peso 1 funciona como
// semáforo binário: Acquire(1) = Wait/P, Release(1) = Signal/V.
//
// Sem o semáforo, o contador final seria inconsistente devido a data races.
// Com o semáforo, apenas uma goroutine por vez acessa a seção crítica.
package main

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/semaphore"
)

var contador int

func incrementar(sem *semaphore.Weighted, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	for i := 0; i < 100_000; i++ {
		sem.Acquire(ctx, 1) // Wait (P) — adquire 1 permissão
		// --- seção crítica ---
		contador++
		// --- fim da seção crítica ---
		sem.Release(1) // Signal (V) — libera 1 permissão
	}
}

func main() {
	// Semáforo binário: capacidade máxima = 1
	sem := semaphore.NewWeighted(1)
	var wg sync.WaitGroup

	n := 5
	fmt.Printf("Iniciando %d goroutines, cada uma incrementando 100.000 vezes...\n", n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go incrementar(sem, &wg)
	}

	wg.Wait()

	esperado := n * 100_000
	fmt.Printf("Valor esperado: %d\n", esperado)
	fmt.Printf("Valor obtido:   %d\n", contador)

	if contador == esperado {
		fmt.Println("OK — exclusão mútua garantida pelo semáforo.")
	} else {
		fmt.Println("ERRO — ocorreu data race (não deveria acontecer com semáforo).")
	}
}
