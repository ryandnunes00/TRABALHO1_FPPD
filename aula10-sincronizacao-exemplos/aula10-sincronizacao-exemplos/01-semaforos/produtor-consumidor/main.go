// Produtor-Consumidor com Semáforos
//
// Demonstra o problema produtor-consumidor usando três semáforos
// da biblioteca golang.org/x/sync/semaphore:
//   - notFull  (contador, init N): conta vagas livres no buffer
//   - notEmpty (contador, init 0): conta itens disponíveis no buffer
//   - mutex    (binário, init 1):  exclusão mútua sobre o buffer
//
// Acquire(1) = Wait/P, Release(1) = Signal/V.
// Baseado no Algorithm 6.8 do Ben-Ari.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

const bufferSize = 5

var (
	buffer   [bufferSize]int
	inIdx    int
	outIdx   int
	mutex    = semaphore.NewWeighted(1) // exclusão mútua
)

// notFull e notEmpty precisam de inicialização com Acquire,
// pois NewWeighted(n) começa com n permissões disponíveis.
// notFull: capacidade N, inicia com N disponíveis (todas as vagas livres).
// notEmpty: capacidade N, inicia com 0 disponíveis (nenhum item no buffer).
var (
	notFull  = semaphore.NewWeighted(bufferSize)
	notEmpty *semaphore.Weighted
)

func init() {
	notEmpty = semaphore.NewWeighted(bufferSize)
	// Drena todas as permissões para simular init(0)
	notEmpty.Acquire(context.Background(), bufferSize)
}

func produtor(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		item := id*100 + i

		notFull.Acquire(ctx, 1) // aguarda vaga
		mutex.Acquire(ctx, 1)   // exclusão mútua

		buffer[inIdx] = item
		fmt.Printf("[Produtor %d] Inseriu %d na posição %d\n", id, item, inIdx)
		inIdx = (inIdx + 1) % bufferSize

		mutex.Release(1)
		notEmpty.Release(1) // sinaliza item disponível

		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
	}
}

func consumidor(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		notEmpty.Acquire(ctx, 1) // aguarda item
		mutex.Acquire(ctx, 1)    // exclusão mútua

		item := buffer[outIdx]
		fmt.Printf("  [Consumidor %d] Retirou %d da posição %d\n", id, item, outIdx)
		outIdx = (outIdx + 1) % bufferSize

		mutex.Release(1)
		notFull.Release(1) // sinaliza vaga liberada

		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

func main() {
	fmt.Printf("Buffer de tamanho %d\n", bufferSize)
	fmt.Println("2 produtores (10 itens cada) e 2 consumidores (10 itens cada)")
	fmt.Println()

	var wg sync.WaitGroup
	wg.Add(4)

	go produtor(1, &wg)
	go produtor(2, &wg)
	go consumidor(1, &wg)
	go consumidor(2, &wg)

	wg.Wait()
	fmt.Println("\nTodos os produtores e consumidores finalizaram.")
}
