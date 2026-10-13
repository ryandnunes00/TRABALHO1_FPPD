// Jantar dos Filósofos com Semáforos
//
// Cada garfo é um semáforo binário (golang.org/x/sync/semaphore).
// Para evitar deadlock, usamos a estratégia de quebra de simetria:
// o filósofo 0 pega os garfos na ordem inversa (direito antes do
// esquerdo), quebrando a espera circular (condição 4 de Coffman).
//
// Acquire(1) = Wait/P, Release(1) = Signal/V.
// Baseado no Algorithm 6.10 do Ben-Ari, com correção de deadlock.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

const numFilosofos = 5

var forks [numFilosofos]*semaphore.Weighted

func pensar(id int) {
	fmt.Printf("  Filósofo %d está pensando...\n", id)
	time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
}

func comer(id int) {
	fmt.Printf(">>> Filósofo %d está comendo <<<\n", id)
	time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
}

func filosofo(id int, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()

	left := id
	right := (id + 1) % numFilosofos

	for rodada := 0; rodada < 3; rodada++ {
		pensar(id)

		// Quebra de simetria: filósofo 0 pega na ordem inversa
		if id == 0 {
			forks[right].Acquire(ctx, 1)
			forks[left].Acquire(ctx, 1)
		} else {
			forks[left].Acquire(ctx, 1)
			forks[right].Acquire(ctx, 1)
		}

		comer(id)

		forks[left].Release(1)
		forks[right].Release(1)
	}
	fmt.Printf("  Filósofo %d terminou.\n", id)
}

func main() {
	fmt.Println("=== Jantar dos Filósofos com Semáforos ===")
	fmt.Println("Estratégia: quebra de simetria (filósofo 0 inverte a ordem)")
	fmt.Println()

	for i := range forks {
		forks[i] = semaphore.NewWeighted(1)
	}

	var wg sync.WaitGroup
	for i := 0; i < numFilosofos; i++ {
		wg.Add(1)
		go filosofo(i, &wg)
	}

	wg.Wait()
	fmt.Println("\nTodos os filósofos terminaram. Sem deadlock!")
}
