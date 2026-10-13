// Jantar dos Filósofos com Monitor
//
// Implementa o Algorithm 7.5 do Ben-Ari: um monitor centralizado
// (ForkMonitor) gerencia a contagem de garfos disponíveis por filósofo.
// Cada filósofo tem uma variável de condição OKtoEat[i].
//
// fork[i] conta quantos garfos adjacentes estão livres (0, 1 ou 2).
// O filósofo só come quando fork[i] == 2.
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const N = 5

type ForkMonitor struct {
	mu      sync.Mutex
	fork    [N]int
	OKtoEat [N]*sync.Cond
}

func NewForkMonitor() *ForkMonitor {
	m := &ForkMonitor{}
	for i := range m.fork {
		m.fork[i] = 2 // ambos os garfos adjacentes livres
	}
	for i := range m.OKtoEat {
		m.OKtoEat[i] = sync.NewCond(&m.mu)
	}
	return m
}

// TakeForks: filósofo i espera até ter 2 garfos disponíveis,
// depois decrementa os vizinhos.
func (m *ForkMonitor) TakeForks(i int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for m.fork[i] != 2 {
		m.OKtoEat[i].Wait()
	}

	m.fork[(i+1)%N]--
	m.fork[(i+N-1)%N]--
}

// ReleaseForks: filósofo i devolve garfos,
// incrementa vizinhos e sinaliza quem atingiu 2.
func (m *ForkMonitor) ReleaseForks(i int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.fork[(i+1)%N]++
	m.fork[(i+N-1)%N]++

	if m.fork[(i+1)%N] == 2 {
		m.OKtoEat[(i+1)%N].Signal()
	}
	if m.fork[(i+N-1)%N] == 2 {
		m.OKtoEat[(i+N-1)%N].Signal()
	}
}

func filosofo(id int, mon *ForkMonitor, wg *sync.WaitGroup) {
	defer wg.Done()

	for rodada := 0; rodada < 3; rodada++ {
		fmt.Printf("  Filósofo %d está pensando...\n", id)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

		mon.TakeForks(id)
		fmt.Printf(">>> Filósofo %d está comendo (rodada %d) <<<\n", id, rodada+1)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)
		mon.ReleaseForks(id)
	}
	fmt.Printf("  Filósofo %d terminou.\n", id)
}

func main() {
	fmt.Println("=== Jantar dos Filósofos com Monitor (Algorithm 7.5, Ben-Ari) ===")
	fmt.Println()

	mon := NewForkMonitor()
	var wg sync.WaitGroup

	for i := 0; i < N; i++ {
		wg.Add(1)
		go filosofo(i, mon, &wg)
	}

	wg.Wait()
	fmt.Println("\nTodos os filósofos terminaram. Sem deadlock!")
}
