// Produtor-Consumidor com Monitores
//
// Demonstra o padrão monitor com sync.Mutex + sync.Cond em Go.
// Duas variáveis de condição (notEmpty, notFull) coordenam
// produtores e consumidores sobre um buffer de capacidade limitada.
//
// Baseado no Algorithm 7.3 do Ben-Ari.
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// BufferMonitor encapsula o buffer e as variáveis de condição.
// O mutex garante exclusão mútua; as cond vars coordenam espera.
type BufferMonitor struct {
	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	items    []int
	limit    int
}

func NewBufferMonitor(limit int) *BufferMonitor {
	b := &BufferMonitor{
		limit: limit,
		items: make([]int, 0, limit),
	}
	b.notEmpty = sync.NewCond(&b.mu)
	b.notFull = sync.NewCond(&b.mu)
	return b
}

func (b *BufferMonitor) Put(item int) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Espera enquanto o buffer estiver cheio.
	// Usa for (não if) por causa de signal-and-continue e spurious wakeups.
	for len(b.items) == b.limit {
		b.notFull.Wait()
	}

	b.items = append(b.items, item)
	b.notEmpty.Signal()
}

func (b *BufferMonitor) Take() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Espera enquanto o buffer estiver vazio.
	for len(b.items) == 0 {
		b.notEmpty.Wait()
	}

	item := b.items[0]
	b.items = b.items[1:]
	b.notFull.Signal()
	return item
}

func produtor(id int, buf *BufferMonitor, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 10; i++ {
		item := id*100 + i
		buf.Put(item)
		fmt.Printf("[Produtor %d] Inseriu %d\n", id, item)
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
	}
}

func consumidor(id int, buf *BufferMonitor, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 10; i++ {
		item := buf.Take()
		fmt.Printf("  [Consumidor %d] Retirou %d\n", id, item)
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

func main() {
	buf := NewBufferMonitor(5)

	fmt.Println("=== Produtor-Consumidor com Monitor (sync.Mutex + sync.Cond) ===")
	fmt.Println("Buffer de tamanho 5")
	fmt.Println("2 produtores (10 itens cada) e 2 consumidores (10 itens cada)")
	fmt.Println()

	var wg sync.WaitGroup
	wg.Add(4)

	go produtor(1, buf, &wg)
	go produtor(2, buf, &wg)
	go consumidor(1, buf, &wg)
	go consumidor(2, buf, &wg)

	wg.Wait()
	fmt.Println("\nTodos os produtores e consumidores finalizaram.")
}
