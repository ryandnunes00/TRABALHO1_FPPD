// Produtor-Consumidor com Canais
//
// O canal bufferizado de Go é a solução mais idiomática para o
// produtor-consumidor: o buffer do canal serve simultaneamente
// como armazenamento e mecanismo de sincronização.
//
// - Envio bloqueia quando o canal está cheio (buffer lotado)
// - Recepção bloqueia quando o canal está vazio (sem itens)
//
// Baseado no Algorithm 8.1 do Ben-Ari.
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func produtor(id int, ch chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 10; i++ {
		item := id*100 + i
		ch <- item // bloqueia se o buffer estiver cheio
		fmt.Printf("[Produtor %d] Enviou %d\n", id, item)
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
	}
}

func consumidor(id int, ch <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 10; i++ {
		item := <-ch // bloqueia se o buffer estiver vazio
		fmt.Printf("  [Consumidor %d] Recebeu %d\n", id, item)
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

func main() {
	const bufferSize = 5

	fmt.Println("=== Produtor-Consumidor com Canais ===")
	fmt.Printf("Canal bufferizado de tamanho %d\n", bufferSize)
	fmt.Println("2 produtores (10 itens cada) e 2 consumidores (10 itens cada)")
	fmt.Println()

	ch := make(chan int, bufferSize)

	var wg sync.WaitGroup
	wg.Add(4)

	go produtor(1, ch, &wg)
	go produtor(2, ch, &wg)
	go consumidor(1, ch, &wg)
	go consumidor(2, ch, &wg)

	wg.Wait()
	fmt.Println("\nTodos os produtores e consumidores finalizaram.")
}
