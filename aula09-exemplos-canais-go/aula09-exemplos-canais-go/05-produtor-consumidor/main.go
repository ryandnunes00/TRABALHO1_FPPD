// Exemplo prático — múltiplos produtores e consumidores.
//
// Combina chan<-, <-chan, close, range e WaitGroup.
// 3 produtores enviam itens; 2 consumidores processam.
// WaitGroup garante que o canal só é fechado após todos
// os produtores terminarem.
package main

import (
	"fmt"
	"sync"
)

func produtor(id int, ch chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 3; i++ {
		item := fmt.Sprintf("P%d-item%d", id, i)
		ch <- item
		fmt.Printf("[P%d] Produziu: %s\n", id, item)
	}
}

func consumidor(id int, ch <-chan string) {
	for item := range ch {
		fmt.Printf("[C%d] Consumiu: %s\n", id, item)
	}
}

func main() {
	ch := make(chan string, 5)
	var wg sync.WaitGroup

	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go produtor(i, ch, &wg)
	}
	for i := 1; i <= 2; i++ {
		go consumidor(i, ch)
	}

	wg.Wait()
	close(ch)
}
