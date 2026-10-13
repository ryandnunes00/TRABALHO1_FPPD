// Correção da condição de corrida com sync.Mutex.
//
// O mutex garante que apenas uma goroutine executa
// contador++ por vez. O resultado é sempre 2000.
package main

import (
	"fmt"
	"sync"
)

var (
	contador int
	mu       sync.Mutex
)

func incrementar(wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 1000; i++ {
		mu.Lock()
		contador++
		mu.Unlock()
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)
	go incrementar(&wg)
	go incrementar(&wg)
	wg.Wait()
	fmt.Println(contador) // sempre 2000
}
