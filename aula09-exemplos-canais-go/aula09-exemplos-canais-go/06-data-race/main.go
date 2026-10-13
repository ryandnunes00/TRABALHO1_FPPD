// Condição de corrida (data race).
//
// Duas goroutines incrementam o mesmo contador sem proteção.
// O resultado é menor que 2000 na maioria das execuções.
//
// Execute com: go run -race main.go
// para ver o aviso do detector de corrida.
package main

import (
	"fmt"
	"sync"
)

var contador int

func incrementar(wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 1000; i++ {
		contador++ // NÃO é atômico: ler → incrementar → escrever
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(2)
	go incrementar(&wg)
	go incrementar(&wg)
	wg.Wait()
	fmt.Println(contador) // ≠ 2000 na maioria das execuções!
}
