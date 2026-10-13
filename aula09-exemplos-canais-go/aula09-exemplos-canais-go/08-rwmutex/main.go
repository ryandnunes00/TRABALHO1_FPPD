// sync.RWMutex — múltiplos leitores simultâneos, escritor exclusivo.
//
// Demonstra o uso de RLock/RUnlock para leituras concorrentes
// e Lock/Unlock para escritas exclusivas em um map compartilhado.
package main

import (
	"fmt"
	"sync"
	"time"
)

var (
	dados = make(map[string]string)
	mu    sync.RWMutex
)

func ler(chave string) string {
	mu.RLock() // múltiplos leitores simultâneos
	v := dados[chave]
	mu.RUnlock()
	return v
}

func escrever(chave, valor string) {
	mu.Lock() // escritor exclusivo
	dados[chave] = valor
	mu.Unlock()
}

func main() {
	var wg sync.WaitGroup

	// Escritores: cada um insere um par chave-valor
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			chave := fmt.Sprintf("chave%d", id)
			valor := fmt.Sprintf("valor%d", id)
			escrever(chave, valor)
			fmt.Printf("[W%d] Escreveu %s=%s\n", id, chave, valor)
		}(i)
	}

	// Pequena espera para que os escritores preencham o map
	time.Sleep(50 * time.Millisecond)

	// Leitores: cada um lê todas as chaves
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				chave := fmt.Sprintf("chave%d", j)
				v := ler(chave)
				fmt.Printf("[R%d] Leu %s=%s\n", id, chave, v)
			}
		}(i)
	}

	wg.Wait()
}
