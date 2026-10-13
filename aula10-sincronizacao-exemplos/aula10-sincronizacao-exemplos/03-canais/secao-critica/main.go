// Seção Crítica com Canais
//
// Demonstra exclusão mútua usando um canal bufferizado de tamanho 1
// como semáforo binário. A presença de um token no canal representa
// a permissão de entrar na seção crítica.
//
// Variante idiomática em Go: chan struct{} (tipo de tamanho zero).
//
// Baseado no Algorithm 8.4 do Ben-Ari.
package main

import (
	"fmt"
	"sync"
)

var contador int

func incrementar(mutex chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 100_000; i++ {
		<-mutex // wait — adquire o token (exclusão)
		// --- seção crítica ---
		contador++
		// --- fim da seção crítica ---
		mutex <- struct{}{} // signal — devolve o token
	}
}

func main() {
	// Canal com buffer 1: funciona como semáforo binário.
	// struct{} ocupa 0 bytes — sinaliza que o valor é irrelevante.
	mutex := make(chan struct{}, 1)
	mutex <- struct{}{} // token inicial

	var wg sync.WaitGroup
	n := 5
	fmt.Printf("Iniciando %d goroutines, cada uma incrementando 100.000 vezes...\n", n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go incrementar(mutex, &wg)
	}

	wg.Wait()

	esperado := n * 100_000
	fmt.Printf("Valor esperado: %d\n", esperado)
	fmt.Printf("Valor obtido:   %d\n", contador)

	if contador == esperado {
		fmt.Println("OK — exclusão mútua garantida pelo canal como semáforo.")
	} else {
		fmt.Println("ERRO — data race detectada.")
	}
}
