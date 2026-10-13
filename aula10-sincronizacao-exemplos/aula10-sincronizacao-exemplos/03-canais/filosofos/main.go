// Jantar dos Filósofos com Canais
//
// Cada garfo é um canal com buffer 1 contendo um token.
// Pegar o garfo = receber do canal; devolver = enviar de volta.
//
// Para evitar deadlock, usamos a estratégia de quebra de simetria:
// o filósofo 0 pega os garfos na ordem inversa.
//
// Baseado no Algorithm 8.5 do Ben-Ari, com correção de deadlock.
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const numFilosofos = 5

func filosofo(id int, forks []chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	left := forks[id]
	right := forks[(id+1)%numFilosofos]

	for rodada := 0; rodada < 3; rodada++ {
		// Pensar
		fmt.Printf("  Filósofo %d está pensando...\n", id)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

		// Pegar garfos (quebra de simetria no filósofo 0)
		if id == 0 {
			<-right // pega direito primeiro
			<-left
		} else {
			<-left // pega esquerdo primeiro
			<-right
		}

		// Comer
		fmt.Printf(">>> Filósofo %d está comendo (rodada %d) <<<\n", id, rodada+1)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

		// Devolver garfos
		left <- struct{}{}
		right <- struct{}{}
	}
	fmt.Printf("  Filósofo %d terminou.\n", id)
}

func main() {
	fmt.Println("=== Jantar dos Filósofos com Canais ===")
	fmt.Println("Estratégia: quebra de simetria (filósofo 0 inverte a ordem)")
	fmt.Println()

	forks := make([]chan struct{}, numFilosofos)
	for i := range forks {
		forks[i] = make(chan struct{}, 1)
		forks[i] <- struct{}{} // token = garfo disponível
	}

	var wg sync.WaitGroup
	for i := 0; i < numFilosofos; i++ {
		wg.Add(1)
		go filosofo(i, forks, &wg)
	}

	wg.Wait()
	fmt.Println("\nTodos os filósofos terminaram. Sem deadlock!")
}
