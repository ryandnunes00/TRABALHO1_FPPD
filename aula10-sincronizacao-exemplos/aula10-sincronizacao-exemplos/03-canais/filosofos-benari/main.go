// Jantar dos Filósofos com Canais — Algorithm 8.5 (Ben-Ari)
//
// Tradução fiel do Algorithm 8.5: cada garfo é um PROCESSO (goroutine)
// que gerencia seu próprio CANAL SÍNCRONO (sem buffer).
//
// O processo-garfo alterna entre:
//   1. Enviar no canal (disponibilizar o garfo) — bloqueia até um filósofo receber
//   2. Receber do canal (aguardar devolução)    — bloqueia até um filósofo enviar
//
// O filósofo:
//   1. Pensa
//   2. Recebe de forks[i]   (pega garfo esquerdo — rendezvous com o processo-garfo)
//   3. Recebe de forks[i+1] (pega garfo direito)
//   4. Come
//   5. Envia em forks[i]    (devolve garfo esquerdo)
//   6. Envia em forks[i+1]  (devolve garfo direito)
//
// Os canais são SÍNCRONOS: a comunicação serve simultaneamente como
// transferência de posse e ponto de sincronização (rendezvous).
//
// Para evitar deadlock: filósofo 0 pega na ordem inversa (quebra de simetria).
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const N = 5

// garfo é o processo do Ben-Ari: disponibiliza e aguarda devolução em loop.
// Corresponde a:
//
//	fork i:
//	  loop forever
//	    forks[i] ⇐ true    // envia (garfo disponível)
//	    forks[i] ⇒ dummy   // recebe (garfo devolvido)
func garfo(ch chan bool) {
	for {
		ch <- true // disponibiliza o garfo (bloqueia até um filósofo receber)
		<-ch       // aguarda devolução     (bloqueia até um filósofo enviar)
	}
}

func filosofo(id int, forks [N]chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	left := forks[id]
	right := forks[(id+1)%N]

	for rodada := 0; rodada < 3; rodada++ {
		// Pensar
		fmt.Printf("  Filósofo %d está pensando...\n", id)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

		// Pegar garfos (quebra de simetria no filósofo 0)
		if id == 0 {
			<-right // recebe do canal (rendezvous com processo-garfo)
			<-left
		} else {
			<-left
			<-right
		}

		// Comer
		fmt.Printf(">>> Filósofo %d está comendo (rodada %d) <<<\n", id, rodada+1)
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond)

		// Devolver garfos
		left <- true
		right <- true

	}
	fmt.Printf("  Filósofo %d terminou.\n", id)
}

func main() {
	fmt.Println("=== Jantar dos Filósofos — Algorithm 8.5 (Ben-Ari) ===")
	fmt.Println("Cada garfo é um processo (goroutine) com canal síncrono")
	fmt.Println("Estratégia: quebra de simetria (filósofo 0 inverte a ordem)")
	fmt.Println()

	var forks [N]chan bool
	for i := range forks {
		forks[i] = make(chan bool) // canal SEM buffer (síncrono)
		go garfo(forks[i])
	}

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go filosofo(i, forks, &wg)
	}

	wg.Wait()
	fmt.Println("\nTodos os filósofos terminaram. Sem deadlock!")
}
