// Select — multiplexação de canais com default e timeout.
//
// Demonstra três variantes:
//   1. select básico com múltiplos canais
//   2. select com default (não-bloqueante)
//   3. select com timeout usando time.After
package main

import (
	"fmt"
	"time"
)

// selectBasico espera por um de dois canais e reage ao primeiro pronto.
func selectBasico() {
	ch1 := make(chan string, 1)
	ch2 := make(chan string, 1)

	go func() {
		time.Sleep(100 * time.Millisecond)
		ch1 <- "canal 1"
	}()
	go func() {
		time.Sleep(200 * time.Millisecond)
		ch2 <- "canal 2"
	}()

	// Duas rodadas: uma para cada canal
	for i := 0; i < 2; i++ {
		select {
		case msg := <-ch1:
			fmt.Println("De ch1:", msg)
		case msg := <-ch2:
			fmt.Println("De ch2:", msg)
		}
	}
}

// selectDefault demonstra o select não-bloqueante.
func selectDefault() {
	ch := make(chan string, 1)

	// Canal vazio: o default executa imediatamente
	select {
	case msg := <-ch:
		fmt.Println("Recebido:", msg)
	default:
		fmt.Println("Nenhuma mensagem disponível")
	}

	// Agora com valor disponível
	ch <- "olá"
	select {
	case msg := <-ch:
		fmt.Println("Recebido:", msg)
	default:
		fmt.Println("Nenhuma mensagem disponível")
	}
}

// selectTimeout demonstra timeout com time.After.
func selectTimeout() {
	ch := make(chan string)

	go func() {
		time.Sleep(2 * time.Second) // demora demais
		ch <- "resposta tardia"
	}()

	select {
	case res := <-ch:
		fmt.Println("OK:", res)
	case <-time.After(500 * time.Millisecond):
		fmt.Println("Timeout! Nenhuma resposta em 500ms")
	}
}

func main() {
	fmt.Println("=== Select Básico ===")
	selectBasico()

	fmt.Println("\n=== Select com Default ===")
	selectDefault()

	fmt.Println("\n=== Select com Timeout ===")
	selectTimeout()
}
