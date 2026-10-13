// Canal síncrono (sem buffer) — rendez-vous entre goroutines.
//
// Demonstra que emissor e receptor bloqueiam até que ambos
// estejam prontos, garantindo que o dado foi entregue.
package main

import "fmt"

func main() {
	ch := make(chan int)

	go func() {
		fmt.Println("Enviando...")
		ch <- 42 // bloqueia até alguém receber
		fmt.Println("Enviado!")
	}()

	fmt.Println("Esperando...")
	valor := <-ch // bloqueia até alguém enviar
	fmt.Println("Recebido:", valor)
}
