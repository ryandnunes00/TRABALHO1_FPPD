// Channels direcionais — restrição de envio e recepção.
//
// O produtor recebe chan<- (somente envio) e o consumidor
// recebe <-chan (somente recepção). O compilador impede
// operações inválidas.
package main

import "fmt"

func produtor(ch chan<- int) { // ch: somente envio
	for i := 0; i < 10; i++ {
		ch <- i
	}
	close(ch)
}

func consumidor(ch <-chan int) { // ch: somente recepção
	for v := range ch {
		fmt.Println("Recebido:", v)
	}
}

func main() {
	ch := make(chan int, 5) // bidirecional
	go produtor(ch)         // conversão implícita para chan<-
	consumidor(ch)          // conversão implícita para <-chan
}
