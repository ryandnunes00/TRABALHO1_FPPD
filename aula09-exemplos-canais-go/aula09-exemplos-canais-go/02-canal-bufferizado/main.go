// Canal bufferizado (assíncrono) — desacoplamento temporal.
//
// O emissor pode enviar sem bloquear enquanto houver espaço
// no buffer. A leitura segue ordem FIFO.
package main

import "fmt"

func main() {
	ch := make(chan int, 3) // buffer com 3 posições

	ch <- 10 // não bloqueia (buffer tem espaço)
	ch <- 20 // não bloqueia
	ch <- 30 // não bloqueia
	// ch <- 40 // bloquearia — buffer cheio!

	fmt.Println(<-ch) // 10 (FIFO)
	fmt.Println(<-ch) // 20
	fmt.Println(<-ch) // 30
}
