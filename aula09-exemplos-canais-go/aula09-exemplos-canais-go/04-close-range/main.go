// Fechamento de canal e iteração com range.
//
// close(ch) sinaliza que não haverá mais valores.
// range ch itera até o canal ser fechado.
package main

import "fmt"

func main() {
	ch := make(chan int, 5)

	go func() {
		for i := 0; i < 5; i++ {
			ch <- i * i
		}
		close(ch) // sinaliza fim
	}()

	for v := range ch { // termina quando ch é fechado
		fmt.Println(v) // 0, 1, 4, 9, 16
	}
}
