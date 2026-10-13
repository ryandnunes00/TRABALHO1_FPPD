// Seção Crítica com Monitores
//
// Demonstra exclusão mútua usando o padrão de monitor em Go:
// sync.Mutex encapsula o estado compartilhado dentro de uma struct,
// e os métodos garantem exclusão mútua automática (lock/unlock).
//
// Baseado no Algorithm 7.1 do Ben-Ari.
package main

import (
	"fmt"
	"sync"
)

// Monitor encapsula o estado protegido.
// Apenas os métodos exportados acessam o campo interno,
// garantindo exclusão mútua por construção.
type ContadorMonitor struct {
	mu    sync.Mutex
	valor int
}

func (m *ContadorMonitor) Incrementar() {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Operação composta (lê + incrementa + escreve) é atômica
	// graças ao lock do monitor.
	temp := m.valor
	m.valor = temp + 1
}

func (m *ContadorMonitor) Valor() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.valor
}

func main() {
	monitor := &ContadorMonitor{}
	var wg sync.WaitGroup

	n := 5
	iteracoes := 100_000
	fmt.Printf("Iniciando %d goroutines, cada uma incrementando %d vezes...\n", n, iteracoes)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iteracoes; j++ {
				monitor.Incrementar()
			}
		}()
	}

	wg.Wait()

	esperado := n * iteracoes
	obtido := monitor.Valor()
	fmt.Printf("Valor esperado: %d\n", esperado)
	fmt.Printf("Valor obtido:   %d\n", obtido)

	if obtido == esperado {
		fmt.Println("OK — exclusão mútua garantida pelo monitor (sync.Mutex).")
	} else {
		fmt.Println("ERRO — data race detectada.")
	}
}
