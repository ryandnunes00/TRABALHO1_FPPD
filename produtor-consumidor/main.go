// Problema 2 — Produtor/Consumidor com Buffer Limitado
//
// P produtores, C consumidores e um buffer compartilhado de capacidade K.
// Produtor bloqueia com buffer CHEIO; consumidor bloqueia com buffer VAZIO.
//
// Duas versões do buffer (ambas implementam a interface Buffer):
//
//  1. BufferChannel   : o buffer é um channel com capacidade K; o bloqueio
//     é o do próprio Go (exemplo do professor "com Canais").
//  2. BufferSemaforo  : array circular protegido por semáforos de contagem
//     notFull / notEmpty + exclusão mútua nas posições de
//     escrita e de leitura (Algorithm 6.8 do Ben-Ari, como
//     no exemplo do professor "com Semáforos").
//
// Semáforo = channel com capacidade N em que cada token é uma permissão:
//
//	Wait/P = receber um token (<-s)   | Signal/V = enviar um token (s <- struct{}{})
//
// (Fazemos assim, em vez de golang.org/x/sync/semaphore, para o projeto não
//
//	depender de biblioteca externa e para poder usar select com time.After.)
//
// Encerramento ordenado:
//   - Channel : quando todos os produtores terminam, main faz close(ch).
//     Consumidores continuam recebendo até esvaziar e só então veem
//     "fechado" -> nenhum item é perdido.
//   - Semáforo: não existe close; main coloca C "pílulas de veneno" (valor -1)
//     no buffer DEPOIS dos produtores terminarem. Como o buffer é
//     FIFO, cada pílula só chega depois de todos os itens reais.
//
// Verificação final: cada item (id único) deve ser consumido EXATAMENTE 1 vez
// e total produzido == total consumido.
//
// Uso: go run -race ./produtor-consumidor -p 3 -c 3 -k 1,10,100
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// pilula é o valor especial que manda um consumidor encerrar (versão semáforo).
const pilula = -1

// Resultado do Take / TakeTimeout
type Status int

const (
	Ok      Status = iota // item retirado
	Fechado               // buffer encerrado e vazio: consumidor deve terminar
	Timeout               // nenhum item chegou dentro do prazo
)

// Buffer é a interface comum às duas implementações, para que o mesmo
// código de produtores/consumidores/medições funcione nas duas versões.
type Buffer interface {
	Put(item int)                              // bloqueia se cheio
	Take() (int, Status)                       // bloqueia se vazio
	TakeTimeout(d time.Duration) (int, Status) // select + time.After
	Close()                                    // chamado após o fim dos produtores
	Len() int                                  // itens no buffer (para ocupação média)
}

// ===========================================================================
// VERSÃO 1: buffer = channel com capacidade K
// ===========================================================================
type BufferChannel struct {
	ch chan int
}

func NovoBufferChannel(k int) *BufferChannel {
	return &BufferChannel{ch: make(chan int, k)}
}

// Put: o envio já bloqueia sozinho quando o channel está cheio.
func (b *BufferChannel) Put(item int) { b.ch <- item }

// Take: a recepção bloqueia quando vazio; ok=false só quando o channel está
// FECHADO E VAZIO (todos os itens já foram drenados).
func (b *BufferChannel) Take() (int, Status) {
	item, ok := <-b.ch
	if !ok {
		return 0, Fechado
	}
	return item, Ok
}

// TakeTimeout: select entre receber um item e o temporizador time.After.
func (b *BufferChannel) TakeTimeout(d time.Duration) (int, Status) {
	select {
	case item, ok := <-b.ch:
		if !ok {
			return 0, Fechado
		}
		return item, Ok
	case <-time.After(d): // nenhum item chegou a tempo
		return 0, Timeout
	}
}

func (b *BufferChannel) Close()   { close(b.ch) }
func (b *BufferChannel) Len() int { return len(b.ch) }

// ===========================================================================
// VERSÃO 2: array circular + semáforos
// ===========================================================================

// Semaforo de contagem: tokens dentro do channel = permissões disponíveis.
type Semaforo chan struct{}

// NovoSemaforo cria um semáforo com capacidade `cap` e `inicial` permissões.
func NovoSemaforo(cap, inicial int) Semaforo {
	s := make(Semaforo, cap)
	for i := 0; i < inicial; i++ {
		s <- struct{}{}
	}
	return s
}
func (s Semaforo) Wait()   { <-s }             // P: consome uma permissão (bloqueia se 0)
func (s Semaforo) Signal() { s <- struct{}{} } // V: devolve uma permissão

type BufferSemaforo struct {
	buf   []int // array circular de tamanho K
	in    int   // próxima posição de ESCRITA (protegida por mutexIn)
	out   int   // próxima posição de LEITURA  (protegida por mutexOut)
	nCons int   // quantidade de consumidores (para saber quantas pílulas enviar)

	notFull  Semaforo // conta VAGAS livres        (inicia em K)
	notEmpty Semaforo // conta ITENS disponíveis    (inicia em 0)
	mutexIn  Semaforo // exclusão mútua entre PRODUTORES (posição de escrita)
	mutexOut Semaforo // exclusão mútua entre CONSUMIDORES (posição de leitura)
}

func NovoBufferSemaforo(k, nCons int) *BufferSemaforo {
	return &BufferSemaforo{
		buf:      make([]int, k),
		nCons:    nCons,
		notFull:  NovoSemaforo(k, k), // K vagas livres
		notEmpty: NovoSemaforo(k, 0), // 0 itens
		mutexIn:  NovoSemaforo(1, 1), // binário
		mutexOut: NovoSemaforo(1, 1), // binário
	}
}

// Put (produtor):
//  1. notFull.Wait()  -> reserva UMA vaga (bloqueia se o buffer está cheio)
//  2. mutexIn.Wait()  -> só um produtor por vez mexe em `in`
//     (sem isso, dois produtores escreveriam na MESMA posição)
//  3. escreve, avança `in`
//  4. mutexIn.Signal()
//  5. notEmpty.Signal() -> avisa que há um item novo
//
// Por que dois mutexes (e não um só)? Um produtor e um consumidor mexem em
// índices e posições diferentes, então podem trabalhar ao mesmo tempo. A
// posição escrita nunca é lida antes de notEmpty.Signal(), e nunca é
// reescrita antes de o consumidor fazer notFull.Signal(): os semáforos
// garantem a ordem (e o -race confirma que não há data race).
func (b *BufferSemaforo) Put(item int) {
	b.notFull.Wait()
	b.mutexIn.Wait()
	b.buf[b.in] = item
	b.in = (b.in + 1) % len(b.buf)
	b.mutexIn.Signal()
	b.notEmpty.Signal()
}

// retirar executa os passos 2-5 do consumidor (depois de ter um item garantido).
//
//	mutexOut.Wait()  -> só um consumidor por vez mexe em `out`
//	(sem isso, dois consumidores retirariam o MESMO item)
func (b *BufferSemaforo) retirar() (int, Status) {
	b.mutexOut.Wait()
	item := b.buf[b.out]
	b.out = (b.out + 1) % len(b.buf)
	b.mutexOut.Signal()
	b.notFull.Signal() // libera uma vaga
	if item == pilula {
		return 0, Fechado
	}
	return item, Ok
}

// Take (consumidor): notEmpty.Wait() garante que existe um item.
func (b *BufferSemaforo) Take() (int, Status) {
	b.notEmpty.Wait() // bloqueia se o buffer está vazio
	return b.retirar()
}

// TakeTimeout: select entre "há item" (receber token de notEmpty) e time.After.
func (b *BufferSemaforo) TakeTimeout(d time.Duration) (int, Status) {
	select {
	case <-b.notEmpty: // conseguiu a permissão de um item
		return b.retirar()
	case <-time.After(d):
		return 0, Timeout
	}
}

// Close: coloca uma pílula por consumidor. Passa pelo Put normal, então
// respeita o limite do buffer e a ordem FIFO (vem depois dos itens reais).
func (b *BufferSemaforo) Close() {
	for i := 0; i < b.nCons; i++ {
		b.Put(pilula)
	}
}

// Len: permissões de notEmpty = itens disponíveis (aproximação suficiente
// para amostrar a ocupação).
func (b *BufferSemaforo) Len() int { return len(b.notEmpty) }

// ===========================================================================
// Configuração e resultados
// ===========================================================================
type Config struct {
	p, c, itens int           // produtores, consumidores, itens por produtor
	tProd       time.Duration // tempo MÉDIO para produzir um item
	tCons       time.Duration // tempo MÉDIO para consumir um item
	timeout     time.Duration // prazo do consumidor 0 (select + time.After)
	atraso      time.Duration // produtores só começam após este atraso (provoca timeouts)
	verbose     bool
}

type Resultado struct {
	versao        string
	k             int
	duracao       time.Duration
	produzidos    int
	consumidos    int
	porConsumidor []int
	timeouts      int
	ocupMedia     float64
	correto       bool
	problema      string
}

// sorteia devolve um tempo com distribuição EXPONENCIAL de média `media`
// (limitado a 8x a média). Tempos irregulares geram "rajadas" de produção ou
// de consumo — é isso que um buffer maior consegue absorver (efeito de K).
func sorteia(media time.Duration) time.Duration {
	if media <= 0 {
		return 0
	}
	d := time.Duration(rand.ExpFloat64() * float64(media))
	if d > 8*media {
		d = 8 * media
	}
	return d
}

// ===========================================================================
// Produtor e consumidor (iguais para as duas versões)
// ===========================================================================

// Cada produtor p gera os ids p*itens ... p*itens+itens-1 (únicos no programa).
func produtor(id int, cfg Config, buf Buffer, produzidos *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < cfg.itens; i++ {
		time.Sleep(sorteia(cfg.tProd)) // "produzindo"
		item := id*cfg.itens + i
		buf.Put(item) // bloqueia se o buffer estiver cheio
		produzidos.Add(1)
		if cfg.verbose {
			fmt.Printf("[Produtor %d] inseriu %d\n", id, item)
		}
	}
}

// consumidor retorna (quantos consumiu, quantos timeouts teve).
// O consumidor 0 usa TakeTimeout (select + time.After); se nenhum item chega
// no prazo, executa um COMPORTAMENTO ALTERNATIVO (aqui: registra e continua).
func consumidor(id int, cfg Config, buf Buffer, vistos []int32) (consumiu, timeouts int) {
	for {
		var item int
		var st Status
		if id == 0 {
			item, st = buf.TakeTimeout(cfg.timeout)
		} else {
			item, st = buf.Take()
		}

		switch st {
		case Fechado:
			return // buffer encerrado e drenado: termina
		case Timeout:
			// Comportamento alternativo: nenhum item em `timeout`.
			timeouts++
			if cfg.verbose || timeouts <= 3 {
				fmt.Printf("  [Consumidor %d] TIMEOUT: nenhum item em %v -> faz tarefa alternativa e tenta de novo\n",
					id, cfg.timeout)
			}
			continue
		}

		atomic.AddInt32(&vistos[item], 1) // marca o item como consumido (para verificar)
		consumiu++
		if cfg.verbose {
			fmt.Printf("  [Consumidor %d] retirou %d\n", id, item)
		}
		time.Sleep(sorteia(cfg.tCons)) // "consumindo"
	}
}

// ===========================================================================
// Executa uma versão com capacidade k
// ===========================================================================
func executar(versao string, k int, cfg Config) Resultado {
	var buf Buffer
	if versao == "channel" {
		buf = NovoBufferChannel(k)
	} else {
		buf = NovoBufferSemaforo(k, cfg.c)
	}

	total := cfg.p * cfg.itens
	vistos := make([]int32, total) // vistos[id] = quantas vezes o item id foi consumido
	var produzidos atomic.Int64

	res := Resultado{versao: versao, k: k, porConsumidor: make([]int, cfg.c)}
	timeoutsPorCons := make([]int, cfg.c)

	// --- consumidores começam primeiro (e esperam) ---
	var wgCons sync.WaitGroup
	for i := 0; i < cfg.c; i++ {
		wgCons.Add(1)
		go func(id int) {
			defer wgCons.Done()
			// cada goroutine escreve só na sua posição -> sem data race
			res.porConsumidor[id], timeoutsPorCons[id] = consumidor(id, cfg, buf, vistos)
		}(i)
	}

	// --- amostrador de ocupação: lê Len() a cada 1 ms ---
	parar := make(chan struct{})
	amostrasDone := make(chan struct{})
	var somaOcup, nAmostras float64
	go func() {
		defer close(amostrasDone)
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				somaOcup += float64(buf.Len())
				nAmostras++
			case <-parar:
				return
			}
		}
	}()

	time.Sleep(cfg.atraso) // produtores atrasados: o consumidor 0 sofre timeouts

	// --- produtores ---
	inicio := time.Now() // o cronômetro só conta a partir daqui
	var wgProd sync.WaitGroup
	for i := 0; i < cfg.p; i++ {
		wgProd.Add(1)
		go produtor(i, cfg, buf, &produzidos, &wgProd)
	}

	wgProd.Wait() // todos os produtores terminaram
	buf.Close()   // ENCERRAMENTO: sinaliza fim; consumidores drenam o buffer
	wgCons.Wait() // todos os consumidores terminaram (buffer vazio)
	res.duracao = time.Since(inicio)

	close(parar)
	<-amostrasDone // espera o amostrador terminar antes de ler somaOcup (sem race)
	if nAmostras > 0 {
		res.ocupMedia = somaOcup / nAmostras
	}

	// --- verificação ---
	res.produzidos = int(produzidos.Load())
	res.correto = true
	for id, v := range vistos {
		res.consumidos += int(v)
		if v == 0 {
			res.correto, res.problema = false, fmt.Sprintf("item %d PERDIDO", id)
		} else if v > 1 {
			res.correto, res.problema = false, fmt.Sprintf("item %d consumido %d vezes", id, v)
		}
	}
	if res.produzidos != res.consumidos {
		res.correto, res.problema = false, "produzido != consumido"
	}
	for _, t := range timeoutsPorCons {
		res.timeouts += t
	}
	return res
}

func imprimir(r Resultado) {
	vazao := float64(r.consumidos) / r.duracao.Seconds()
	status := "OK"
	if !r.correto {
		status = "ERRO: " + r.problema
	}
	fmt.Printf("%-9s K=%-4d | itens %d/%d | tempo %-8v | vazão %8.0f itens/s | ocupação média %6.2f (%5.1f%%) | timeouts %d | verificação: %s\n",
		r.versao, r.k, r.consumidos, r.produzidos, r.duracao.Round(time.Millisecond),
		vazao, r.ocupMedia, 100*r.ocupMedia/float64(r.k), r.timeouts, status)
	fmt.Printf("          itens por consumidor: %v\n", r.porConsumidor)
}

func lerListaK(s string) []int {
	var ks []int
	for _, p := range strings.Split(s, ",") {
		k, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || k < 1 {
			fmt.Println("erro: valor de K inválido:", p)
			os.Exit(1)
		}
		ks = append(ks, k)
	}
	return ks
}

func main() {
	p := flag.Int("p", 3, "número de produtores")
	c := flag.Int("c", 3, "número de consumidores")
	ks := flag.String("k", "1,10,100", "capacidades do buffer, separadas por vírgula")
	itens := flag.Int("itens", 200, "itens produzidos por cada produtor")
	tprod := flag.Int("tprod", 2, "tempo médio para produzir 1 item (ms)")
	tcons := flag.Int("tcons", 2, "tempo médio para consumir 1 item (ms)")
	timeout := flag.Int("timeout", 15, "prazo do consumidor 0 (select + time.After), em ms")
	atraso := flag.Int("atraso", 40, "atraso inicial dos produtores (ms) — provoca timeouts do consumidor 0")
	versao := flag.String("v", "ambas", "versão: channel | semaforo | ambas")
	verbose := flag.Bool("verbose", false, "mostra cada inserção/retirada")
	flag.Parse()

	if *p < 1 || *c < 1 || *itens < 1 {
		fmt.Println("erro: use -p, -c e -itens >= 1")
		os.Exit(1)
	}
	cfg := Config{
		p: *p, c: *c, itens: *itens,
		tProd:   time.Duration(*tprod) * time.Millisecond,
		tCons:   time.Duration(*tcons) * time.Millisecond,
		timeout: time.Duration(*timeout) * time.Millisecond,
		atraso:  time.Duration(*atraso) * time.Millisecond,
		verbose: *verbose,
	}

	versoes := []string{"channel", "semaforo"}
	switch *versao {
	case "channel", "semaforo":
		versoes = []string{*versao}
	case "ambas":
	default:
		fmt.Println("erro: versão inválida:", *versao)
		os.Exit(1)
	}

	fmt.Printf("=== Produtor/Consumidor | P=%d produtores, C=%d consumidores, %d itens/produtor (total %d) ===\n",
		cfg.p, cfg.c, cfg.itens, cfg.p*cfg.itens)
	fmt.Printf("Consumidor 0 usa select+time.After (timeout %v); produtores atrasados %v\n\n", cfg.timeout, cfg.atraso)

	falhou := false
	for _, v := range versoes {
		for _, k := range lerListaK(*ks) {
			r := executar(v, k, cfg)
			imprimir(r)
			falhou = falhou || !r.correto
		}
		fmt.Println()
	}

	if falhou {
		fmt.Println("FALHA na verificação!")
		os.Exit(1)
	}
	fmt.Println("Verificação final: para todas as execuções, total produzido == total consumido e nenhum item perdido/duplicado.")
	fmt.Println("Programa encerrado: todas as goroutines finalizaram.")
}
