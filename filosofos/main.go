// Problema 1 — Jantar dos Filósofos com CHANNELS
//
// Cada garfo é um channel com buffer 1 contendo um "token" (struct{}{}):
//   - pegar o garfo   = RECEBER do channel  (<-garfo)  -> bloqueia se o token não está lá
//   - devolver o garfo = ENVIAR no channel  (garfo <- struct{}{})
//
// (mesma ideia do exemplo do professor "Jantar dos Filósofos com Canais".)
//
// O programa executa 4 versões, uma depois da outra:
//  1. base        : todos pegam primeiro o garfo da ESQUERDA  -> DEADLOCK (demonstração)
//  2. hierarquia  : hierarquia de recursos (garfo de menor número primeiro)
//     (é a "quebra de simetria" do exemplo do professor, generalizada)
//  3. limite      : no máximo N-1 filósofos sentados à mesa ao mesmo tempo
//  4. garcom      : um processo garçom (goroutine) decide quem pode pegar os garfos
//
// Para cada versão coleta: refeições de cada filósofo, tempo de espera pelos
// garfos (média e máximo) e o índice de Jain (medida de fairness).
//
// Uso:  go run -race ./filosofos -n 5 -r 20
//
//	go run -race ./filosofos -n 5 -r 20 -e garcom -v
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Estado de cada filósofo (usado só para mostrar o que aconteceu no deadlock)
// ---------------------------------------------------------------------------
const (
	pensando      int32 = iota // não quer comer
	esperando                  // quer comer, ainda não tem nenhum garfo
	seguraUmGarfo              // tem 1 garfo e espera o outro (hold and wait!)
	comendo                    // tem os dois garfos
)

var nomeEstado = map[int32]string{
	pensando:      "pensando",
	esperando:     "esperando garfos",
	seguraUmGarfo: "segura 1 garfo, espera o outro",
	comendo:       "comendo",
}

// ---------------------------------------------------------------------------
// Mesa: recursos compartilhados por todas as estratégias
// ---------------------------------------------------------------------------
type Mesa struct {
	n      int
	garfos []chan struct{} // garfo i = channel de capacidade 1 com 1 token
	pausa  time.Duration   // pausa entre pegar o 1º e o 2º garfo (expõe o deadlock)
	estado []atomic.Int32  // estado de cada filósofo (só para diagnóstico)
}

func novaMesa(n int, pausa time.Duration) *Mesa {
	m := &Mesa{n: n, pausa: pausa}
	m.garfos = make([]chan struct{}, n)
	m.estado = make([]atomic.Int32, n)
	for i := range m.garfos {
		m.garfos[i] = make(chan struct{}, 1)
		m.garfos[i] <- struct{}{} // token no channel = garfo livre
	}
	return m
}

// pegar tenta receber o token do garfo f. Retorna false se a execução foi
// abortada (usado só para sair do deadlock da versão base de forma limpa).
func (m *Mesa) pegar(f int, abort <-chan struct{}) bool {
	select {
	case <-m.garfos[f]:
		return true
	case <-abort:
		return false
	}
}

// devolver coloca o token de volta (nunca bloqueia: o buffer tem 1 vaga e
// só quem tem o garfo pode devolvê-lo).
func (m *Mesa) devolver(f int) { m.garfos[f] <- struct{}{} }

// pegarDois pega o garfo `primeiro` e depois o `segundo`, nessa ordem.
// A pausa entre os dois garante que, na versão base, todos consigam pegar o
// primeiro garfo antes de qualquer um pegar o segundo -> deadlock reproduzível.
func (m *Mesa) pegarDois(id, primeiro, segundo int, abort <-chan struct{}) bool {
	if !m.pegar(primeiro, abort) {
		return false
	}
	m.estado[id].Store(seguraUmGarfo)
	time.Sleep(m.pausa)
	if !m.pegar(segundo, abort) {
		m.devolver(primeiro) // aborto: devolve o que tinha
		return false
	}
	return true
}

// ---------------------------------------------------------------------------
// Interface das estratégias
// ---------------------------------------------------------------------------
type Estrategia interface {
	Pegar(id int, abort <-chan struct{}) bool // false = abortado
	Devolver(id int)
	Encerrar() // libera recursos extras (ex.: encerra o garçom)
}

// ---------------------------------------------------------------------------
// 1) BASE: todos pegam ESQUERDA e depois DIREITA  -> deadlock
// ---------------------------------------------------------------------------
type base struct{ m *Mesa }

func (b *base) Pegar(id int, abort <-chan struct{}) bool {
	esq, dir := id, (id+1)%b.m.n
	return b.m.pegarDois(id, esq, dir, abort)
}
func (b *base) Devolver(id int) {
	b.m.devolver(id)
	b.m.devolver((id + 1) % b.m.n)
}
func (b *base) Encerrar() {}

// ---------------------------------------------------------------------------
//  2. HIERARQUIA DE RECURSOS: sempre pega primeiro o garfo de MENOR número.
//     Quebra a espera circular (4ª condição de Coffman). Na prática, só o
//     filósofo N-1 inverte a ordem (esquerda=N-1, direita=0).
//
// ---------------------------------------------------------------------------
type hierarquia struct{ m *Mesa }

func (h *hierarquia) Pegar(id int, abort <-chan struct{}) bool {
	esq, dir := id, (id+1)%h.m.n
	if dir < esq { // garfo da direita tem número menor: pega ele primeiro
		esq, dir = dir, esq
	}
	return h.m.pegarDois(id, esq, dir, abort)
}
func (h *hierarquia) Devolver(id int) {
	h.m.devolver(id)
	h.m.devolver((id + 1) % h.m.n)
}
func (h *hierarquia) Encerrar() {}

// ---------------------------------------------------------------------------
//  3. LIMITE: no máximo N-1 filósofos disputando garfos ao mesmo tempo.
//     Com N-1 sentados, sempre sobra 1 garfo livre alguém consegue o par
//     -> a espera circular nunca fecha. O "porteiro" é um channel com
//     capacidade N-1 usado como semáforo de contagem.
//
// ---------------------------------------------------------------------------
type limite struct {
	m       *Mesa
	lugares chan struct{} // N-1 tokens = N-1 lugares à mesa
}

func novoLimite(m *Mesa) *limite {
	l := &limite{m: m, lugares: make(chan struct{}, m.n-1)}
	for i := 0; i < m.n-1; i++ {
		l.lugares <- struct{}{}
	}
	return l
}

func (l *limite) Pegar(id int, abort <-chan struct{}) bool {
	select { // pega um lugar à mesa
	case <-l.lugares:
	case <-abort:
		return false
	}
	esq, dir := id, (id+1)%l.m.n // dentro da mesa, todos usam esquerda->direita
	if !l.m.pegarDois(id, esq, dir, abort) {
		l.lugares <- struct{}{}
		return false
	}
	return true
}
func (l *limite) Devolver(id int) {
	l.m.devolver(id)
	l.m.devolver((id + 1) % l.m.n)
	l.lugares <- struct{}{} // levanta da mesa
}
func (l *limite) Encerrar() {}

// ---------------------------------------------------------------------------
//  4. GARÇOM: processo centralizado (goroutine) que só autoriza um filósofo
//     quando OS DOIS garfos dele estão livres (elimina "hold and wait").
//     Os pedidos ficam em fila FIFO. Para evitar starvation, um pedido NÃO
//     pode ser atendido se um pedido MAIS ANTIGO disputa um dos mesmos garfos
//     (o mais antigo "reserva" seus garfos).
//
// ---------------------------------------------------------------------------
type pedido struct {
	id   int
	resp chan struct{} // garçom envia aqui quando autoriza (buffer 1: nunca bloqueia)
}

type garcom struct {
	m         *Mesa
	pedidos   chan pedido   // filósofos -> garçom: "quero comer"
	liberacao chan int      // filósofos -> garçom: "terminei de comer"
	fim       chan struct{} // main -> garçom: encerrar
	terminou  chan struct{} // garçom -> main: encerrei
}

func novoGarcom(m *Mesa) *garcom {
	g := &garcom{
		m:         m,
		pedidos:   make(chan pedido),
		liberacao: make(chan int),
		fim:       make(chan struct{}),
		terminou:  make(chan struct{}),
	}
	go g.rodar()
	return g
}

// rodar é o laço do garçom. Só ELE mexe em `usado` e `fila` -> sem mutex,
// a exclusão mútua vem de a goroutine ser a única dona desses dados.
func (g *garcom) rodar() {
	defer close(g.terminou)
	n := g.m.n
	usado := make([]bool, n) // garfo i está reservado para alguém que come?
	var fila []pedido        // pedidos pendentes, do mais antigo ao mais novo

	atender := func() {
		reservado := make([]bool, n) // garfos "reservados" por pedidos mais antigos
		var restante []pedido
		for _, p := range fila {
			l, r := p.id, (p.id+1)%n
			if !usado[l] && !usado[r] && !reservado[l] && !reservado[r] {
				usado[l], usado[r] = true, true
				p.resp <- struct{}{} // autoriza
			} else {
				reservado[l], reservado[r] = true, true // mais antigo tem prioridade
				restante = append(restante, p)
			}
		}
		fila = restante
	}

	for {
		select {
		case p := <-g.pedidos:
			fila = append(fila, p)
			atender()
		case id := <-g.liberacao:
			usado[id], usado[(id+1)%n] = false, false
			atender()
		case <-g.fim:
			return
		}
	}
}

func (g *garcom) Pegar(id int, abort <-chan struct{}) bool {
	p := pedido{id: id, resp: make(chan struct{}, 1)}
	select {
	case g.pedidos <- p: // faz o pedido
	case <-abort:
		return false
	}
	select {
	case <-p.resp: // espera autorização
	case <-abort:
		return false
	}
	// Autorizado: os dois garfos estão livres, então estes receives não bloqueiam.
	return g.m.pegarDois(id, id, (id+1)%g.m.n, abort)
}
func (g *garcom) Devolver(id int) {
	g.m.devolver(id)
	g.m.devolver((id + 1) % g.m.n)
	g.liberacao <- id // avisa o garçom
}
func (g *garcom) Encerrar() {
	close(g.fim)
	<-g.terminou // espera o garçom terminar: nenhuma goroutine sobra
}

// ---------------------------------------------------------------------------
// Filósofo
// ---------------------------------------------------------------------------
type Config struct {
	n, r      int
	pensar    time.Duration // tempo máximo pensando (sorteado entre 0 e este valor)
	comer     time.Duration // tempo máximo comendo
	pausa     time.Duration
	pausaBase time.Duration // pausa maior só na versão base (força o deadlock)
	detectar  time.Duration // sem progresso por este tempo = deadlock
	verbose   bool
}

type Resultado struct {
	nome      string
	deadlock  bool
	refeicoes []int
	espera    []time.Duration // soma das esperas de cada filósofo
	esperaMax []time.Duration
	duracao   time.Duration
}

func sorteia(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(max)))
}

func filosofo(id int, cfg Config, m *Mesa, e Estrategia, abort <-chan struct{},
	res *Resultado, progresso *atomic.Int64, wg *sync.WaitGroup) {
	defer wg.Done()

	for rodada := 0; rodada < cfg.r; rodada++ {
		// PENSAR
		m.estado[id].Store(pensando)
		time.Sleep(sorteia(cfg.pensar))

		// PEGAR GARFOS (medindo o tempo de espera)
		m.estado[id].Store(esperando)
		inicio := time.Now()
		if !e.Pegar(id, abort) {
			return // execução abortada (deadlock detectado)
		}
		espera := time.Since(inicio)

		// COMER
		m.estado[id].Store(comendo)
		if cfg.verbose {
			fmt.Printf("  >>> Filósofo %d comendo (rodada %d, esperou %v)\n",
				id, rodada+1, espera.Round(time.Microsecond))
		}
		time.Sleep(sorteia(cfg.comer))

		// DEVOLVER GARFOS
		e.Devolver(id)

		// Cada goroutine escreve só nas suas posições do slice -> sem data race.
		res.refeicoes[id]++
		res.espera[id] += espera
		if espera > res.esperaMax[id] {
			res.esperaMax[id] = espera
		}
		progresso.Add(1) // o "watchdog" usa isso para detectar deadlock
	}
	m.estado[id].Store(pensando)
}

// ---------------------------------------------------------------------------
// Executa uma estratégia completa
// ---------------------------------------------------------------------------
func executar(nome string, cfg Config) Resultado {
	// Na versão base usamos uma pausa maior entre o 1º e o 2º garfo: assim todos
	// os filósofos chegam a segurar o garfo esquerdo ao mesmo tempo (deadlock).
	pausa := cfg.pausa
	if nome == "base" {
		pausa = cfg.pausaBase
	}
	m := novaMesa(cfg.n, pausa)

	var e Estrategia
	switch nome {
	case "base":
		e = &base{m}
	case "hierarquia":
		e = &hierarquia{m}
	case "limite":
		e = novoLimite(m)
	case "garcom":
		e = novoGarcom(m)
	}

	res := Resultado{
		nome:      nome,
		refeicoes: make([]int, cfg.n),
		espera:    make([]time.Duration, cfg.n),
		esperaMax: make([]time.Duration, cfg.n),
	}

	abort := make(chan struct{})
	var progresso atomic.Int64
	var wg sync.WaitGroup
	inicio := time.Now()

	for i := 0; i < cfg.n; i++ {
		wg.Add(1)
		go filosofo(i, cfg, m, e, abort, &res, &progresso, &wg)
	}

	// Espera terminar numa goroutine para poder usar select com o watchdog.
	terminaram := make(chan struct{})
	go func() { wg.Wait(); close(terminaram) }()

	// WATCHDOG: se o contador de refeições não muda por `detectar`, é deadlock.
	ticker := time.NewTicker(cfg.detectar / 10)
	defer ticker.Stop()
	ultimo, ultimaMudanca := int64(-1), time.Now()
loop:
	for {
		select {
		case <-terminaram:
			break loop
		case <-ticker.C:
			if p := progresso.Load(); p != ultimo {
				ultimo, ultimaMudanca = p, time.Now()
			} else if time.Since(ultimaMudanca) >= cfg.detectar {
				res.deadlock = true
				fmt.Printf("\n  !!! DEADLOCK detectado: nenhuma refeição há %v. Estado:\n", cfg.detectar)
				for i := 0; i < cfg.n; i++ {
					fmt.Printf("      Filósofo %d: %s\n", i, nomeEstado[m.estado[i].Load()])
				}
				close(abort) // acorda todos os bloqueados para encerrar sem vazar goroutines
				<-terminaram
				break loop
			}
		}
	}
	res.duracao = time.Since(inicio)
	e.Encerrar()
	return res
}

// ---------------------------------------------------------------------------
// Estatísticas
// ---------------------------------------------------------------------------
func (r Resultado) esperaMedia(i int) time.Duration {
	if r.refeicoes[i] == 0 {
		return 0
	}
	return r.espera[i] / time.Duration(r.refeicoes[i])
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// Índice de Jain: (Σx)² / (n·Σx²). Vale 1 quando todos recebem igual e
// tende a 1/n quando um só recebe tudo. Aplicado às esperas médias.
func jain(xs []float64) float64 {
	var s, s2 float64
	for _, x := range xs {
		s += x
		s2 += x * x
	}
	if s2 == 0 {
		return 1
	}
	return s * s / (float64(len(xs)) * s2)
}

func (r Resultado) resumo() (total int, mediaGeral, maxGeral, indiceJain float64) {
	medias := make([]float64, len(r.refeicoes))
	var somaEspera time.Duration
	var maxE time.Duration
	for i := range r.refeicoes {
		total += r.refeicoes[i]
		somaEspera += r.espera[i]
		medias[i] = ms(r.esperaMedia(i))
		if r.esperaMax[i] > maxE {
			maxE = r.esperaMax[i]
		}
	}
	if total > 0 {
		mediaGeral = ms(somaEspera) / float64(total)
	}
	return total, mediaGeral, ms(maxE), jain(medias)
}

// jainTxt devolve "n/a" quando houve deadlock (a métrica não tem sentido).
func jainTxt(r Resultado, j float64) string {
	if r.deadlock {
		return "n/a"
	}
	return fmt.Sprintf("%.4f", j)
}

func imprimir(r Resultado, cfg Config) {
	fmt.Printf("\n--- Resultado: %s ---\n", r.nome)
	fmt.Printf("%-10s %-10s %-18s %-14s\n", "Filósofo", "Refeições", "Espera média (ms)", "Espera máx (ms)")
	for i := range r.refeicoes {
		fmt.Printf("%-10d %-10d %-18.3f %-14.3f\n", i, r.refeicoes[i], ms(r.esperaMedia(i)), ms(r.esperaMax[i]))
	}
	total, _, _, j := r.resumo()
	fmt.Printf("Total de refeições: %d (esperado %d) | Duração: %v | Jain: %s | Deadlock: %v\n",
		total, cfg.n*cfg.r, r.duracao.Round(time.Millisecond), jainTxt(r, j), r.deadlock)
}

func main() {
	n := flag.Int("n", 5, "número de filósofos (>= 2)")
	r := flag.Int("r", 20, "número de refeições (iterações) de cada filósofo")
	estrat := flag.String("e", "todas", "estratégia: base | hierarquia | limite | garcom | todas")
	pensar := flag.Int("pensar", 5, "tempo máximo pensando (ms)")
	comer := flag.Int("comer", 5, "tempo máximo comendo (ms)")
	pausa := flag.Int("pausa", 2, "pausa entre o 1º e o 2º garfo (ms)")
	pausaBase := flag.Int("pausa-base", 30, "pausa entre garfos na versão base (ms) — força o deadlock")
	detectar := flag.Int("detectar", 1000, "ms sem progresso para declarar deadlock")
	verbose := flag.Bool("v", false, "mostra cada refeição")
	flag.Parse()

	if *n < 2 || *r < 1 {
		fmt.Println("erro: use -n >= 2 e -r >= 1")
		os.Exit(1)
	}
	cfg := Config{
		n: *n, r: *r,
		pensar:    time.Duration(*pensar) * time.Millisecond,
		comer:     time.Duration(*comer) * time.Millisecond,
		pausa:     time.Duration(*pausa) * time.Millisecond,
		pausaBase: time.Duration(*pausaBase) * time.Millisecond,
		detectar:  time.Duration(*detectar) * time.Millisecond,
		verbose:   *verbose,
	}

	nomes := []string{"base", "hierarquia", "limite", "garcom"}
	if *estrat != "todas" {
		ok := false
		for _, nm := range nomes {
			ok = ok || nm == *estrat
		}
		if !ok {
			fmt.Println("erro: estratégia inválida:", *estrat)
			os.Exit(1)
		}
		nomes = []string{*estrat}
	}

	fmt.Printf("=== Jantar dos Filósofos (channels) | N=%d filósofos, R=%d refeições cada ===\n", cfg.n, cfg.r)

	var resultados []Resultado
	for _, nm := range nomes {
		fmt.Printf("\n######## Estratégia: %s ########\n", nm)
		res := executar(nm, cfg)
		imprimir(res, cfg)
		resultados = append(resultados, res)
	}

	// Tabela comparativa final
	fmt.Println("\n=================== COMPARAÇÃO ===================")
	fmt.Printf("%-11s %-9s %-10s %-17s %-15s %-8s %-10s\n",
		"Estratégia", "Deadlock", "Refeições", "Espera média(ms)", "Espera máx(ms)", "Jain", "Duração")
	for _, res := range resultados {
		total, media, maxE, j := res.resumo()
		fmt.Printf("%-11s %-9v %-10d %-17.3f %-15.3f %-8s %-10v\n",
			res.nome, res.deadlock, total, media, maxE, jainTxt(res, j), res.duracao.Round(time.Millisecond))
	}
	fmt.Println("\nPrograma encerrado: todas as goroutines finalizaram.")
}
