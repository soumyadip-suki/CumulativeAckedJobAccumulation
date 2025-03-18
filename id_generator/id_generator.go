package id_generator

import "sync"

type Generator struct {
	mu     sync.Mutex
	lastID int
}

func NewGenerator() *Generator {
	return &Generator{
		lastID: 0,
	}
}

func (g *Generator) Generate() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	id := g.lastID
	g.lastID++
	return id
}
