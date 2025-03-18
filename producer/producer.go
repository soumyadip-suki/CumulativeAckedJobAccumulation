package producer

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/LearningMotors/cumulative_acked_job_accumulation/id_generator"
	"github.com/LearningMotors/cumulative_acked_job_accumulation/jobs"
)

const (
	producerSleep = 50
)

type Producer struct {
	limit       int
	channel     chan *jobs.Job
	idGenerator *id_generator.Generator
}

func NewProducer(limit int) *Producer {
	return &Producer{
		limit:       limit,
		idGenerator: id_generator.NewGenerator(),
		channel:     make(chan *jobs.Job, 1000),
	}
}

func (p *Producer) GetChannel() <-chan *jobs.Job {
	return p.channel
}

func (p *Producer) Produce() {
	defer close(p.channel)
	t := time.Now()
	for i := 0; i < p.limit; i++ {
		time.Sleep(time.Duration(rand.Intn(producerSleep)) * time.Millisecond)
		job := jobs.NewJob(p.idGenerator.Generate())
		p.channel <- job
	}
	fmt.Println("Time taken to produce", time.Since(t))
}
