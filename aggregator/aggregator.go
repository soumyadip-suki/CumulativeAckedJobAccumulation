package aggregator

import (
	"fmt"
	"sync"

	"github.com/LearningMotors/cumulative_acked_job_accumulation/jobs"
	"github.com/LearningMotors/cumulative_acked_job_accumulation/sink"
)

type Aggregator struct {
	mu        sync.Mutex
	lastID    int
	channel   chan *jobs.Job
	flushSink *sink.Sink
}

func NewAggregator() *Aggregator {
	channel := make(chan *jobs.Job, 1000)
	agg := &Aggregator{
		channel:   channel,
		lastID:    -1,
		flushSink: sink.NewSink(channel),
	}
	return agg
}

func (a *Aggregator) Start() {
	// jobsIds := a.flushSink.Flush()
	// fmt.Println("Flushed jobs", jobsIds)
	a.flushSink.Flush()
	fmt.Println("Flushed jobs")
}

func (a *Aggregator) TryAddJob(job *jobs.Job) (int, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if job.GetID() == a.lastID+1 {
		a.lastID = job.GetID()
		a.channel <- job
		return 0, nil
	}
	return (job.GetID() - a.lastID), fmt.Errorf("job ID is out of order... wait till pointer reaches here")
}

func (a *Aggregator) Close() {
	close(a.channel)
}
