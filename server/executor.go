package server

import (
	"math/rand"
	"sync"
	"time"

	"github.com/LearningMotors/cumulative_acked_job_accumulation/jobs"
)

const (
	baseSleep  = 300
	sleepRange = 800
)

type Executor struct {
	mu         sync.Mutex
	totalJobs  int
	totalSleep int
}

func NewExecutor() *Executor {
	return &Executor{
		totalSleep: 0,
		totalJobs:  0,
	}
}

func (e *Executor) ExecuteJob(job *jobs.Job) *jobs.Job {

	sleepDuration := rand.Intn(sleepRange) + baseSleep
	time.Sleep(time.Duration(sleepDuration) * time.Millisecond)

	e.mu.Lock()
	defer e.mu.Unlock()
	e.totalSleep += sleepDuration
	e.totalJobs++
	return job
}

func (e *Executor) GetAvgSleep() int {
	// e.mu.Lock()
	// defer e.mu.Unlock()
	return e.totalSleep / e.totalJobs
}
