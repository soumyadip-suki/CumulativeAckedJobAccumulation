package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/LearningMotors/cumulative_acked_job_accumulation/aggregator"
	"github.com/LearningMotors/cumulative_acked_job_accumulation/jobs"
	"github.com/LearningMotors/cumulative_acked_job_accumulation/server"
)

type Client struct {
	jobChannel   <-chan *jobs.Job
	aggregator   *aggregator.Aggregator
	executor     *server.Executor
	needsBackoff bool
}

func NewClient(jobChannel <-chan *jobs.Job, needsBackoff bool) *Client {
	return &Client{
		jobChannel:   jobChannel,
		executor:     server.NewExecutor(),
		aggregator:   aggregator.NewAggregator(),
		needsBackoff: needsBackoff,
	}
}

func (c *Client) Initialize() {
	c.aggregator.Start()
}

func (c *Client) ConsumeAndProcess() {
	t := time.Now()
	defer c.aggregator.Close()

	wg := sync.WaitGroup{}
	for job := range c.jobChannel {
		wg.Add(1)
		go func(job *jobs.Job) {
			defer wg.Done()
			respJob := c.executor.ExecuteJob(job)
			for {
				backoff, err := c.aggregator.TryAddJob(respJob)
				if err != nil {
					if c.needsBackoff {
						backOff(backoff)
					}
					continue
				}
				break
			}
		}(job)
	}
	wg.Wait()
	fmt.Println("Avg Execution Latency", c.executor.GetAvgSleep(), "milliseconds")
	fmt.Println("Time taken to consume and process", time.Since(t))
}

func backOff(backoff int) {
	time.Sleep(time.Duration(backoff/100) * time.Millisecond)
}
