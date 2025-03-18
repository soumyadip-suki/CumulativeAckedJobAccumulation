package sink

import (
	"github.com/LearningMotors/cumulative_acked_job_accumulation/jobs"
)

type Sink struct {
	channel <-chan *jobs.Job
}

func NewSink(channel <-chan *jobs.Job) *Sink {
	return &Sink{channel: channel}
}

func (s *Sink) Flush() []int {
	jobsIds := []int{}
	for job := range s.channel {
		// fmt.Println("Flushed", job.GetID())
		jobsIds = append(jobsIds, job.GetID())
	}
	return jobsIds
}
