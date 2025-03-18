package main

import (
	"fmt"

	"github.com/LearningMotors/cumulative_acked_job_accumulation/client"
	"github.com/LearningMotors/cumulative_acked_job_accumulation/producer"
	"golang.org/x/sync/errgroup"
)

func main() {
	numJobs := int(1e3)
	fmt.Println("Num jobs", numJobs)
	producer := producer.NewProducer(numJobs)
	errGrp := errgroup.Group{}
	errGrp.Go(func() error {
		producer.Produce()
		return nil
	})

	client := client.NewClient(producer.GetChannel(), true)
	errGrp.Go(func() error {
		client.Initialize()
		return nil
	})
	errGrp.Go(func() error {
		client.ConsumeAndProcess()
		return nil
	})

	err := errGrp.Wait()
	if err != nil {
		fmt.Println("Error", err)
	}

}
