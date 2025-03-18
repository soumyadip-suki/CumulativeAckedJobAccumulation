package jobs

type Job struct {
	id int
}

func NewJob(id int) *Job {
	return &Job{id: id}
}

func (j *Job) GetID() int {
	return j.id
}
