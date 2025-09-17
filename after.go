package jobworker

type After struct {
	success chan bool
	counter int
}

func NewAfter() *After {
	return &After{
		success: make(chan bool),
	}
}

func (o *After) Queue(b business, opts ...JobOption) {
	QueueJobIf(NewJob(b, opts...), o.success)
}

func (o *After) Notify(err error) {
	for i := 0; i < o.counter; i++ {
		o.success <- err == nil
	}
	close(o.success)
}
