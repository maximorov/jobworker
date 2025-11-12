package jobworker

// After provides a mechanism to queue jobs that should only be executed
// after a certain condition is met, typically the successful completion of another operation.
type After struct {
	success chan bool
	counter int
}

// NewAfter creates a new After instance.
func NewAfter() *After {
	return &After{
		success: make(chan bool),
	}
}

// Queue adds a job to be executed if the condition is met.
func (o *After) Queue(b business, opts ...JobOption) {
	o.counter++
	QueueJobIf(NewJob(b, opts...), o.success)
}

// Notify signals the completion of the primary operation.
// If the error is nil, the queued jobs will be executed.
func (o *After) Notify(err error) {
	for i := 0; i < o.counter; i++ {
		o.success <- err == nil
	}
	close(o.success)
}
