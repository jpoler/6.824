package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.

	// Note that this implementation is more complex than simply starting ntasks
	// goroutines where each is responsible for ensuring that it's task completes.
	// That method would work, but unfortunately would break very quickly when the
	// number of tasks is much greater than the number of workers e.g. there are a
	// million tasks and only a handful of workers. In that case a goroutine per
	// task would spawn one million goroutines.

	// Instead, this implementation will only spawn a goroutine when a worker is
	// available.

	tasksCh := make(chan DoTaskArgs, ntasks)
	for i := 0; i < ntasks; i++ {
		arg := DoTaskArgs{
			JobName:       jobName,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}

		switch phase {
		case mapPhase:
			arg.File = mapFiles[i]
		}

		tasksCh <- arg
	}

	var wg sync.WaitGroup
	doneCh := make(chan struct{})

loop:
	for {
		worker := <-registerChan

		// This is gross, but it gets the job done: Try to get a task. If none are
		// in the queue, wait until all goroutines spawned by this loop finish. If
		// there are still no more tasks, we are done.
		var arg DoTaskArgs
		select {
		case arg = <-tasksCh:
		default:
			wg.Wait()
			select {
			case arg = <-tasksCh:
			default:
				break loop
			}
		}
		wg.Add(1)
		go func(worker string, arg DoTaskArgs) {
			defer func() {
				select {
				case registerChan <- worker:
				case <-doneCh:
				}
			}()

			defer wg.Done()

			if !call(worker, "Worker.DoTask", arg, nil) {
				tasksCh <- arg
			}
		}(worker, arg)
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
