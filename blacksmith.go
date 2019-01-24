package blacksmith

import (
	"github.com/mgagliardo91/go-utils"
)

// TaskName is an enumeration of jobs to execute
type TaskName int

// Task represents a unit of work to execute
type Task struct {
	LogProvider
	// Payload holds the "body" of the task
	Payload interface{} `json:"payload"`
	// TaskName identifies the type of task
	TaskName TaskName `json:"taskName"`
}

// TaskHandler is a function used to execute a type of task
type TaskHandler func(task Task)

// Worker works on a goroutine to carry out a Task
type Worker struct {
	LogProvider
	workerPool  chan chan Task
	taskChannel chan Task
	stopChannel utils.StopChannel
	executeTask TaskHandler
}

// Blacksmith is in charge of splitting and dispatching work to workers
type Blacksmith struct {
	LogProvider
	taskQueue   chan Task
	workerPool  chan chan Task
	maxWorkers  int
	stopChannel utils.StopChannel
	workers     []Worker
	handlerMap  map[TaskName]TaskHandler
	handlerFn   TaskHandler
}

// New generates a Blacksmith in charge of creating/dividing tasks
func New(maxWorkers int) *Blacksmith {
	blacksmith := Blacksmith{
		taskQueue:   make(chan Task),
		workerPool:  make(chan chan Task, maxWorkers),
		maxWorkers:  maxWorkers,
		stopChannel: utils.NewStopChannel(),
		workers:     make([]Worker, maxWorkers),
		handlerMap:  make(map[TaskName]TaskHandler),
	}
	blacksmith.InitLog("Blacksmith")
	return &blacksmith
}

// SetHandlerFn sets a TaskHandler that allows the client to handle any task
func (b *Blacksmith) SetHandlerFn(taskHandler TaskHandler) *Blacksmith {
	b.handlerFn = taskHandler
	return b
}

// SetHandler maps a task name to a task handler so that all tasks of this name will be executed by the handler
func (b *Blacksmith) SetHandler(taskName TaskName, taskHandler TaskHandler) *Blacksmith {
	b.handlerMap[taskName] = taskHandler
	return b
}

// Run starts the Blacksmith which will initialize the workers
func (b *Blacksmith) Run() *Blacksmith {
	b.LogfUsing(GetLogger().Tracef, "Starting %v workers\n", b.maxWorkers)
	for i := 0; i < b.maxWorkers; i++ {
		worker := Worker{
			workerPool:  b.workerPool,
			taskChannel: make(chan Task),
			stopChannel: utils.NewStopChannel(),
		}
		worker.InitLog("worker").SetPrefix(b.Identifier())
		worker.start(b.executeTask)
		b.workers[i] = worker
	}

	b.Log("Blacksmith started.")
	go b.dispatch()

	return b
}

// Stop shuts down the Blacksmith which will wait for all workers to complete
func (b *Blacksmith) Stop() *Blacksmith {
	b.LogUsing(GetLogger().Trace, "Received request to stop")
	b.stopChannel.RequestStop()
	b.Log("Blacksmith stopped.")
	return b
}

// QueueTask adds a new task to the work queue
func (b *Blacksmith) QueueTask(taskName TaskName, payload interface{}) {
	b.taskQueue <- Task{TaskName: taskName, Payload: payload}
}

func (b *Blacksmith) dispatch() {
	for {
		select {
		case job := <-b.taskQueue:
			go func(job Task) {
				taskChannel := <-b.workerPool

				b.LogfUsing(GetLogger().Tracef, "Dispatching Task: %+v\n", job)
				taskChannel <- job
			}(job)
		case <-b.stopChannel.OnRequest:
			go func() {
				b.LogUsing(GetLogger().Trace, "Closing all workers")
				for _, worker := range b.workers {
					worker.stop()
				}
				b.LogUsing(GetLogger().Trace, "Quitting")
				b.stopChannel.Stop()
			}()
		}
	}
}

func (b *Blacksmith) executeTask(task Task) {
	if b.handlerFn != nil {
		b.handlerFn(task)
		return
	}

	t := b.handlerMap[task.TaskName]

	if t != nil {
		t(task)
	} else {
		b.LogfUsing(GetLogger().Tracef, "Cannot locate task handler for task name: %s", task.TaskName)
	}
}

func (worker Worker) start(taskHandler TaskHandler) {
	worker.LogUsing(GetLogger().Trace, "Started")
	go func() {
		for {
			worker.workerPool <- worker.taskChannel

			select {
			case task := <-worker.taskChannel:
				task.InitLog("Task").SetPrefix(worker.Identifier())
				worker.LogfUsing(GetLogger().Tracef, "Processing task %+v\n", task)
				taskHandler(task)
			case <-worker.stopChannel.OnRequest:
				worker.LogUsing(GetLogger().Trace, "Quitting")
				worker.stopChannel.Stop()
				return
			}
		}
	}()
}

func (worker Worker) stop() {
	worker.stopChannel.RequestStop()
}
