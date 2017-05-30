package maelstrom

import (
	"log"
	"utils"
	"fmt"
	"time"
)

type TaskHandlerF func (task *Task) (workerError error, fatal bool)

var taskHandlerMap map[string]TaskHandlerF

func SetTaskHandlerMap(m map[string]TaskHandlerF) {
	taskHandlerMap = m
}

type TaskFatalHandlerF func (task *Task, taskError error) error

var taskFatalHandlerMap map[string]TaskFatalHandlerF

func SetTaskFatalHandlerMap(m map[string]TaskFatalHandlerF) {
	taskFatalHandlerMap = m
}

func NextRunTime(lastRunTime time.Time, attempts int) time.Time {
	return lastRunTime.Add(time.Minute * time.Duration(1<<(uint(attempts)+1)))
}

func Worker(t interface{}) interface{} {
	var (
		taskError error
		fatal     bool
		logEntry  TaskHistoryRecord
	)

	task, ok := t.(Task)
	if !ok {
		log.Println("Failed to run task: call parameter passed to Worker is not a Task struct.")
		return nil
	}

	if task.Status != TASK_STATUS_PENDING { // this should never happen except for a very unlikely concurrency collision
		log.Printf("Failed to run task %s: invalid state (%d).\n", task, task.Status)
		return false
	}

	if taskError = task.GetAllLocks(); taskError != nil {
		log.Printf("Failed to obtain required locks for task %d.\n * %s", task, taskError)
		return false
	}

	log.Printf("Worker starting task %s.\n", task)

	(*MongoTask)(&task).SaveStatus(TASK_STATUS_RUNNING)
	logEntry.Start() // logEntry.StartedAt gets initialized here, so this MUST go earlier than next line
	task.LastRunAt = utils.NewTimePtr(logEntry.StartedAt)

	if taskHandler, found := taskHandlerMap[task.Type]; found {
		taskError, fatal = taskHandler(&task)
	} else {
		taskError, fatal = fmt.Errorf("Failed to run task %s: unknown task type.", task), true
	}

	logEntry.Finish(taskError)
	task.Attempts++
	task.Log = append(task.Log, logEntry)

	if taskError == nil {
		log.Printf("Task %s complete after %d attempt(s).", task, task.Attempts)
		task.Status, task.NextRunAt = TASK_STATUS_COMPLETE, nil
	} else {
		log.Printf("Task %s, attempt %d failed:\n * %s", task, task.Attempts, taskError)
		if fatal || task.Attempts >= maxAttempts {
			if fatal {
				log.Println("This error is permanent, task cannot be completed.")
			} else {
				log.Println("Maximum number of attempts reached, task cannot be completed.")
			}
			task.Status, task.NextRunAt = TASK_STATUS_FAILED, nil
			if taskFatalHandler, found := taskFatalHandlerMap[task.Type]; found {
				if postMortem := taskFatalHandler(&task, taskError); postMortem != nil { // perform any additional work required when task entered fatal state
					log.Printf("Additional error(s) were encountered while trying to perform last will of the dying task:\n * %s", postMortem)
				}
			}
		} else {
			nextRunTime := NextRunTime(*task.LastRunAt, task.Attempts)
			log.Println("This error is temporary, will retry after", nextRunTime)
			task.Status, task.NextRunAt = TASK_STATUS_PENDING, &nextRunTime
		}
	}

	(*MongoTask)(&task).Save()

	if taskError = task.ReleaseAllLocks(); taskError != nil {
		log.Printf("Failed to release obtained locks for task %d.\n * %s", task, taskError) // TODO maybe should panic here? unreleased locks are very serious shit.
	}

	log.Printf("Worker processed task %s in %v.\n", task, logEntry.RunningTime())

	return nil
}
