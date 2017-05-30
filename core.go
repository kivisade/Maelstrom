package maelstrom

import (
	"log"
	"time"
	"gopkg.in/mgo.v2/bson"
	"app"
	"gopkg.in/mgo.v2"
)

const MONGO_COLLECTION_TASKS = "tasks"

var (
	ctx         *app.Context    = app.GetContext()
	mgoDb       *mgo.Database   = ctx.Database()
	mgoTasks    *mgo.Collection = mgoDb.C(MONGO_COLLECTION_TASKS)
	maxAttempts int             = ctx.Config.Worker.MaxProcessingAttempts
)

type sendWorkCallback func(jobData interface{}) (interface{}, error)

func RunTasks(sendWork sendWorkCallback) {
	log.Println("RunTasks started.")
	start, tasksCount := time.Now(), 0

	iter := mgoTasks.Find(bson.M{
		`Status`:    TASK_STATUS_PENDING,  // task is in a runnable state
		`Attempts`:  bson.M{`$lt`: maxAttempts}, // max attempts limit has not been reached
		`NextRunAt`: bson.M{`$lte`: time.Now()}, // the time has come (`NextRunAt` specified for this task is already in the past)
	}).Sort(`NextRunAt`, `_id`).Iter()

	task := new(Task)
	for iter.Next(task) {
		switch CheckTaskDepends(task) {
		case TASK_DEPENDS_NONE, TASK_DEPENDS_CLEAR:
			log.Printf("Sending task %s to execution queue.\n", task)
			go sendWork(*task) // cannot work with pointers here, will lead to collisions; must send a copy of data to Worker
		case TASK_DEPENDS_FAILED:
			task.Status = TASK_STATUS_FAILED
			mgoTasks.UpdateId(task.Id, task)
		}
		tasksCount++
	}

	runningTime := time.Since(start)
	if tasksCount > 0 {
		log.Printf("RunTasks processed %d tasks in %v.\n", tasksCount, runningTime)
	} else {
		log.Printf("RunTasks had nothing to do for %v.\n", runningTime)
	}
}

func CheckTaskDepends(task *Task) TaskDependStatus {
	if len(task.Depends) == 0 {
		log.Printf("Task %s has no dependencies.\n", task)
		return TASK_DEPENDS_NONE
	}

	iter := mgoTasks.Find(bson.M{`_id`: bson.M{`$in`: task.Depends}}).Iter()

	var waiting, fatal TaskList

	depend := new(Task)
	for iter.Next(depend) {
		switch depend.Status {
		case TASK_STATUS_PENDING, TASK_STATUS_PAUSED, TASK_STATUS_RUNNING:
			waiting.AddCopy(*depend) // cannot work with pointers here, will lead to collisions
		case TASK_STATUS_FAILED, TASK_STATUS_CANCELLED:
			fatal.AddCopy(*depend) // cannot work with pointers here, will lead to collisions
		}
	}

	switch {
	case fatal.Count() > 0:
		log.Printf("Task %s cannot be executed because of failed dependencies:\n%s", task, fatal)
		return TASK_DEPENDS_FAILED
	case waiting.Count() > 0:
		log.Printf("Task %s is still waiting for the following dependencies:\n%s", task, waiting)
		return TASK_DEPENDS_WAIT
	default:
		log.Printf("All dependencies for task %s are clear.\n", task)
		return TASK_DEPENDS_CLEAR
	}
}
