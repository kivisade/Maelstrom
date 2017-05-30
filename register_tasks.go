package maelstrom

import (
	"log"
	"time"
)

type TaskRegistratorF func() (tasksCount int)

var taskRegistratorMap map[string]TaskRegistratorF

func SetTaskRegistratorMap(m map[string]TaskRegistratorF) {
	taskRegistratorMap = m
}

func RegisterTasks() {
	var (
		start      time.Time = time.Now()
		total, cnt int
	)

	log.Println("RegisterTasks started.")

	for taskType, regFn := range taskRegistratorMap {
		cnt = regFn()
		if cnt > 0 {
			log.Printf("Registered %d new '%s' (or related) tasks.", cnt, taskType)
		}
		total += cnt
	}

	runningTime := time.Since(start)
	if total > 0 {
		log.Printf("RegisterTasks processed %d tasks in %v.\n", total, runningTime)
	} else {
		log.Printf("RegisterTasks had nothing to do for %v.\n", runningTime)
	}
}
