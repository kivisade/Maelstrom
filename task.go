package maelstrom

import (
	"gopkg.in/mgo.v2/bson"
	"time"
	"bytes"
	"fmt"
	"utils"
	"log"
	"mongo/lock"
	"common/types"
)

type TaskStatus int
type TaskDependStatus int

const (
	TASK_STATUS_CANCELLED TaskStatus = -2
	TASK_STATUS_FAILED    TaskStatus = -1
	TASK_STATUS_PENDING   TaskStatus = 0
	TASK_STATUS_COMPLETE  TaskStatus = 1
	TASK_STATUS_PAUSED    TaskStatus = 2
	TASK_STATUS_RUNNING   TaskStatus = 3

	TASK_DEPENDS_FAILED TaskDependStatus = -1
	TASK_DEPENDS_NONE   TaskDependStatus = 0
	TASK_DEPENDS_CLEAR  TaskDependStatus = 1
	TASK_DEPENDS_WAIT   TaskDependStatus = 2
)

//======================================================================================================================
// !!WARNING!! The declaration of this struct MUST EXACTLY MATCH the declaration of 'taskBsonUnmarshalT' struct
// (see file 'task_bsonable.go'). Additionally, Task.SetBSON() function MUST COMPLETELY REFLECT this struct.
// Failing this requirement will lead to tasks not properly loading from MongoDB!
type Task struct {
	Id        bson.ObjectId       `bson:"_id"`       // Уникальный идентификатор задания
	Client    bson.ObjectId       `bson:"Client"`    // Идентификатор клиента SRM, для которого выполняется это задание (т.е., того клиента, с чьими объектами оно работает)
	RequestId *bson.ObjectId      `bson:"RequestId"` // Идентификатор запроса (входящего сообщения), на основании которого была создана эта задача (необязательное поле)
	Depends   []bson.ObjectId     `bson:"Depends"`   // Список зависимостей (идентификаторов задач, от которых зависит данная задача)
	Locks     []bson.ObjectId     `bson:"Locks"`     // Список блокировок (могут использоваться идентификаторы чего угодно; на время выполнения данной задачи будут установлены блокировки с такими именами)
	Type      string              `bson:"Type"`      // Тип задачи (символьное имя)
	Body      interface{}         `bson:"Body"`      // Параметры задания: информация, необходимая для работы функции-обработчика этого типа заданий (структура зависит от типа задания)
	CreatedAt time.Time           `bson:"CreatedAt"` // Дата и время создания задания
	Status    TaskStatus          `bson:"Status"`    // Текущий статус задания (см. список статусов выше)
	Attempts  int                 `bson:"Attempts"`  // Количество уже совершённых попыток выполнения задания на текущий момент (напр., для заданий, выполненных с первой попытки, == 1)
	LastRunAt *time.Time          `bson:"LastRunAt"` // Дата и время последнего (на текущий момент) запуска задания (попытки выполнения)
	NextRunAt *time.Time          `bson:"NextRunAt"` // Дата и время следующей попытки (для завершённых и окончательно проваленных заданий сбрасывается в nil)
	Log       []TaskHistoryRecord `bson:"Log"`       // Полная история запусков задания (попыток выполнения)
}

func NewTask(client bson.ObjectId, taskType string, body interface{}) (t *Task) {
	t = &Task{
		Id:        bson.NewObjectId(),
		Client:    client,
		Type:      taskType,
		Body:      body,
		CreatedAt: time.Now(),
		NextRunAt: utils.NowPtr(), // schedule new task for immediate execution (first attempt as soon as possible)
	}
	log.Println("Created new task:", t)
	return
}

func (t Task) String() string {
	return fmt.Sprintf("%s (%s)", t.Id.Hex(), t.Type)
}

func (t *Task) NewDependentTask(taskType string, body interface{}) (depend *Task) {
	depend = NewTask(t.Client, taskType, body)
	t.Depends = append(t.Depends, depend.Id)
	return
}

func (t *Task) SetRequestId(requestId bson.ObjectId) *Task {
	t.RequestId = &requestId
	return t
}

func (t *Task) AddLock(lockId bson.ObjectId) *Task {
	t.Locks = append(t.Locks, lockId)
	return t
}

func (t Task) GetAllLocks() (result error) {
	var obtained []bool = make([]bool, len(t.Locks))
	for i, lockId := range t.Locks {
		obtained[i] = lock.GetLock(lockId.Hex(), 3*time.Second)
		if !obtained[i] {
			result = fmt.Errorf("Failed to obtain MongoDB lock %s.", lockId.Hex())
			break
		} else {
			log.Printf("Obtained lock %s for task %s.", lockId.Hex(), t)
		}
	}
	if result != nil {
		for i, release := range obtained {
			if release {
				lock.ReleaseLock(t.Locks[i].Hex()) // TODO check errors here!
			}
		}
	}
	return
}

func (t Task) ReleaseAllLocks() error {
	var muerr = types.NewMultiError()
	for _, lockId := range t.Locks {
		if err := lock.ReleaseLock(lockId.Hex()); err != nil {
			muerr.Add(types.TraceError(fmt.Errorf("Failed to release MongoDB lock %s.", lockId.Hex()), err))
		} else {
			log.Printf("Released lock %s for task %s.", lockId.Hex(), t)
		}
	}
	return muerr.Return()
}

//======================================================================================================================
type TaskHistoryRecord struct {
	StartedAt  time.Time `bson:"StartedAt"`
	FinishedAt time.Time `bson:"FinishedAt"`
	Error      *string   `bson:"Error"`
}

func (r *TaskHistoryRecord) Start() {
	r.StartedAt = time.Now()
}

func (r *TaskHistoryRecord) Finish(err error) {
	r.FinishedAt = time.Now()
	if err != nil {
		r.Error = utils.NewStringPtr(err.Error())
	}
}

func (r TaskHistoryRecord) RunningTime() time.Duration {
	return r.FinishedAt.Sub(r.StartedAt)
}

//======================================================================================================================
type TaskList struct {
	tasks []*Task
}

func (tl *TaskList) Add(task *Task) {
	tl.tasks = append(tl.tasks, task)
}

func (tl *TaskList) AddCopy(task Task) {
	tl.tasks = append(tl.tasks, &task)
}

func (tl TaskList) Count() int {
	return len(tl.tasks)
}

func (tl TaskList) SliceOfInterfaces() (s []interface{}) {
	s = make([]interface{}, len(tl.tasks))
	for i, task := range tl.tasks {
		s[i] = task
	}
	return
}

func (tl TaskList) String() string {
	buf := new(bytes.Buffer)
	for _, task := range tl.tasks {
		fmt.Fprintf(buf, " - %s\n", task)
	}
	return buf.String()
}
