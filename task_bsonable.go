package maelstrom

import (
	"gopkg.in/mgo.v2/bson"
	"time"
	"fmt"
)

type TaskBodyUnmarshalF func(raw bson.Raw) (body interface{}, err error)

var taskBodyUnmarshalMap map[string]TaskBodyUnmarshalF

func SetTaskBodyUnmarshalMap(m map[string]TaskBodyUnmarshalF) {
	taskBodyUnmarshalMap = m
}

func RegisterTaskBodyUnmarshalF(taskType string, unmarshalFunc TaskBodyUnmarshalF) {
	taskBodyUnmarshalMap[taskType] = unmarshalFunc
}

// !!WARNING!! The declaration of this struct MUST EXACTLY MATCH the declaration of 'Task' struct
// (see file 'task.go'). Additionally, Task.SetBSON() function below MUST COMPLETELY REFLECT this struct.
// Failing this requirement will lead to tasks not properly loading from MongoDB!
type taskBsonUnmarshalT struct {
	// this struct MUST be IDENTICAL to the previous one, except for Body field, which must be of type bson.Raw
	Id        bson.ObjectId       `bson:"_id"`
	Client    bson.ObjectId       `bson:"Client"`
	RequestId *bson.ObjectId      `bson:"RequestId"`
	Depends   []bson.ObjectId     `bson:"Depends"`
	Locks     []bson.ObjectId     `bson:"Locks"`
	Type      string              `bson:"Type"`
	Body      bson.Raw            `bson:"Body"`
	CreatedAt time.Time           `bson:"CreatedAt"`
	Status    TaskStatus          `bson:"Status"`
	Attempts  int                 `bson:"Attempts"`
	LastRunAt *time.Time          `bson:"LastRunAt"`
	NextRunAt *time.Time          `bson:"NextRunAt"`
	Log       []TaskHistoryRecord `bson:"Log"`
}

func (t *Task) SetBSON(raw bson.Raw) (err error) {
	var (
		tt   taskBsonUnmarshalT
		body interface{}
	)

	if err = raw.Unmarshal(&tt); err != nil {
		return
	}

	if f, ok := taskBodyUnmarshalMap[tt.Type]; ok {
		body, err = f(tt.Body)
	} else {
		err = fmt.Errorf("Cannot unmarshal BSON data for Task: unknown task type '%s'.", tt.Type)
	}

	if err != nil {
		return
	}

	*t = Task{ // this must reflect the entire Task struct; all fields are copied "as is" except for Body field
		Id:        tt.Id,
		Client:    tt.Client,
		RequestId: tt.RequestId,
		Depends:   tt.Depends,
		Locks:     tt.Locks,
		Type:      tt.Type,
		Body:      body,
		CreatedAt: tt.CreatedAt,
		Status:    tt.Status,
		Attempts:  tt.Attempts,
		LastRunAt: tt.LastRunAt,
		NextRunAt: tt.NextRunAt,
		Log:       tt.Log,
	}

	return
}
