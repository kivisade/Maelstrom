package maelstrom

import (
	"gopkg.in/mgo.v2/bson"
)

type MongoTask Task

func (t *MongoTask) Save() {
	mgoTasks.UpsertId(t.Id, t)
}

func (t *MongoTask) SaveStatus(status TaskStatus) {
	t.Status = status
	mgoTasks.UpdateId(t.Id, bson.M{`$set`: bson.M{`Status`: status}})
}
