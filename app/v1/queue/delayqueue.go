package queue

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	rdbClient "github.com/qtyty/delay-mq/v1/internal/redis"
	"time"
	"unsafe"
)

type Queue struct {
	Topic    string
	RedisCli *rdbClient.RedisClient
	Handler  func(string)

	PendingKey string //ZSet
	ReadyKey   string //List

	FetchInterval time.Duration
	FetchLimit    int

	Close chan struct{}
}

type Job struct {
	Id        string        `json:"id"`
	Name      string        `json:"name"`
	Payload   string        `json:"payload"`
	DelayTime time.Time     `json:"delayTime"`
	TTL       time.Duration `json:"ttl"`
}

func NewJob(id, name, payload string, t time.Time) *Job {
	return &Job{
		Id:        id,
		Name:      name,
		Payload:   payload,
		DelayTime: t,
		TTL:       5 * time.Minute,
	}
}

func (job *Job) StructToString() (string, error) {
	bytes, err := json.Marshal(job)
	return *(*string)(unsafe.Pointer(&bytes)), err
}

func (job *Job) StringToStruct(str string) (*Job, error) {
	j := &Job{}
	bytes := *(*[]byte)(unsafe.Pointer(&str))
	err := json.Unmarshal(bytes, j)
	return j, err
}

func NewQueue(topic string, callback func(string)) *Queue {
	return &Queue{
		Topic:    topic,
		RedisCli: rdbClient.NewRedisClient(),
		Handler:  callback,

		PendingKey: topic + "_pending",
		ReadyKey:   topic + "_ready",

		FetchInterval: 5 * time.Second,
		FetchLimit:    10,

		Close: make(chan struct{}),
	}
}

func (q *Queue) WithFetchInterval(d time.Duration) *Queue {
	q.FetchInterval = d
	return q
}

func (q *Queue) WithFetchLimit(limit int) *Queue {
	q.FetchLimit = limit
	return q
}

func (q *Queue) SendScheduleJob(jobName string, payload string, t time.Time) error {
	id := uuid.Must(uuid.NewRandom()).String()

	job := NewJob(id, jobName, payload, t)

	jobStr, err := job.StructToString()
	if err != nil {
		return fmt.Errorf("failed to convert job to string : %v, %+v", err, job)
	}
	jobTTL := job.DelayTime.Sub(time.Now()) + job.TTL
	// set job to redis
	err = q.RedisCli.Set(id, jobStr, jobTTL)
	if err != nil {
		return fmt.Errorf("failed to set job to redis: %v, %+v", err, job)
	}

	// set job to zset
	err = q.RedisCli.ZAdd(q.PendingKey, job.Id, job.DelayTime.Unix())
	if err != nil {
		return fmt.Errorf("failed to set job to zset: %v, %+v", err, job)
	}
	return nil
}

func (q *Queue) SendDelayJob(jobName string, payload string, d time.Duration) error {
	return q.SendScheduleJob(jobName, payload, time.Now().Add(d))
}
