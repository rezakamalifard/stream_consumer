package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/ilyakaznacheev/cleanenv"
	"time"
)

type RedisConfig struct {
	Addr     string `env:"ZEBEL_ADDR" env-default:"localhost:6379"`
	Password string `env:"ZEBEL_PASSWORD" env-default:""`
	DB       int    `env:"NAME" env-default:"0"`
}

func main() {
	client, _, _ := NewClient()


	streams := []string{"mystream", ">"}

	xreadgroupargs := &redis.XReadGroupArgs{
		Group:    "mygroup",
		Consumer: "Alice1",
		Streams:  streams,
		Count:    200,
		Block:    2000,
	}

	for {
		readMessages, err := client.XReadGroup(xreadgroupargs).Result()
		if err != nil {
			panic(err)
		}
		for _, readMessage := range readMessages {
			streamMessagesCount := int64(len(readMessage.Messages))
			fmt.Println(streamMessagesCount)
			for i := range readMessage.Messages {
				message := &readMessage.Messages[i]
				fmt.Println(message.Values["ip"])
				// go do this

				// add ack
				resultAck, err := client.XAck("mystream", "mygroup", message.ID).Result()
				if err != nil {
					panic(err)
				}
				fmt.Println(resultAck)
			}
		}
	}

}
func NewClient() (*redis.Client, string, error) {
	var redisConf RedisConfig
	err := cleanenv.ReadEnv(&redisConf)
	if err != nil {
		panic(err)
	}
	client := redis.NewClient(&redis.Options{
		Addr:         redisConf.Addr,
		Password:     redisConf.Password,
		DB:           redisConf.DB,
		MinIdleConns: 100,
		ReadTimeout:  3* time.Minute,
		DialTimeout:  2 * time.Minute,
		IdleTimeout: 4 * time.Minute,
		PoolSize: 400,
	})
	pong, err := client.Ping().Result()

	if err != nil {
		panic(err)
	}

	return client, pong, err
}
