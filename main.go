package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var Brokers = []string{"kafka:9092"}

func main() {
	if len(os.Args) == 1 {
		fmt.Fprintln(os.Stderr, "Specify filter; format: <topic>:<partition>:<offset>, only topic is required")
		os.Exit(1)
	}

	filters := strings.SplitN(os.Args[1], ":", 3)

	topic := filters[0]
	var partition int32 = -1
	var offset int64 = -1

	if len(filters) >= 2 {
		i64, err := strconv.ParseInt(filters[1], 10, 32)
		check(err)
		partition = int32(i64)
	}

	if len(filters) >= 3 {
		var err error
		offset, err = strconv.ParseInt(filters[2], 10, 64)
		check(err)
	}

	logger := log.New(os.Stderr, "", log.LstdFlags)
	logger.SetOutput(os.Stderr)
	// sarama.Logger = logger

	cfg := sarama.NewConfig()
	cfg.ClientID = "krm"
	cfg.Consumer.Return.Errors = true

	client, err := sarama.NewClient(Brokers, cfg)
	check(err)
	defer client.Close()

	partitions, err := client.Partitions(topic)
	check(err)

	rand.Seed(time.Now().UTC().UnixNano())

	if partition < 0 {
		partition = partitions[rand.Intn(len(partitions))]
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	check(err)
	defer consumer.Close()

	offsetNewest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
	check(err)

	offsetOldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	check(err)

	if offsetNewest > offsetOldest {
		if offset < 0 {
			offset = offsetOldest + rand.Int63n(offsetNewest-offsetOldest)
		}

		partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
		check(err)
		defer partitionConsumer.Close()

		msg := <-partitionConsumer.Messages()
		fmt.Fprintf(os.Stderr, "Picked: %s:%d:%d -- key: %s\n", topic, partition, msg.Offset, msg.Key)
		if msg.Value == nil {
			fmt.Print("null")
		} else {
			os.Stdout.Write(msg.Value)
		}
	} else {
		log.Println("WARN: Topic Empty")
	}
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
