package publish_log

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type dataConfig struct {
	projectID string
	topicID	  string
	credentialFileJson []byte
}

func PublishLog(ctx context.Context, data map[string]interface{}, config []byte) error {
	
	var cfg dataConfig
	json.Unmarshal(config, &cfg)

	d, err := json.Marshal(data)
	if err != nil {
		log.Fatal("error when marshal data :", err)
	}

	client, errClient := pubsub.NewClient(ctx, cfg.projectID, option.WithCredentialsJSON(cfg.credentialFileJson))
	if err != nil {
		return fmt.Errorf("logger pubsub: NewClient: %v", errClient)
	}
	defer client.Close()

	t := client.Topic(cfg.topicID)
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(d),
	})
	
	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Errorf("pubsub: result.Get: %v", err)
	}

	fmt.Printf("Published a message; msg ID: %v\n", id)
	return nil
}