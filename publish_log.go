package publish_log

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

type dataConfig struct {
	ProjectID string
	TopicID	  string
	CredentialFileJson []byte
}

func PublishLog(ctx context.Context, data map[string]interface{}, config []byte) error {
	
	var cfg dataConfig
	json.Unmarshal(config, &cfg)

	d, err := json.Marshal(data)
	if err != nil {
		fmt.Printf("error marshal :", err)
	}

	client, err := pubsub.NewClient(ctx, cfg.ProjectID, option.WithCredentialsJSON(cfg.CredentialFileJson))
	if err != nil {
		return fmt.Errorf("pubsub: NewClient: %v", err)
	}
	defer client.Close()

	t := client.Topic(cfg.TopicID)
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