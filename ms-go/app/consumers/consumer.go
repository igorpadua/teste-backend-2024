package consumers

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"ms-go/app/models"
	"ms-go/app/services/products"
	"time"
)

func ConsumerRails() {
	topic := "rails-to-go"
	brokeAddress := "localhost:9092"
	groupID := "rails-consumer-group"

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokeAddress},
		Topic:          topic,
		CommitInterval: time.Second,
		GroupID:        groupID,
	})

	defer func() {
		if err := reader.Close(); err != nil {
			panic(err)
		}
	}()

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			panic(err)
		}
		var product models.Product

		if err := json.Unmarshal(m.Value, &product); err != nil {
			panic(err)
		}

		_, err = products.Details(models.Product{ID: product.ID})

		if err != nil {
			_, err = products.Create(product, false)
		} else {
			_, err = products.Update(product, false)
		}
	}
}
