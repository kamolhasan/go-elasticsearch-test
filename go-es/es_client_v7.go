package go_es

import (
	"context"
	"encoding/json"

	esv7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/pkg/errors"
)

type ESClientV7 struct {
	client *esv7.Client
}

func (es *ESClientV7) ClusterStatus() (string, error) {
	ctx, cncl := context.WithCancel(context.Background())
	defer cncl()
	res, err := es.client.Cluster.Health(
		es.client.Cluster.Health.WithPretty(),
		es.client.Cluster.Health.WithContext(ctx),
	)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	response := make(map[string]interface{})
	if err2 := json.NewDecoder(res.Body).Decode(&response); err2 != nil {
		return "", errors.Wrap(err2, "failed to parse the response body")
	}
	if value, ok := response["status"]; ok {
		if strValue, ok := value.(string); ok {
			return strValue, nil
		}
		return "", errors.New("failed to convert response to string")
	}
	return "", errors.New("status is missing")
}
