package go_es

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	esv6 "github.com/elastic/go-elasticsearch/v6"
	esv7 "github.com/elastic/go-elasticsearch/v7"
	"net/http"
	"strings"
)

type ESClient interface {
	ClusterStatus() (string, error)
}

func GetElasticClient(username, password, esVersion, url string) (ESClient, error) {

	switch {
	// for Elasticsearch 6.x.x
	case strings.HasPrefix(esVersion, "6."):
		client, err := esv6.NewClient(esv6.Config{
			Addresses:         []string{url},
			Username:          username,
			Password:          password,
			EnableDebugLogger: true,
			DisableRetry:      true,
			Transport: &http.Transport{
				IdleConnTimeout: 1 * time.Millisecond,
				DialContext: (&net.Dialer{
					Timeout: 30 * time.Second,
				}).DialContext,
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
					MaxVersion:         tls.VersionTLS12,
				},
			},
		})
		if err != nil {

			return nil, err
		}
		// do a manual health check to test client
		res, err := client.Cluster.Health(
			client.Cluster.Health.WithPretty(),
		)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		if res.IsError() {
			return nil, fmt.Errorf("health check failed with status code: %d", res.StatusCode)
		}
		return &ESClientV6{client: client}, nil

	// for Elasticsearch 7.x.x
	case strings.HasPrefix(esVersion, "7."):
		client, err := esv7.NewClient(esv7.Config{
			Addresses:         []string{url},
			Username:          username,
			Password:          password,
			EnableDebugLogger: true,
			DisableRetry:      true,

			Transport: &http.Transport{
				IdleConnTimeout: 1 * time.Millisecond,
				DialContext: (&net.Dialer{
					Timeout: 30 * time.Second,
				}).DialContext,
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: true,
					MaxVersion:         tls.VersionTLS12,
				},
			},
		})
		if err != nil {
			return nil, err
		}
		// do a manual health check to test client
		res, err := client.Cluster.Health(
			client.Cluster.Health.WithPretty(),
		)
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()

		if res.IsError() {
			return nil, fmt.Errorf("health check failed with status code: %d", res.StatusCode)
		}
		return &ESClientV7{client: client}, nil
	}

	return nil, fmt.Errorf("unknown database verseion: %s", esVersion)
}
