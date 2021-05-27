package main

import (
	"fmt"
	go_es "github.com/kamolhasan/go-elasticsearch-test/go-es"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var urlList, usernameList, passwordList []string

func main() {
	urls := os.Getenv("ES_URLS")
	usernames := os.Getenv("ES_USERNAMES")
	passwords := os.Getenv("ES_PASSWORDS")
	usernameList = strings.Split(usernames, ",")
	passwordList = strings.Split(passwords, ",")
	urlList = strings.Split(urls, ",")

	fmt.Println(usernameList)
	fmt.Println(passwordList)
	fmt.Println(urlList, len(urlList))

	termChan := make(chan os.Signal)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	go CheckElasticsearchHealth(termChan)

	<-termChan

	log.Println("Shutting down.....!")
}

func CheckElasticsearchHealth(stopCh <-chan os.Signal) {
	log.Println("Starting Elasticsearch health checker...")
	for {
		select {
		case <-stopCh:
			log.Println("Shutting down Elasticsearch health checker...")
			return
		default:
			CheckElasticsearchHealthOnce()
			time.Sleep(10 * time.Second)
		}
	}
}

func CheckElasticsearchHealthOnce() {

	var wg sync.WaitGroup
	fmt.Println("urlLIST:", urlList)
	for idx, url := range urlList {
		fmt.Println("idx", idx, "url", url)

		wg.Add(1)
		go func(username, password, url string) {
			defer func() {
				wg.Done()
			}()

			fmt.Println("FOR: ", idx, username, password, "7.x.x", url)

			// Create database client
			dbClient, err := go_es.GetElasticClient(username, password, "7.x.x", url)
			if err != nil {
				log.Println("Error: ", err)
				return
			}

			// Get database status, could be red, green or yellow.
			status, err := dbClient.ClusterStatus()
			if err != nil {
				log.Println("Error: ", err)
				return
			}

			log.Println(url, " ----> Status: ", status)

		}(usernameList[idx], passwordList[idx], url)
	}

	// Wait until all go-routine complete executions
	wg.Wait()
	fmt.Println("WaitGroup Done..................")
}
