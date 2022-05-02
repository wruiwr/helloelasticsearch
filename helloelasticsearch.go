package main

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	es "gopkg.in/olivere/elastic.v2"
)

// Tweet is a structure used for serializing/deserializing data in Elasticsearch.
type Tweet struct {
	User     string                `json:"user"`
	Message  string                `json:"message"`
	Retweets int                   `json:"retweets"`
	Image    string                `json:"image,omitempty"`
	Created  time.Time             `json:"created,omitempty"`
	Tags     []string              `json:"tags,omitempty"`
	Location string                `json:"location,omitempty"`
	Suggest  *es.SuggestField 	   `json:"suggest_field,omitempty"`
}

const mapping = `
{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"tweet":{
			"properties":{
				"user":{
					"type":"string"
				},
				"message":{
					"type":"string",
					"store": true
				},
				"image":{
					"type":"string"
				},
				"created":{
					"type":"date"
				},
				"tags":{
					"type":"string"
				},
				"location":{
					"type":"geo_point"
				},
				"suggest_field":{
					"type":"completion"
				}
			}
		}
	}
}`

func main() {
	// Obtain a client and connect to the default Elasticsearch installation
	// on 127.0.0.1:9200. Of course you can configure your client to connect
	// to other hosts and configure it in various other ways.
	client, err := es.NewClient()	
	if err != nil {
		// Handle error
		log.Panicf("error: %v, when creating a new client.\n", err)
	}

	// Ping the Elasticsearch server to get e.g. the version number.
	info, code, err := client.Ping().Do()
	if err != nil {
		// Handle error
		log.Panicf("error: %v, when pinging a new client.\n", err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	// Getting the ES version number.
	esversion, err := client.ElasticsearchVersion("http://127.0.0.1:9200")
	if err != nil {
		// Handle error
		log.Panicf("error: %v, when getting ES version number.\n", err)
	}
	fmt.Printf("Elasticsearch version %s\n", esversion)

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists("twitter").Do()
	if err != nil {
		// Handle error
		log.Panicf("error: %v, when checking a specified index.\n", err)
	}
	if exists {
		fmt.Println("the twitter index already exists. Deleting it now.")
		// Delete the index
		deleteIndex, err := client.DeleteIndex("twitter").Do()
		if err != nil {
		    // Handle error
			log.Panicf("error: %v, when deleting a specified index.\n", err)
		}
		if !deleteIndex.Acknowledged {
			// Not acknowledged
			fmt.Println("Not acknowledged")
		}
	}

	// Create a new index.
	createIndex, err := client.CreateIndex("twitter").BodyString(mapping).Do()
	if err != nil {
    	// Handle error
		log.Panicf("error: %v, when creating a specified index.\n", err)
	}
	if !createIndex.Acknowledged {
		// Not acknowledged
		fmt.Println("Not acknowledged")
	}

	// Use the IndexExists service to check if a specified index exists.
	exists, err = client.IndexExists("twitter").Do()
	if err != nil {
    	// Handle error
		log.Panicf("error: %v, when checking a specified index.\n", err)
	}
	if !exists {
		fmt.Println("Index twitter does no exist.")
	}

	// Index a tweet using JSON serialization
	tweet1 := Tweet{User: "olivere", Message: "Take Five", Retweets: 0}
	put1, err := client.Index().
	    Index("twitter").
	    Type("tweet").
	    Id("1").
	    BodyJson(tweet1).
	    Do()
	if err != nil {
	    // Handle error
		log.Panicf("error: %v, when indexing a tweet.\n", err)
	}
	fmt.Printf("Indexed tweet %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)

	// Index a second tweet (by string)
	tweet2 := `{"user" : "olivere", "message" : "It's a Raggy Waltz"}`
	put2, err := client.Index().
		Index("twitter").
		Type("tweet").
		Id("2").
		BodyString(tweet2).
		Do()
	if err != nil {
		// Handle error
		log.Panicf("error: %v, when indexing a tweet.\n", err)
	}
	fmt.Printf("Indexed tweet %s to index %s, type %s\n", put2.Id, put2.Index, put2.Type)


	// GET tweet with specified ID
	get, err := client.Get().Index("twitter").Type("tweet").Id("1").Do()
	if err != nil {
		// Handle error
		log.Panicf("error: %v, when getting a tweet.\n", err)
	}

	if get.Found {
		fmt.Printf("Got document %s in version %d from index %s, type %s\n", get.Id, get.Version, get.Index, get.Type)
		var t Tweet
		err := json.Unmarshal(*get.Source, &t)
		if err != nil {
			// Handle error
			log.Panicf("error: %v, when decoding json.\n", err)
		}
		// Print info with tweet
		fmt.Printf("Tweet by %s: %s\n", t.User, t.Message)
	}

	// Flush to make sure the documents got written.
	// In order to perform search, we must do flush.
	_, err = client.Flush().Index("twitter").Do()
	if err != nil {
		log.Panicf("error: %v, when flushing index.\n", err)
	}

	// Search with a term query.
	termQuery := es.NewTermQuery("user", "olivere")
	searchResult, err := client.Search().
	    Index("twitter").   // search in index "twitter"
		Query(termQuery).   // specify the query
	    Sort("user", true). // sort by "user" field, ascending
	    From(0).Size(10).   // take documents 0-9
	    Pretty(true).       // pretty print request and response JSON
	    Do()                // execute
	// searchResult, err := client.Search().Index("twitter").Type("tweet").Query(es.NewTermQuery("user", "olivere")).Do()
	// searchResult, err := client.Search().Index("twitter").Query(es.NewMatchAllQuery()).Do()
	if err != nil {
	    // Handle error
		log.Panicf("error: %v, when searching.\n", err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	fmt.Printf("Query took %d milliseconds\n", searchResult.TookInMillis)

	// Each is a convenience function that iterates over hits in a search result.
	// It makes sure you don't need to check for nil values in the response.
	// However, it ignores errors in serialization.
	var ttyp Tweet
	for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
	    if t, ok := item.(Tweet); ok {
	        fmt.Printf("Tweet by %s: %s\n", t.User, t.Message)
	    }
	}
	// TotalHits is another convenience function that works even when something goes wrong.
	fmt.Printf("Found a total of %d tweets\n", searchResult.TotalHits())
	fmt.Printf("Search result total hits %v\n", searchResult.Hits.TotalHits)
	
	// Here's how you iterate through the search results with full control over each step.
	if searchResult.Hits.TotalHits > 0 {
		fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)

		// Iterate through results
    	for _, hit := range searchResult.Hits.Hits {
    	    // hit.Index contains the name of the index

    	    // Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
    	    var t Tweet
    	    err := json.Unmarshal(*hit.Source, &t)
    	    if err != nil {
    	        // Deserialization failed
				log.Panicf("error: %v, when decoding json.\n", err)
    	    }

    	    // print info with tweet
    	    fmt.Printf("Tweet by %s: %s\n", t.User, t.Message)
    	}
	} else {
		// No hits
		fmt.Println("Found no tweets.")
	}

	// Update a tweet by the update API of Elasticsearch.
	// We just increment the number of retweets.
	// If the document exists, then the script will be executed.
	// If the document does not exist, the contents of the upsert element will be inserted as a new document
	update, err := client.Update().Index("twitter").Type("tweet").Id("1").
		Script("ctx._source.retweets += num").
		ScriptParams(map[string]interface{}{"num": 1}). 
		Upsert(map[string]interface{}{"retweets": 0}).
		Do()
	if err != nil {
		// Handle error
		log.Panicf("error: %v, when updating a tweet.\n", err)
		panic(err)
	}
	fmt.Printf("New version of tweet %q is now %d", update.Id, update.Version)
}
