package main

import (
	//"context"
	"database/sql"
	"flag"
	"fmt"

	_ "github.com/lib/pq"

	//"strings"
	//"cloud.google.com/go/spanner"
	"github.com/golang/glog"
	//"github.com/google/hashr/core/hashr"
	//"github.com/google/hashr/storage/cloudspanner"
	postgresClient "github.com/google/hashr/client/postgres"
	//"golang.org/x/oauth2/google"
	//"google.golang.org/api/cloudbuild/v1"
	//"google.golang.org/api/compute/v1"
	//"google.golang.org/api/storage/v1"
)

var (
	hashStorage   = flag.String("hashStorage", "", "Storage used for computed hashes, can have one of the two values: postgres, cloudspanner")
	cacheDir      = flag.String("cache_dir", "/tmp/", "Path to cache dir used to store local cache.")
	spannerDBPath = flag.String("spanner_db_path", "", "Path to spanner DB.")

	// Postgres DB flags
	postgresHost     = flag.String("postgres_host", "localhost", "PostgreSQL instance address.")
	postgresPort     = flag.Int("postgres_port", 5432, "PostgresSQL instance port.")
	postgresUser     = flag.String("postgres_user", "hashr", "PostgresSQL user.")
	postgresPassword = flag.String("postgres_password", "hashr", "PostgresSQL password.")
	postgresDBName   = flag.String("postgres_db", "hashr", "PostgresSQL database.")

	// Mode of retrieval
	get = flag.String("get_sample", "", "Sample to retrieve based on sha256 hash")
)

func main() {
	flag.Parse()
	var client *postgresClient.Client
	switch *hashStorage {
	case postgresClient.Name:
		psqlInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			*postgresHost, *postgresPort, *postgresUser, *postgresPassword, *postgresDBName)

		db, err := sql.Open("postgres", psqlInfo)
		if err != nil {
			glog.Exitf("Error initializing Postgres client: %v", err)
		}
		defer db.Close()

		client, err = postgresClient.NewClient(db)
		if err != nil {
			glog.Exitf("Error initializing Postgres storage: %v", err)
		}
	default:
		glog.Exit("hashStorage flag needs to have one of the two values: postgres, cloudspanner")

	}
	if *get == "" {
		samples, err := client.GetSamples()
		if err != nil {
			glog.Exitf("Error retriving samples: %v", err)
		}
		fmt.Println(samples)
	} else {

		sample, err := client.GetSample(*get)
		if err != nil {
			glog.Exitf("Error retriving samples: %v", err)
		}
		fmt.Println(*sample)
	}
}
