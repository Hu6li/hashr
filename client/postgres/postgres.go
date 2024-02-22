// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package postgres provides functions required to export data to PostgreSQL.
package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

const (
	// Name contains name of the client.
	Name = "postgres"
)

// Client is an instance of Postgres Client.
type Client struct {
	sqlDB *sql.DB
}

// Sample representing samples as stored in DB.
type Sample struct {
	sha256      string
	mimetype    string
	file_output string
	size        int
}

// Name returns Client name.
func (c *Client) Name() string {
	return Name
}

// NewClient creates new Postregre Client.
func NewClient(sqlDB *sql.DB) (*Client, error) {
	// Check if the "samples" table exists.
	_, err := tableExists(sqlDB, "samples")
	if err != nil {
		return nil, fmt.Errorf("error while checking if samples table exists: %v", err)
	}

	// Check if the "payloads" table exists.
	_, err = tableExists(sqlDB, "payloads")
	if err != nil {
		return nil, fmt.Errorf("error while checking if payloads table exists: %v", err)
	}

	// Check if the "sources" table exists.
	_, err = tableExists(sqlDB, "sources")
	if err != nil {
		return nil, fmt.Errorf("error while checking if sources table exists: %v", err)
	}

	// Check if the "samples_sources" table exists.
	_, err = tableExists(sqlDB, "samples_sources")
	if err != nil {
		return nil, fmt.Errorf("error while checking if samples_sources table exists: %v", err)
	}

	return &Client{sqlDB: sqlDB}, nil
}

func (c *Client) GetSamples() ([]Sample, error) {
	exists, err := tableExists(c.sqlDB, "samples")
	if err != nil {
		return nil, err
	}
	var samples []Sample
	if exists {
		var sql string
		sql = `SELECT * FROM samples;`
		rows, err := c.sqlDB.Query(sql)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var sample Sample
			err := rows.Scan(&sample.sha256, &sample.mimetype, &sample.file_output, &sample.size)
			if err != nil {
				return nil, err
			}
			samples = append(samples, sample)
		}
	} else {
		return nil, fmt.Errorf("Table samples does not exist")
	}
	return samples, nil
}

func (c *Client) GetSample(sha256 string) (*Sample, error) {
	exists, err := tableExists(c.sqlDB, "samples")
	if err != nil {
		return nil, err
	}
	var sample Sample
	if exists {
		sqlStatement := `SELECT * FROM samples WHERE sha256=$1;`
		row := c.sqlDB.QueryRow(sqlStatement, sha256)
		err := row.Scan(&sample.sha256, &sample.mimetype, &sample.file_output, &sample.size)
		return &sample, err
	}
	return nil, fmt.Errorf("Table samples does not exist")
}

func (c *Client) sourceExists(sha256 string) (bool, error) {
	sqlStatement := `
	SELECT sha256 
	FROM sources 
	WHERE sha256=$1;`
	var quickSha256 string
	row := c.sqlDB.QueryRow(sqlStatement, sha256)
	switch err := row.Scan(&quickSha256); err {
	case sql.ErrNoRows:
		return false, nil
	case nil:
		return true, nil
	default:
		return false, err
	}
}

func tableExists(db *sql.DB, tableName string) (bool, error) {
	// Query to check if the table exists in PostgreSQL
	query := `
        SELECT EXISTS (
            SELECT 1
            FROM   information_schema.tables
            WHERE  table_name = $1
        )
    `

	var exists bool
	err := db.QueryRow(query, tableName).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
