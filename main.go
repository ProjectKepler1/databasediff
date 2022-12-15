package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/joho/godotenv"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var (
	maxOpenConnection = 5
	// list tables that need to be compared
	tables = []string{
		"imx_table_A",
		"imx_table_B",
		"imx_table_C",
	}
)

type DB struct {
	DB          *sqlx.DB
	ServiceName string
}

type Databases struct {
	source DB
	dest   DB
}

type TableDiff struct {
	Name                         string
	SourceRowCount, DestRowCount int
}

func initializeDatabases(sourceDB, sourceConn, destDB, destConn string) (*Databases, error) {
	srcdb, err := sqlx.Open("postgres", sourceConn)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	srcdb.SetMaxOpenConns(maxOpenConnection)

	destdb, err := sqlx.Open("postgres", destConn)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	destdb.SetMaxOpenConns(maxOpenConnection)

	return &Databases{
		DB{srcdb, sourceDB},
		DB{destdb, destDB},
	}, nil
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	// human-legible name of source DB (i.e. public-api)
	sourceDB := os.Getenv("SRC_DB")
	sourceConn := os.Getenv("SRC_CONN")
	// i.e. orderbook DB
	destDB := os.Getenv("DEST_DB")
	destConn := os.Getenv("DEST_CONN")
	databases, err := initializeDatabases(sourceDB, sourceConn, destDB, destConn)
	if err != nil {
		panic(err)
	}
	fmt.Println("Databases initialized")

	defer func(databases *Databases) {
		err := databases.source.DB.Close()
		if err != nil {
			panic(err)
		}
		err = databases.dest.DB.Close()
		if err != nil {
			panic(err)
		}
		fmt.Println("Database connections closed")
	}(databases)

	ctx := context.Background()
	limiter := make(chan bool, maxOpenConnection)
	tableDiffStream := make(chan TableDiff, len(tables))
	defer close(tableDiffStream)

	for _, tableName := range tables {
		go compareTables(ctx, limiter, tableDiffStream, tableName, databases)
	}

	printTableDiffStream(tableDiffStream, sourceDB, destDB)
	fmt.Println("Done")
}

func printTableDiffStream(tableDiffStream chan TableDiff, sourceDB, destDB string) {
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', 0)
	if _, err := fmt.Fprintf(w, "\nTable\t%s\t%s\tDiff\n", sourceDB, destDB); err != nil {
		panic(err)
	}

	for range tables {
		select {
		case tableDiff := <-tableDiffStream:
			printTableDiff(w, tableDiff)
		}
	}
	if err := w.Flush(); err != nil {
		panic(err)
	}
}

func printTableDiff(w *tabwriter.Writer, tableDiff TableDiff) {
	_, err := fmt.Fprintf(w, "%s\t%d\t%d\t\t%d\n", tableDiff.Name, tableDiff.SourceRowCount, tableDiff.DestRowCount, tableDiff.SourceRowCount-tableDiff.DestRowCount)
	if err != nil {
		panic(err)
	}
}

func compareTables(ctx context.Context, limiter chan bool, tableDiffStream chan TableDiff, tableName string, databases *Databases) {
	limiter <- true

	table := TableDiff{Name: tableName}
	start := time.Now()
	c1 := make(chan int)
	c2 := make(chan int)

	go getRowCount(&databases.source, ctx, table, c1)
	go getRowCount(&databases.dest, ctx, table, c2)

	for i := 0; i < 2; i++ {
		select {
		case msg1 := <-c1:
			table.SourceRowCount = msg1
		case msg2 := <-c2:
			table.DestRowCount = msg2
		}
	}

	fmt.Printf("Retrieved row counts from %s in %s\n", tableName, time.Since(start))
	close(c1)
	close(c2)
	tableDiffStream <- table
	<-limiter
}

func getRowCount(db *DB, ctx context.Context, table TableDiff, countStream chan int) {
	conn, err := db.DB.Conn(ctx)
	if err != nil {
		println(err.Error())
		panic(err)
	}

	defer func(conn *sql.Conn) {
		if err := conn.Close(); err != nil {
			println(err.Error())
			panic(err)
		}
	}(conn)

	count := -1
	// don't concatenate table name in production code...
	if err = conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table.Name).Scan(&count); err != nil {
		println(err.Error())
		panic(err)
	}
	countStream <- count
}
