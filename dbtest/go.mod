module github.com/dynata/go-dbmutex/dbtest

go 1.14

replace github.com/dynata/go-dbmutex => ../

require (
	github.com/dynata/go-dbmutex v0.0.0-20201101151841-91142163ea71
	github.com/go-sql-driver/mysql v1.5.0
	github.com/lib/pq v1.8.0
	github.com/luna-duclos/instrumentedsql v1.1.4-0.20201103091713-40d03108b6f4
	github.com/ory/dockertest/v3 v3.6.2
)
