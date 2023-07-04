package ydb

// error codes https://github.com/lib/pq/blob/master/error.go

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/golang-migrate/migrate/v4"

	"github.com/dhui/dktest"
	dt "github.com/golang-migrate/migrate/v4/database/testing"
	_ "github.com/golang-migrate/migrate/v4/source/file"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const (
	host          = "localhost"
	port          = "2136"
	testDB        = "database=/local"
	scheme        = "x-use-grpc-scheme"
	dbPingTimeout = 5 * time.Second
)

var (
	opts = dktest.Options{
		ReadyTimeout: 15 * time.Second,
		Hostname:     host,
		Env: map[string]string{
			"YDB_USE_IN_MEMORY_PDISKS": "true",
		},
		PortBindings: nat.PortMap{
			nat.Port(fmt.Sprintf("%s/tcp", port)): []nat.PortBinding{{
				HostIP:   "0.0.0.0",
				HostPort: port,
			}},
		},
		ReadyFunc: isReady,
	}

	image = "cr.yandex/yc/yandex-docker-local-ydb:latest"
)

func init() {
	_ = os.Setenv("YDB_ANONYMOUS_CREDENTIALS", "1")
}

func ydbConnectionString(options ...string) string {
	return fmt.Sprintf("grpc://localhost:%s/?%s", port, strings.Join(options, "&"))
}

func isReady(ctx context.Context, c dktest.ContainerInfo) bool {
	db, err := sql.Open("ydb", ydbConnectionString(host, port, testDB, scheme))
	if err != nil {
		return false
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Println("close error:", err)
		}
	}()

	ctxWithTimeout, cancel := context.WithTimeout(ctx, dbPingTimeout)
	defer cancel()

	if err = db.PingContext(ctxWithTimeout); err != nil {
		switch err {
		case sqldriver.ErrBadConn, io.EOF:
			return false
		default:
			log.Println(err)
		}
		return false
	}

	// Wait for container to bootup
	time.Sleep(10 * time.Second)

	return true
}

func Test(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		addr := ydbConnectionString(host, port, testDB, scheme)
		p := &YDB{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		dt.Test(t, d, []byte("SELECT 1"))
	})
}

func TestMigrate(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		addr := ydbConnectionString(host, port, testDB, scheme)
		p := &YDB{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		m, err := migrate.NewWithDatabaseInstance("file://./examples/migrations", "ydb", d)
		if err != nil {
			t.Fatal(err)
		}

		dt.TestMigrate(t, m)
	})
}

func TestMultipleStatements(t *testing.T) {
	dktest.Run(t, image, opts, func(t *testing.T, c dktest.ContainerInfo) {
		addr := ydbConnectionString(host, port, testDB, scheme)
		p := &YDB{}
		d, err := p.Open(addr)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := d.Close(); err != nil {
				t.Error(err)
			}
		}()
		if err := d.Run(strings.NewReader("CREATE TABLE foo (foo Utf8, PRIMARY KEY(foo)); CREATE TABLE bar (bar Utf8, PRIMARY KEY(bar));")); err != nil {
			t.Fatalf("expected err to be nil, got %v", err)
		}

		// make sure second table exists
		var table string

		if err := d.(*YDB).conn.QueryRowContext(ydb.WithQueryMode(context.Background(), ydb.ScanQueryMode), "SELECT DISTINCT Path FROM `.sys/partition_stats` WHERE Path LIKE 'bar'").Scan(&table); err != sql.ErrNoRows {
			t.Fatalf("expected table bar to exist")
		}
	})
}
