// Command respite runs a Redis-compatible server.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Prem1706/respite/internal/aof"
	"github.com/Prem1706/respite/internal/server"
)

func main() {
	addr := flag.String("addr", ":6379", "address to listen on")
	aofPath := flag.String("aof", "appendonly.aof", "append-only file (empty string disables persistence)")
	fsync := flag.String("appendfsync", "everysec", "when to fsync the AOF: always, everysec or no")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	policy, err := aof.ParseFsyncPolicy(*fsync)
	if err != nil {
		log.Error(err.Error())
		os.Exit(2)
	}

	srv, err := server.New(server.Config{AOFPath: *aofPath, Fsync: policy, Logger: log})
	if err != nil {
		log.Error("startup failed", "err", err)
		os.Exit(1)
	}
	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("listen failed", "err", err)
		os.Exit(1)
	}

	// On Ctrl-C or SIGTERM, shut down cleanly so the AOF is flushed and fsynced.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		srv.Close()
	}()

	log.Info("ready to accept connections", "addr", ln.Addr().String(), "aof", *aofPath)
	if err := srv.Serve(ln); err != nil {
		log.Error("serve failed", "err", err)
	}
	if err := srv.Close(); err != nil { // waits for a shutdown already in progress
		log.Error("shutdown failed", "err", err)
		os.Exit(1)
	}
}
