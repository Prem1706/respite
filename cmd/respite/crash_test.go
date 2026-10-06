package main

import (
	"bufio"
	"fmt"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestCrashLosesNoAcknowledgedWrites builds the real binary, sends INCRs to
// it, kills it with SIGKILL partway through (no chance to flush or clean
// up), restarts it from the AOF and checks the counter.
//
// Every INCR the client saw a reply for must survive. One more may also have
// survived: written to the AOF, but killed before its reply was sent.
//
// "everysec" passes too because SIGKILL kills the process, not the machine:
// the AOF is written to the OS before each reply, and the OS still has it.
// Only a power cut could lose up to a second of writes under that policy.
func TestCrashLosesNoAcknowledgedWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and kills the real binary")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "respite")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}

	for _, policy := range []string{"always", "everysec"} {
		t.Run(policy, func(t *testing.T) {
			aof := filepath.Join(dir, policy+".aof")
			srv, conn := startBinary(t, bin, aof, policy)

			acked := 0
			deadline := time.Now().Add(time.Second)
			r := bufio.NewReader(conn)
			for {
				if time.Now().After(deadline) && acked > 0 {
					srv.Process.Kill() // SIGKILL
					deadline = time.Now().Add(time.Hour)
				}
				if _, err := fmt.Fprint(conn, "*2\r\n$4\r\nINCR\r\n$1\r\nn\r\n"); err != nil {
					break
				}
				line, err := r.ReadString('\n')
				if err != nil {
					break
				}
				acked, _ = strconv.Atoi(strings.TrimSpace(line[1:]))
			}
			srv.Wait()

			srv, conn = startBinary(t, bin, aof, policy)
			defer srv.Process.Kill()
			fmt.Fprint(conn, "*2\r\n$3\r\nGET\r\n$1\r\nn\r\n")
			r = bufio.NewReader(conn)
			r.ReadString('\n') // $<length>
			line, _ := r.ReadString('\n')
			after, _ := strconv.Atoi(strings.TrimSpace(line))
			if after < acked || after > acked+1 {
				t.Fatalf("%d INCRs were acknowledged, but the counter is %d after the crash", acked, after)
			}
			t.Logf("acknowledged %d, recovered %d", acked, after)
		})
	}
}

func startBinary(t *testing.T, bin, aof, policy string) (*exec.Cmd, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cmd := exec.Command(bin, "-addr", addr, "-aof", aof, "-appendfsync", policy)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if conn, err := net.Dial("tcp", addr); err == nil {
			t.Cleanup(func() { conn.Close() })
			return cmd, conn
		}
		time.Sleep(20 * time.Millisecond)
	}
	cmd.Process.Kill()
	t.Fatal("server did not start")
	return nil, nil
}
