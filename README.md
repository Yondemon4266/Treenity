*This project has been created as part of the 42 curriculum by aluslu, zqian.*

# Treenity

## Description

Treenity is a small message queue system written in Go, in the spirit of Kafka but much
simpler. It is made of two programs that talk to each other through **named pipes (FIFOs)**:

- **`server`**: keeps topics and their messages in memory, registers consumers and sends
  each of them the messages it asked for.
- **`client`**: one executable with several modes:
  - management: create topics, list topics, show a consumer's metadata (`info`);
  - **producer**: reads messages from stdin and sends them to a topic;
  - **consumer**: subscribes to a topic and prints the messages it receives.

A message is a **key**, a **value** and an **offset**. The offset is a 32-bit integer given
by the server, starting at 0 and incremented by one for every message of a topic.
Key + value are limited to **1024 bytes**: bigger messages are rejected, not split.

Consumers can:
- filter messages with a **key prefix** (`--prefix user.` only receives keys starting with `user.`);
- start from any position with `--offset`, to replay old messages or skip to newer ones;
- come back later under the same name and **resume** from their last committed offset.

Several producers and consumers can use the same topic at the same time, and several
servers can run side by side without interfering (each one is identified by its PID).

## Instructions

### Requirements

- Linux (or WSL), because the project uses FIFOs (`mkfifo`) and POSIX signals.
- Go **1.22** or newer. The project uses only the Go standard library.

### Build

```bash
make        # builds ./server and ./client
make clean  # removes ./server and ./client
make re     # clean, then build again
make test   # runs the prefix-matching unit tests
```

### Run

Start the server. It prints its IPC identifier (the path of its main FIFO) on stdout:

```bash
./server
# /tmp/treenity.server.1337
```

In other terminals, use this identifier with the client:

```bash
IPC=/tmp/treenity.server.1337

./client $IPC create user_events          # topic created
./client $IPC list                        # user_events

# Producer: one "key:value" message per line on stdin
printf 'user.login:alice\nuser.logout:bob\nadmin.reset:all\n' | ./client $IPC produce user_events

# Consumer: runs until Ctrl+C or until the server stops
./client $IPC subscribe user_events c1 --prefix user.
# subscribed to user_events
# user.login:alice
# user.logout:bob

./client $IPC info c1
# {"client":"c1","topic":"user_events","offset":2,"prefix":"user.","ipc":"/tmp/treenity.server.1337:consumer:c1"}
```

Full syntax:

```
./client <ipc_identifier> create <topic_name>
./client <ipc_identifier> list
./client <ipc_identifier> produce <topic_name> [--raw]
./client <ipc_identifier> subscribe <topic_name> <subscriber_name> [--prefix <prefix>] [--offset <offset>] [--raw]
./client <ipc_identifier> info <subscriber_name>
```

Topic and client names must match `^[a-zA-Z0-9_.-]{1,32}$`.

### Message formats

- **Text** (default): one `key:value` line per message. The key ends at the first `:`, so the
  value may contain `:`. The consumer prints messages in the same format.
- **Raw** (`--raw`): binary records, sizes are 4-byte little-endian integers, with no separator
  between records.
  - producer input: `[keysize][key][valuesize][value]`
  - consumer output: `[offset][keysize][key][valuesize][value]`

```bash
printf '\x03\x00\x00\x00key\x05\x00\x00\x00value' | ./client $IPC produce user_events --raw
```

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | success |
| 1 | general error: invalid arguments, invalid message, cannot connect |
| 2 | topic error: topic already exists / does not exist, name already in use |
| 3 | IPC communication error |

Only the documented results are written to stdout. Errors and logs go to stderr.

### Stopping

`Ctrl+C` (SIGINT) or SIGTERM stops a program cleanly:
- a **consumer** tells the server it disconnects, then exits with code 0;
- the **server** stops accepting new work, tells every consumer to leave, waits for them
  (5 seconds at most), then removes all its FIFOs.

### Validation tools

The subject provides `fish` (a producer of random fish) and `aquarium` (a consumer that
draws them). Put them in `subject/attachments/`, start a server, then:

```bash
make fish     TREENITY_IPC=/tmp/treenity.server.1337 TOPIC=bocal
make aquarium TREENITY_IPC=/tmp/treenity.server.1337 TOPIC=bocal SUB_NAME=aqua_sub
```

`make bubble` starts a server, 200 `fish` producers and one `aquarium` in one command.

## Architecture

```
cmd/server, cmd/client   entry points (main)
internal/server          server: requests, topics, consumers, shutdown
internal/client          client: arguments, commands, connection to the server
internal/protocol        shared types, names, limits, exit codes, text/raw formats
internal/fifo            create, open, write and remove FIFOs
internal/hashmap         hand-written hashmap for the client index
internal/trie            prefix tree for consumer filtering
```

### Routine model

- **Main routine (one per server).** It reads requests from the main FIFO one line at a time
  and handles them in order: `create`, `list`, `produce`, `subscribe`, `ack`, `disconnect`,
  `info`. It owns the server-wide state: the table of topics and the client index (hashmap).
- **One routine per topic.** Created with the topic (`newTopic`), it owns that topic's message
  log, its connected consumers and its prefix trie. It appends produced messages to the log
  and writes them to the consumers whose prefix matches.
- **One signal routine.** On SIGINT/SIGTERM it wakes the main routine up by setting a read
  deadline on the main FIFO.

### Synchronization strategy

There are **no mutexes**: every piece of shared state has exactly **one owner routine**, and
the other routines only talk to it through **Go channels** ("share memory by communicating").

- The topic table, the client hashmap and the committed offsets are only read and written by
  the main routine.
- A topic's log, its consumers and its trie are only touched by that topic's routine. The main
  routine sends it work through channels: `incoming` (a new message), `attach` / `detach`
  (a consumer arrives or leaves), `farewell` (shutdown is starting) and `stop` (remove the FIFOs).
- Because a single routine appends to a log, offsets are always strictly increasing and the
  messages of one producer keep their order. Messages from different producers may be
  interleaved.

Writes to a consumer FIFO have a 50 ms deadline. A consumer that reads slowly therefore never
blocks the others: the topic routine skips it and tries again every 100 ms, from the offset it
had reached.

### Offsets and acknowledgements

The server sends a message at offset `N`. The consumer prints it, then sends `ack N+1` on the
main FIFO. The main routine stores that value in the client's record, so the stored offset is
always the **next** message the consumer wants. When a consumer comes back without `--offset`,
it starts from this stored value. A new consumer starts at 0, and `--offset` overrides both.

### Shutdown and failures

- **Server shutdown.** The server stops accepting everything except `ack` and `disconnect`
  (other requests get exit code 3). Each topic sends a `shutdown` message on every consumer
  FIFO, so blocked consumers wake up, send `disconnect` and exit with code 0. When every
  consumer has left, or after 5 seconds, the server removes the consumer FIFOs and its main
  FIFO.
- **Consumer killed with SIGKILL.** A killed consumer cannot say goodbye. The server reads the
  client's PID from its reply FIFO name and checks every 100 ms with `kill(pid, 0)` that the
  process still exists. If it does not, the topic sends a `disconnect` request for it on the main
  FIFO, so its name can be used again and the server keeps serving the other clients.

## IPC Choice

The subject allows three mechanisms: System V message queues, POSIX message queues and named
pipes. We chose **named pipes (FIFOs)**.

### Why FIFOs

- **Native in Go.** The standard library creates a FIFO with `syscall.Mkfifo`, then it is an
  ordinary `os.File`: we read it with `bufio`, write it with `Write`, and close it with
  `Close`. No raw system calls, no C code (cgo), no external library.
- **They work with Go's runtime.** A FIFO `os.File` supports deadlines (`SetReadDeadline`,
  `SetWriteDeadline`), and we rely on them twice:
  - the signal routine stops the server's blocking read on its main FIFO by setting a read
    deadline;
  - writes to a consumer FIFO have a 50 ms deadline, so a slow consumer never blocks its topic.
- **Atomic writes.** A write of up to `PIPE_BUF` (4096 bytes on Linux) is atomic. Every request
  is one JSON line shorter than 4096 bytes, so many clients can write to the same server FIFO at
  the same time without their requests getting mixed.
- **A FIFO is a file path.** The IPC identifier the server prints is directly usable and
  readable (`/tmp/treenity.server.<pid>`). FIFOs are easy to inspect (`ls -l /tmp`) and to
  remove (`rm`), and the server PID in the name keeps several servers apart.
- **Data stays in memory.** A FIFO's data goes through the kernel and is never written to
  disk, and the default pipe buffer (64 KiB) is large enough for our messages.

### Why not POSIX message queues

POSIX queues (`mq_open`, `mq_send`, `mq_receive`) have real advantages: they keep message
boundaries and support priorities. We did not choose them because:
- **No support in Go's standard library.** The `syscall` package has no `mq_*` function. We
  would have to call the raw system calls ourselves (`syscall.Syscall` with `SYS_MQ_OPEN`…)
  or use cgo, and external IPC libraries are forbidden by the subject.
- **No integration with the Go runtime.** A blocking `mq_receive` blocks a whole OS thread,
  and there is no `SetReadDeadline` to wake it up on SIGINT. We would need `mq_timedreceive`
  with polling loops instead.
- **Small default limits on Linux.** By default a queue holds at most **10 messages**
  (`/proc/sys/fs/mqueue/msg_max`), and the whole system allows **256 queues**
  (`queues_max`). Our design needs one channel per client and one per consumer, so many
  clients could hit this limit unless the limits are raised as root.
- **Harder to inspect.** Queues are only visible if the `mqueue` file system is mounted
  (`/dev/mqueue`).

### Why not System V message queues

System V queues (`msgget`, `msgsnd`, `msgrcv`) are the oldest of the three. Their message
types (`mtype`) could even let one queue carry messages for many clients. We did not choose
them because:
- **No support in Go's standard library** either: no `msgget`/`msgsnd`/`msgrcv`, so again raw
  system calls with a hand-built C structure (`long mtype` followed by the data), or cgo.
- **Identifiers are numbers, not paths.** A queue is found with a key built by `ftok()`. That
  is less clear as an IPC identifier, and two servers could get the same key.
- **Queues outlive their process.** A System V queue stays in the kernel until someone removes
  it (`msgctl(IPC_RMID)` or `ipcrm`), even after the process dies. A crashed server would leave
  queues behind, visible only with `ipcs`.
- **Small limits too.** By default one queue holds **16 KiB** in total (`msgmnb`): only about
  16 messages of 1 KiB before senders block.
- **No integration with the Go runtime** (no deadlines, blocking calls), as with POSIX queues.

### Summary

| | FIFO (our choice) | POSIX message queue | System V message queue |
|---|---|---|---|
| Go standard library | yes (`syscall.Mkfifo` + `os.File`) | no | no |
| Read/write deadlines in Go | yes | no | no |
| Message boundaries | no, we use one JSON line per message | yes | yes |
| Identifier | file path | name (`/name`) | number (`ftok` key) |
| Default Linux limits | 64 KiB buffer per FIFO | 10 messages per queue, 256 queues | 16 KiB per queue |
| After a crash | file left in `/tmp`, replaced on the next create | stays until `mq_unlink` | stays until `ipcrm` |

For this project, FIFOs were the simplest correct solution: what they lack (message boundaries,
two-way communication) is easy to add on top of them, as described below.

### Limits of FIFOs and how we handle them
- A FIFO is **one-way** and has no notion of message: we need our own framing (one JSON line
  per message).
- Opening a FIFO blocks until the other side opens it too, unless it is opened in
  non-blocking mode or in read-write mode.
- FIFOs are files: they stay in `/tmp` if a program dies without removing them.

### Bidirectional communication

We use one FIFO per direction:

| FIFO | Created by | Direction | Used for |
|------|------------|-----------|----------|
| `/tmp/treenity.server.<pid>` | server | clients → server | every request (management, registration, produce, ack, disconnect) |
| `<endpoint>:reply:<client pid>` | client | server → client | the response to each request |
| `<endpoint>:consumer:<name>` | server | server → one consumer | the messages of a subscription |

Every request carries the path of the client's reply FIFO, and the server writes its JSON
response there. So the main endpoint is only used for requests, and each subscribed consumer has its own
dedicated channel for its messages. The server PID in the endpoint name keeps several
servers apart.

### Resource management

The server opens its own FIFOs in read-write mode, so they never see
end-of-file when a client closes its side, and clients open the server FIFO in non-blocking
mode, so they fail at once (exit code 1) if no server is listening. On stop, every FIFO
starting with the server's endpoint path is removed. A FIFO left by a crashed run is replaced
when it is created again, so a restart is never blocked.

## Data Structures

### Client index: hand-written hashmap (`internal/hashmap`)

The server indexes consumer metadata (name, topic, offset, prefix, consumer FIFO) by client
name in our own hashmap. No Go `map` is used for this index.

- **Hash function:** djb2 (`h = h*33 + byte`, starting from 5381). Simple and fast, with a good
  spread for short text keys like client names.
- **Collision resolution: separate chaining.** Each bucket is a slice of key/value pairs.
  `Get`, `Set` and `Remove` hash the key to a bucket, then compare the keys in that bucket.
- **Growth:** it starts with 8 buckets. When there are more than 2 entries per bucket on
  average, the number of buckets doubles and every entry is placed again. Buckets stay short,
  so operations are O(1) on average.
- It is generic (`Map[V]`), so it could store other values, but the server uses it for the
  client index.

### Prefix matching: trie (`internal/trie`)

Each topic keeps a **trie** (prefix tree) of its connected consumers: one node per byte, and
each node lists the names of the consumers whose prefix ends there.

- `Add(prefix, name)` walks down the trie, creating nodes when needed, and stores the name on
  the last node. Several consumers can share the same prefix.
- `Match(key)` walks down the trie along the bytes of the message key and collects the names
  found on the way. Each name found is a consumer whose prefix is a prefix of the key.
- **Empty prefix (wildcard):** an empty prefix is stored on the root itself, so these
  consumers are added to the result directly, before any traversal.
- `Remove(prefix, name)` removes a consumer when it disconnects.

**Why a trie:** the cost of `Match` depends only on the **length of the key** (at most 1024
bytes), not on the number of consumers. With N consumers, comparing the key with every prefix
costs O(N × prefix length); the trie answers in O(key length) and returns exactly the
consumers to serve. Keys are compared as raw bytes, so binary keys work too. The cost is some
extra memory for the nodes.

| Prefix | Key | Match? |
|--------|-----|--------|
| `user` | `user` | yes (exact match) |
| `user` | `user.login` | yes (prefix match) |
| `user` | `admin` | no |
| `""` | anything | yes (wildcard) |

## Testing

All tests use Go's standard `testing` package and run with `go test`.

### Prefix-matching unit tests

They are in `internal/trie/trie_test.go` and are built and run with:

```bash
make test
```

This compiles the trie package with its tests and runs them (`go test -v`). `make` returns a
non-zero exit code if a test fails. The tests cover what the subject asks for:

| Requirement | Tests |
|-------------|-------|
| Basic prefix matching (exact, partial, no match) | `TestMatchRules` |
| Empty prefix receives every message | `TestEmptyPrefix` |
| Data structure operations | `TestSharedPrefix`, `TestNestedPrefixesShortestFirst`, `TestRemove`, `TestMatchReturnsCopy`, `TestBinaryBytes` |
| Edge cases: empty, minimal, large-scale | `TestEmptyTrie`, `TestMinimal`, `TestLargeScale` |
| Integration with mixed prefix types | `TestMixedPrefixesIntegration` |

### All tests

```bash
make test-all       # go test -v ./...
go test -race ./... # same, with Go's race detector
```

This also runs the tests of the other packages: the hashmap (collisions, growth), the FIFO
helpers, the protocol (names, limits, text and raw formats), the client commands and their exit
codes, and the server (subscriptions, offsets, prefix filtering, slow consumers, a real server
driven through its FIFOs, graceful shutdown, a consumer killed with SIGKILL).

## Resources

- [Go documentation: `os`, `syscall`, `os/signal`, `bufio`, `encoding/json`](https://pkg.go.dev/std)
- [Go documentation: `testing`](https://pkg.go.dev/testing)
- [Effective Go: concurrency, "share memory by communicating"](https://go.dev/doc/effective_go#concurrency)
- `man 7 fifo`, `man 7 pipe` (atomic writes and `PIPE_BUF`), `man 3 mkfifo`, `man 2 kill`
- [Trie (Wikipedia)](https://en.wikipedia.org/wiki/Trie)
- [Hash table, separate chaining (Wikipedia)](https://en.wikipedia.org/wiki/Hash_table#Separate_chaining)
- [djb2 hash function](http://www.cse.yorku.ca/~oz/hash.html)
- [Apache Kafka introduction](https://kafka.apache.org/intro) (topics, offsets, consumers)
- [Conventional Commits](https://www.conventionalcommits.org/)

### AI usage

We used an AI assistant (Claude) as a helper:
- **Explanations:** Go concepts (goroutines, channels, `os.Exit` and `defer`), FIFO behaviour
  (blocking `open`, `ENXIO`, `EPIPE`, atomic writes) and Git commands.
- **Review:** comparing the code and the tests with the subject and the evaluation scale, which
  helped us find bugs (partial raw records, negative raw sizes, `info` output, requests accepted
  during shutdown, consumers killed with SIGKILL).
- **Tests:** suggesting missing test cases and making the test output easier to read.
- **Documentation:** drafting and correcting the English of this README.

We reviewed, tested and discussed everything generated before putting it in the project, and
each of us can explain every part of it.
