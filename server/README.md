# Introduction

The server utilizes gRPC, implemented using [tonic](https://github.com/hyperium/tonic), and
employs [buf](https://buf.build/docs/introduction) for downloading and compiling external dependencies. This includes
tools like gnostic for OpenAPI generation and validate for validation purposes.

For building, compiling, downloading, and generating all proto-related materials, execute the following command:

```bash
make build-proto
``` 

## Running the server

To run any component in this directory ensure zookeeper is running with the following command:

```bash
cd .. && make run-zookeeper
```

To run the router execute the following command:

```bash
make run-router
```

To run the server execute the following command:

```bash
make run-server
```

Optionally, you can run another server as follower:

```bash
make run-follower
```

## Interacting with the server

To interact with the server, you can use the cli tool:

```bash
cargo run --bin client -- add test test
cargo run --bin client -- get test
```
