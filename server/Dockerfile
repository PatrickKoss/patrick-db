# Using official rust base image
FROM rust:1.75.0-alpine3.19

# Set the application directory
WORKDIR /app

# Install musl-tools to make many crates compile successfully
RUN apk add --no-cache musl-dev

# Install cargo-watch
RUN cargo install cargo-watch

# Install buf and protoc
RUN apk add --no-cache curl unzip && \
    curl -sSL https://github.com/protocolbuffers/protobuf/releases/download/v25.0/protoc-25.0-linux-x86_64.zip -o protoc.zip && \
    unzip protoc.zip -d /usr/local bin/protoc && \
    unzip protoc.zip -d /usr/local 'include/*' && \
    rm protoc.zip && \
    curl -sSL https://github.com/bufbuild/buf/releases/download/v1.28.0/buf-Linux-x86_64 -o /usr/local/bin/buf && \
    chmod +x /usr/local/bin/buf

# Copy the files to the Docker image
COPY ./ ./
