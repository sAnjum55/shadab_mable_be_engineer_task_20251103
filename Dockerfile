# -------- BUILD STAGE --------
    FROM golang:1.24-bullseye AS builder

    WORKDIR /app
    
    # Copy go.mod and go.sum first for caching
    COPY go.mod go.sum ./
    RUN go mod download
    
    # Copy the entire project
    COPY . .
    
    # Build static binary
    RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o runner main.go
    
    # -------- RUNTIME STAGE --------
    FROM debian:bullseye-slim
    
    WORKDIR /app
    
    # Copy compiled binary from builder
    COPY --from=builder /app/runner /app/runner
    RUN chmod +x /app/runner
    
    # Copy testdata for input files
    COPY pipeline/testdata ./pipeline/testdata
    
    # Expose ports
    EXPOSE 8123
    EXPOSE 3000
    
    # Run the executable
    ENTRYPOINT ["./runner"]
    