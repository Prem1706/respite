# Build a static binary, then copy it into an empty image. The result is a
# few MB and contains nothing but the server.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /respite ./cmd/respite

FROM scratch
COPY --from=build /respite /respite
WORKDIR /data
EXPOSE 6379 9121
ENTRYPOINT ["/respite"]
CMD ["-aof", "/data/appendonly.aof", "-metrics", ":9121"]
