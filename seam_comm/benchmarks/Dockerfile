FROM alpine:3.19

# 安装NATS服务器
RUN apk add --no-cache curl \
    && curl -L https://github.com/nats-io/nats-server/releases/download/v2.10.8/nats-server-v2.10.8-linux-amd64.tar.gz -o nats-server.tar.gz \
    && tar -xzf nats-server.tar.gz \
    && mv nats-server-v2.10.8-linux-amd64/nats-server /usr/local/bin/ \
    && rm -rf nats-server-v2.10.8-linux-amd64 nats-server.tar.gz \
    && chmod +x /usr/local/bin/nats-server

# 创建NATS数据目录
RUN mkdir -p /data/jetstream

# 暴露NATS端口
EXPOSE 4222 8222

# 启动NATS服务器，启用JetStream
ENTRYPOINT ["nats-server", "--jetstream", "--store_dir", "/data/jetstream"]

# 默认命令参数
CMD ["--http_port", "8222"] 