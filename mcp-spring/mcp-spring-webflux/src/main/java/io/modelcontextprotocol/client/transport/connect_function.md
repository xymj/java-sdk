```mermaid

sequenceDiagram
    participant Caller
    participant connect()
    participant eventStream() as Event Stream
    participant handler as User's Handler

    Caller->>connect(): 调用 connect(handler)
    connect()->>eventStream(): 获取 SSE 流
    eventStream()->>connect(): 返回 Flux<ServerSentEvent<String>>
    connect()->>Flux: 通过 concatMap 处理事件流
    loop 每个事件的处理
        Flux->>handle(): 封装事件为 Mono
        handle()->>handler: 调用 transform(handler)
        handler-->>Flux: 返回处理后的 Mono
    end
    connect()->>messageEndpointSink: 发射第一个 endpoint URI
    messageEndpointSink-->>connect(): 触发 then() 完成
```