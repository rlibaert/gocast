# Domain

```mermaid
sequenceDiagram
    Broadcaster->+StreamPub: Publishes
    destroy StreamPub.AsSub
    StreamPub->StreamPub.AsSub: Remaps
    loop
        StreamPub->StreamSub: Writes
        destroy Subscriber
        StreamSub->Subscriber: Writes
    end
    StreamPub->-Broadcaster: Total bytes written 
    alt
        StreamPub->StreamSub: moves to fallback
    else
        StreamPub->StreamSub: closes
    end
```
