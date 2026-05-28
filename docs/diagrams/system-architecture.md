# System Architecture

This Mermaid diagram shows the current attendance automation system flow from biometric device extraction
through Pub/Sub-backed serverless processing and Google Sheets review output.

```mermaid
flowchart LR
  subgraph SD[Source Device]
    ZK[ZKTeco biometric device]
  end

  subgraph IR[Ingestion Runtime]
    PI[src/pi4/pi4_client.py compatibility Pi client]
    ING[attendance_etl.ingestion client]
    EVENT[Canonical attendance event contract target]
  end

  subgraph MSG[Messaging]
    PUBSUB[Google Pub/Sub topics]
  end

  subgraph SP[Serverless Processing]
    WRAP[Google Cloud Functions deployment wrappers]
    FUNC[attendance_etl.functions]
  end

  subgraph OUT[Storage and Review Output]
    SHEETS[Google Sheets attendance review output]
  end

  subgraph FUT[Future Storage]
    PG[PostgreSQL storage]
  end

  ZK --> PI
  PI --> ING
  ING --> EVENT
  EVENT --> PUBSUB
  PUBSUB --> WRAP
  WRAP --> FUNC
  FUNC --> SHEETS
  FUNC -. future .-> PG
```
