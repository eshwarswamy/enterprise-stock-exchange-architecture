Enterprise Stock Exchange Platform — Complete System Architecture

  ---
  1. EXECUTIVE SUMMARY

  This document defines the end-to-end architecture for a globally distributed, fault-tolerant Stock Exchange platform processing 2 billion+ records across
  20 nodes in two regions (Plano, Hazelwood). The system ingests market/trading/reference data from 4 upstream sources, processes it through event-driven
  microservices, persists to Cassandra and Elasticsearch, and delivers to 4 downstream consumers — all with sub-millisecond P99 latency guarantees, 99.999%
  availability, and full regulatory audit compliance.

  Core Design Principles:
  - Event-driven, eventually consistent — Kafka as the backbone
  - CQRS + Event Sourcing — separate read/write paths
  - Defense in depth — security at every layer
  - Observability-first — every component instrumented
  - Chaos-engineered — failure is assumed, not exceptional

  ---
  2. REQUIREMENTS ANALYSIS

  Functional Requirements

  ┌────────────┬────────────────────────────────────────────────────────────────────────┐
  │  Category  │                              Requirement                               │
  ├────────────┼────────────────────────────────────────────────────────────────────────┤
  │ Ingestion  │ Accept data from 4 upstream apps in real-time (<5ms ingestion latency) │
  ├────────────┼────────────────────────────────────────────────────────────────────────┤
  │ Processing │ Transform raw market/trading data to DW-compatible format              │
  ├────────────┼────────────────────────────────────────────────────────────────────────┤
  │ Storage    │ Persist 2B+ records in Cassandra with hot/cold tiering                 │
  ├────────────┼────────────────────────────────────────────────────────────────────────┤
  │ Search     │ Full-text and analytical queries via Elasticsearch                     │
  ├────────────┼────────────────────────────────────────────────────────────────────────┤
  │ Delivery   │ Push processed data to 4 downstream consumers                          │
  ├────────────┼────────────────────────────────────────────────────────────────────────┤
  │ APIs       │ RESTful + streaming APIs for external consumers                        │
  ├────────────┼────────────────────────────────────────────────────────────────────────┤
  │ Audit      │ Immutable audit trail for all transactions                             │
  └────────────┴────────────────────────────────────────────────────────────────────────┘

  Non-Functional Requirements

  ┌───────────────┬──────────────────────────────────────────┐
  │    Metric     │                  Target                  │
  ├───────────────┼──────────────────────────────────────────┤
  │ Throughput    │ 1M+ events/sec sustained, 5M peak        │
  ├───────────────┼──────────────────────────────────────────┤
  │ Latency (P99) │ <10ms API, <50ms end-to-end pipeline     │
  ├───────────────┼──────────────────────────────────────────┤
  │ Availability  │ 99.999% (5 nines = ~5 min downtime/year) │
  ├───────────────┼──────────────────────────────────────────┤
  │ RPO           │ 0 (zero data loss)                       │
  ├───────────────┼──────────────────────────────────────────┤
  │ RTO           │ <30 seconds (automated failover)         │
  ├───────────────┼──────────────────────────────────────────┤
  │ Scalability   │ Horizontal, linear scaling               │
  ├───────────────┼──────────────────────────────────────────┤
  │ Compliance    │ SOX, PCI-DSS, FINRA, SEC Rule 17a-4      │
  └───────────────┴──────────────────────────────────────────┘

  ---
  3. HIGH-LEVEL ARCHITECTURE

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                           STOCK EXCHANGE PLATFORM — HLA                             │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
  │  │ Upstream #1  │  │ Upstream #2  │  │ Upstream #3  │  │ Upstream #4  │           │
  │  │ Market Data  │  │ Trade Engine │  │  Reference   │  │   Order Mgmt │           │
  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
  │         │                 │                 │                 │                    │
  │         └─────────────────┴────────┬────────┴─────────────────┘                    │
  │                                    │                                                │
  │                           ┌────────▼────────┐                                      │
  │                           │   AWS Route53   │ ← DNS + Health-check failover        │
  │                           │   + CloudFront  │                                      │
  │                           └────────┬────────┘                                      │
  │                                    │                                                │
  │                           ┌────────▼────────┐                                      │
  │                           │  WAF + DDoS     │ ← AWS Shield Advanced                │
  │                           │  Protection     │                                      │
  │                           └────────┬────────┘                                      │
  │                                    │                                                │
  │                    ┌───────────────▼───────────────┐                               │
  │                    │        ALB / NLB Layer         │                               │
  │                    │  (Layer 7 / Layer 4 routing)   │                               │
  │                    └───────────────┬───────────────┘                               │
  │                                    │                                                │
  │                    ┌───────────────▼───────────────┐                               │
  │                    │      Spring Cloud Gateway      │                               │
  │                    │  (Rate Limiting, Auth, Routing)│                               │
  │                    └───────────────┬───────────────┘                               │
  │                                    │                                                │
  │         ┌──────────────────────────┼──────────────────────────┐                    │
  │         │                          │                          │                    │
  │  ┌──────▼───────┐         ┌────────▼────────┐       ┌────────▼────────┐           │
  │  │   Ingest     │         │   Processing    │       │   Query/Read    │           │
  │  │  Services    │         │   Services      │       │   Services      │           │
  │  └──────┬───────┘         └────────┬────────┘       └────────┬────────┘           │
  │         │                          │                          │                    │
  │         └──────────────────────────┼──────────────────────────┘                    │
  │                                    │                                                │
  │                    ┌───────────────▼───────────────┐                               │
  │                    │         Apache Kafka           │                               │
  │                    │    (MSK — 3 Broker Cluster)    │                               │
  │                    └───────────────┬───────────────┘                               │
  │                                    │                                                │
  │         ┌──────────────────────────┼──────────────────────────┐                    │
  │         │                          │                          │                    │
  │  ┌──────▼───────┐         ┌────────▼────────┐       ┌────────▼────────┐           │
  │  │  Cassandra   │         │ Elasticsearch   │       │  Data Warehouse │           │
  │  │   Cluster    │         │   Cluster       │       │  Sync Service   │           │
  │  └──────────────┘         └─────────────────┘       └────────┬────────┘           │
  │                                                               │                    │
  │                                                      ┌────────▼────────┐           │
  │                                                      │    AWS S3 /     │           │
  │                                                      │  Redshift / DW  │           │
  │                                                      └─────────────────┘           │
  │                                                                                     │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
  │  │Downstream #1 │  │Downstream #2 │  │Downstream #3 │  │Downstream #4 │           │
  │  │ Risk Engine  │  │  Analytics   │  │  Compliance  │  │  Settlement  │           │
  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘           │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  ---
  4. LOW-LEVEL ARCHITECTURE DIAGRAM

  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │                         PLANO REGION (Primary)                                       │
  │                                                                                      │
  │  ┌─────────────────────────────────────────────────────────────────────────────┐    │
  │  │                          EKS Cluster (12 nodes)                             │    │
  │  │                                                                             │    │
  │  │  ┌────────────────────────────────────────────────────────────────────┐    │    │
  │  │  │                    Namespace: ingress-system                       │    │    │
  │  │  │  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐ │    │    │
  │  │  │  │  NGINX Ingress  │   │  Cert-Manager   │   │  External-DNS   │ │    │    │
  │  │  │  │  Controller     │   │  (TLS Certs)    │   │  (Route53 Sync) │ │    │    │
  │  │  │  └────────┬────────┘   └─────────────────┘   └─────────────────┘ │    │    │
  │  │  └───────────┼────────────────────────────────────────────────────────┘    │    │
  │  │              │                                                              │    │
  │  │  ┌───────────▼────────────────────────────────────────────────────────┐    │    │
  │  │  │                    Namespace: platform-core                        │    │    │
  │  │  │                                                                    │    │    │
  │  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐    │    │    │
  │  │  │  │  API Gateway │  │ Auth Service │  │  Config Service      │    │    │    │
  │  │  │  │  (3 replicas)│  │ (3 replicas) │  │  (Spring Cloud Cfg)  │    │    │    │
  │  │  │  │  Port: 8080  │  │  Port: 8081  │  │  Port: 8888          │    │    │    │
  │  │  │  └──────┬───────┘  └──────┬───────┘  └──────────────────────┘    │    │    │
  │  │  │         │                 │                                        │    │    │
  │  │  │  ┌──────▼─────────────────▼──────────────────────────────────┐    │    │    │
  │  │  │  │              Service Mesh (Istio / Envoy)                 │    │    │    │
  │  │  │  │         mTLS between all services, traffic policies       │    │    │    │
  │  │  │  └───────────────────────────────────────────────────────────┘    │    │    │
  │  │  └────────────────────────────────────────────────────────────────────┘    │    │
  │  │                                                                             │    │
  │  │  ┌─────────────────────────────────────────────────────────────────────┐   │    │
  │  │  │                   Namespace: data-ingestion                         │   │    │
  │  │  │                                                                     │   │    │
  │  │  │  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐  │   │    │
  │  │  │  │ Market Data     │   │ Trade Processing │   │ Reference Data  │  │   │    │
  │  │  │  │ Ingestion Svc   │   │ Ingestion Svc   │   │ Ingestion Svc   │  │   │    │
  │  │  │  │ (5 replicas)    │   │ (5 replicas)    │   │ (3 replicas)    │  │   │    │
  │  │  │  │ HPA: 5-20 pods  │   │ HPA: 5-20 pods  │   │ HPA: 3-10 pods  │  │   │    │
  │  │  │  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘  │   │    │
  │  │  └───────────┼────────────────────┼────────────────────┼─────────────┘   │    │
  │  │              │                    │                    │                  │    │
  │  │  ┌───────────▼────────────────────▼────────────────────▼──────────────┐  │    │
  │  │  │                  Amazon MSK (Kafka Cluster)                        │  │    │
  │  │  │         3 Brokers x 2 AZs = 6 broker instances                    │  │    │
  │  │  │  Topics: market-data, trades, reference, transforms, dlq, audit    │  │    │
  │  │  └───────────┬────────────────────────────────────────────────────────┘  │    │
  │  │              │                                                            │    │
  │  │  ┌───────────▼────────────────────────────────────────────────────────┐  │    │
  │  │  │               Namespace: data-processing                           │  │    │
  │  │  │                                                                    │  │    │
  │  │  │  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │  │    │
  │  │  │  │ Data Transform  │  │  Cassandra Write  │  │  ES Indexing     │  │  │    │
  │  │  │  │ Service         │  │  Service          │  │  Service         │  │  │    │
  │  │  │  │ (5 replicas)    │  │  (5 replicas)     │  │  (3 replicas)    │  │  │    │
  │  │  │  └─────────────────┘  └──────────────────┘  └──────────────────┘  │  │    │
  │  │  │                                                                    │  │    │
  │  │  │  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │  │    │
  │  │  │  │ DW Sync Service │  │ Downstream Svc   │  │  Audit Service   │  │  │    │
  │  │  │  │ (2 replicas)    │  │ (3 replicas)     │  │  (3 replicas)    │  │  │    │
  │  │  │  └─────────────────┘  └──────────────────┘  └──────────────────┘  │  │    │
  │  │  └────────────────────────────────────────────────────────────────────┘  │    │
  │  └─────────────────────────────────────────────────────────────────────────┘    │
  │                                                                                      │
  │  ┌──────────────────────┐    ┌───────────────────────┐    ┌─────────────────────┐  │
  │  │  Cassandra Cluster   │    │ Elasticsearch Cluster │    │  Amazon S3 + DW     │  │
  │  │  3 DCs x 3 nodes     │    │  3 Master + 6 Data    │    │  Redshift/Athena    │  │
  │  │  RF=3, LOCAL_QUORUM  │    │  Hot(3) + Warm(3)     │    │  Parquet/ORC format │  │
  │  └──────────────────────┘    └───────────────────────┘    └─────────────────────┘  │
  └──────────────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────────────────┐
  │                       HAZELWOOD REGION (Secondary/DR)                               │
  │                     (Mirror architecture — 8 EKS nodes)                             │
  │              Active-Active for read; Active-Passive for writes                       │
  └──────────────────────────────────────────────────────────────────────────────────────┘

  ---
  5. END-TO-END DATA FLOW

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                         COMPLETE DATA FLOW DIAGRAM                                  │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  Step 1: UPSTREAM DATA INGESTION
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  [Upstream App] ──HTTPS/gRPC──► [ALB] ──► [API Gateway] ──► [Market Data Ingestion Svc]
                                                                        │
                                                             Validate + Deserialize
                                                             Schema Registry Check
                                                                        │
                                                                        ▼
                                                           Publish to Kafka Topic:
                                                           "raw.market-data.v1"
                                                           Partition Key: symbol+timestamp

  Step 2: KAFKA EVENT PROCESSING
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

                      ┌─────────────────────────────────────────┐
                      │            Kafka Topic Flow              │
                      │                                         │
  raw.market-data ──► │  Topic: raw.market-data.v1              │
                      │  Partitions: 64, RF: 3, Retention: 7d   │
                      │                                         │
                      └──────────────┬──────────────────────────┘
                                     │
                 ┌───────────────────┼───────────────────┐
                 │                   │                   │
                 ▼                   ▼                   ▼
      [Transform Consumer]  [Audit Consumer]   [Monitoring Consumer]
           Group: transform-cg    audit-cg        metrics-cg
                 │
                 ▼
      Apply transformation rules
      Enrich with reference data
      Validate business rules
                 │
                 ▼
      Publish to: transformed.data.v1
                 │
                 ├──► [Cassandra Write Consumer] ──► Cassandra
                 ├──► [ES Indexing Consumer]     ──► Elasticsearch
                 ├──► [DW Sync Consumer]         ──► S3 → Redshift
                 └──► [Downstream Delivery]      ──► 4 Downstream Apps

  Step 3: PERSISTENCE LAYER
  ━━━━━━━━━━━━━━━━━━━━━━━━━━

                      ┌──────────────────────────────────┐
                      │        Cassandra Write Path       │
                      │                                  │
  Transformed Data ──►│ BatchStatement (unlogged batch)  │
                      │ Partition: symbol + date bucket  │
                      │ Consistency: LOCAL_QUORUM         │
                      │ Write → Commit Log → Memtable    │
                      │ Async flush to SSTable            │
                      └──────────────────────────────────┘

                      ┌──────────────────────────────────┐
                      │      Elasticsearch Index Path     │
                      │                                  │
  Transformed Data ──►│ Bulk API (batch 500 docs)        │
                      │ Index: market-data-YYYY-MM        │
                      │ Primary shards: 5, Replicas: 1   │
                      │ ILM: Hot→Warm→Cold→Delete         │
                      └──────────────────────────────────┘

  Step 4: DOWNSTREAM DELIVERY
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━

  [Downstream Delivery Svc] ──► Polls Kafka topic: downstream.events.v1
                            ──► Applies per-consumer filters/transformations
                            ──► Pushes via Webhook/SSE/WebSocket/Kafka
                            ──► Tracks delivery ACK per consumer
                            ──► Retries with exponential backoff on failure

  Step 5: CLIENT API REQUEST FLOW
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Client ──DNS──► Route53 ──► CloudFront ──► WAF ──► ALB
      ──► API Gateway (JWT validate, rate limit) ──► Router
      ──► Query Service ──► Elasticsearch (for search)
                        ──► Cassandra (for point lookup)
      ──► Response assembled ──► JSON/Protobuf ──► Client

  ---
  6. MICROSERVICES ARCHITECTURE

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                        MICROSERVICES ECOSYSTEM                                      │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
  │  │                         TIER 1: GATEWAY LAYER                               │   │
  │  │  ┌────────────────────────┐    ┌────────────────────────┐                   │   │
  │  │  │   API Gateway Service  │    │   Service Discovery     │                   │   │
  │  │  │   Spring Cloud Gateway │    │   Eureka / K8s DNS      │                   │   │
  │  │  └────────────────────────┘    └────────────────────────┘                   │   │
  │  └─────────────────────────────────────────────────────────────────────────────┘   │
  │                                                                                     │
  │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
  │  │                        TIER 2: SECURITY LAYER                               │   │
  │  │  ┌──────────────────────┐    ┌──────────────────────┐                       │   │
  │  │  │  Authentication Svc  │    │  Authorization Svc   │                       │   │
  │  │  │  OAuth2 + JWT + OIDC │    │  RBAC + OPA Policies │                       │   │
  │  │  └──────────────────────┘    └──────────────────────┘                       │   │
  │  └─────────────────────────────────────────────────────────────────────────────┘   │
  │                                                                                     │
  │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
  │  │                       TIER 3: INGESTION LAYER                               │   │
  │  │  ┌───────────────────┐  ┌────────────────────┐  ┌────────────────────────┐  │   │
  │  │  │ Market Data       │  │ Trade Processing   │  │  Reference Data Svc    │  │   │
  │  │  │ Ingestion Svc     │  │ Ingestion Svc      │  │  + Order Mgmt Svc      │  │   │
  │  │  └───────────────────┘  └────────────────────┘  └────────────────────────┘  │   │
  │  └─────────────────────────────────────────────────────────────────────────────┘   │
  │                                                                                     │
  │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
  │  │                      TIER 4: PROCESSING LAYER                               │   │
  │  │  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐   │   │
  │  │  │ Data Transform  │  │  Enrichment Svc  │  │   Validation Service      │   │   │
  │  │  │ Service         │  │  (Ref data join) │  │   (Business Rules)        │   │   │
  │  │  └─────────────────┘  └──────────────────┘  └──────────────────────────┘   │   │
  │  └─────────────────────────────────────────────────────────────────────────────┘   │
  │                                                                                     │
  │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
  │  │                     TIER 5: PERSISTENCE LAYER                               │   │
  │  │  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────────┐   │   │
  │  │  │ Cassandra Svc   │  │  ES Indexing Svc │  │  DW Sync Service          │   │   │
  │  │  │ (Write + Read)  │  │  (Bulk indexer)  │  │  (S3 + Redshift)          │   │   │
  │  │  └─────────────────┘  └──────────────────┘  └──────────────────────────┘   │   │
  │  └─────────────────────────────────────────────────────────────────────────────┘   │
  │                                                                                     │
  │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
  │  │                       TIER 6: DELIVERY LAYER                                │   │
  │  │  ┌───────────────────┐  ┌────────────────────┐  ┌────────────────────┐     │   │
  │  │  │ Downstream        │  │ Notification Svc   │  │  Reporting Svc     │     │   │
  │  │  │ Delivery Svc      │  │ (WebSocket/SNS)    │  │  (Async reports)   │     │   │
  │  │  └───────────────────┘  └────────────────────┘  └────────────────────┘     │   │
  │  └─────────────────────────────────────────────────────────────────────────────┘   │
  │                                                                                     │
  │  ┌─────────────────────────────────────────────────────────────────────────────┐   │
  │  │                     TIER 7: PLATFORM SERVICES                               │   │
  │  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │   │
  │  │  │ Audit Svc    │  │ Monitoring   │  │ Config Svc   │  │ User Svc     │   │   │
  │  │  │ (Immutable)  │  │ Svc          │  │ (Cloud Cfg)  │  │ (Profile+Pref│   │   │
  │  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘   │   │
  │  └─────────────────────────────────────────────────────────────────────────────┘   │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  6.1 Microservice Specifications

  API Gateway Service

  Responsibility:
    - Single entry point for all client traffic
    - Request routing, load balancing
    - Rate limiting (token bucket: 10K req/sec/client)
    - JWT validation (pre-filter)
    - Request/response logging
    - Circuit breaking to downstream services
    - SSL termination
    - Request transformation (header injection, body transformation)
    - WebSocket proxying for streaming APIs

  APIs Exposed:
    - ALL external-facing routes (proxied)
    - /actuator/gateway/routes (admin)

  Kafka Topics:
    - Publishes to: gateway.access.logs (access log events)
    - Publishes to: rate.limit.exceeded (for alerting)

  Technology:
    - Spring Cloud Gateway (WebFlux-based, non-blocking)
    - Resilience4j CircuitBreaker per route
    - Redis for rate-limit state (distributed token bucket)
    - Spring Security for JWT pre-validation

  Scaling:
    - Stateless — horizontal scale freely
    - HPA: CPU > 60% → scale up, min: 3, max: 20
    - Anti-affinity: spread across AZs

  Circuit Breaker Config:
    slidingWindowSize: 10
    failureRateThreshold: 50%
    waitDurationInOpenState: 30s
    permittedCallsInHalfOpenState: 3

  Sample Route Config (YAML):
    spring:
      cloud:
        gateway:
          routes:
            - id: market-data-route
              uri: lb://market-data-ingestion-svc
              predicates:
                - Path=/api/v1/market/**
                - Method=POST,GET
              filters:
                - name: CircuitBreaker
                  args:
                    name: market-data-cb
                    fallbackUri: forward:/fallback
                - name: RequestRateLimiter
                  args:
                    redis-rate-limiter.replenishRate: 1000
                    redis-rate-limiter.burstCapacity: 2000
                - AddRequestHeader=X-Correlation-ID, #{T(java.util.UUID).randomUUID().toString()}

  Authentication Service

  Responsibility:
    - Issue JWT tokens (Access + Refresh)
    - OAuth2 Authorization Server (Spring Authorization Server)
    - OpenID Connect provider
    - Token validation + introspection
    - Session management
    - MFA enforcement for privileged users

  APIs:
    POST  /oauth2/token          — token issuance
    POST  /oauth2/introspect     — token validation
    POST  /oauth2/revoke         — token revocation
    GET   /oauth2/jwks           — public key endpoint
    GET   /.well-known/openid-configuration

  Database: PostgreSQL (RDS Aurora) for user credentials, sessions
    - Passwords: bcrypt(cost=12) hashed
    - Tokens: stored as SHA-256 hash (never plaintext)

  Kafka:
    - Publishes: auth.events (login, logout, token-issued, mfa-challenge)
    - Publishes: security.alerts (brute-force, anomaly)

  JWT Structure:
    Header: { alg: RS256, kid: <key-id> }
    Payload: {
      sub: userId,
      iss: https://auth.exchange.internal,
      aud: [api-gateway, downstream-svc],
      exp: now + 15min,
      iat: now,
      jti: UUID (for revocation),
      roles: [ROLE_TRADER, ROLE_ADMIN],
      scope: [market:read, trade:write],
      region: plano,
      correlationId: UUID
    }
    Signature: RS256 with 2048-bit RSA key (rotated every 90 days)

  Token Refresh Flow:
    Client ──► POST /oauth2/token (grant_type=refresh_token, refresh_token=<rt>)
    Auth Svc ──► Validate RT (not expired, not revoked, in Redis)
    Auth Svc ──► Issue new AT (15min) + new RT (7 days, rotated)
    Auth Svc ──► Invalidate old RT
    Auth Svc ──► Return new AT + RT

  Circuit Breaker:
    - Auth is synchronous path, but has local token cache (Caffeine)
    - Redis cluster for refresh token store (TTL = token expiry)
    - Fallback: cached public key for offline JWT validation

  Market Data Ingestion Service

  Responsibility:
    - Receive raw market data from 4 upstream sources
    - Protocol adapters: FIX 4.4, FAST, binary, JSON/REST
    - Schema validation against Confluent Schema Registry
    - Deduplication (idempotency key in Redis, TTL=1h)
    - Sequence number validation
    - Publish to Kafka with exactly-once semantics
    - Back-pressure handling via reactive streams

  APIs:
    POST /api/v1/market/quote      — single quote
    POST /api/v1/market/quotes/bulk — batch (up to 10K records)
    POST /api/v1/market/trade      — trade event
    POST /api/v1/market/orderbook  — order book snapshot

  Kafka Topics (Producer):
    - raw.market-data.v1 (partitioned by symbol)
    - raw.trades.v1 (partitioned by symbol+sequence)

  Idempotency Implementation:
    @KafkaListener
    public void process(MarketDataEvent event) {
      String key = event.getSourceId() + ":" + event.getSequenceNum();
      Boolean isNew = redisTemplate.opsForValue()
          .setIfAbsent(key, "1", Duration.ofHours(1));
      if (Boolean.FALSE.equals(isNew)) {
        meterRegistry.counter("events.duplicate").increment();
        return; // already processed
      }
      // proceed with processing
    }

  Kafka Producer Config:
    enable.idempotence=true
    acks=all
    max.in.flight.requests.per.connection=5
    retries=Integer.MAX_VALUE
    delivery.timeout.ms=120000
    linger.ms=5 (micro-batching for throughput)
    batch.size=65536 (64KB)
    compression.type=lz4

  Failure Handling:
    - Retry with exponential backoff (1s, 2s, 4s, 8s, max 3 retries)
    - After 3 retries → publish to DLQ topic: raw.market-data.dlq.v1
    - DLQ processor: manual review + alert + potential replay

  Scaling:
    - HPA on custom metric: kafka_producer_buffer_available_bytes < 20%
    - Scale from 5 → 20 replicas under load

  Data Transformation Service

  Responsibility:
    - Consume from raw.* topics
    - Apply business transformation rules (configurable via Spring Cloud Config)
    - Currency normalization, timezone conversion, decimal precision
    - Enrich with reference data (symbol master, exchange codes)
    - Validate transformed output against target schema
    - Output to transformed.data.v1 topic

  Transformation Pipeline (Spring Batch style):
    Reader (Kafka) → Processor (Transform) → Writer (Kafka)

    ItemProcessor chain:
      1. NormalizationProcessor  — standardize field names, types
      2. EnrichmentProcessor     — join with reference data (local cache)
      3. ValidationProcessor     — apply 50+ business rules
      4. DWFormatProcessor       — convert to DW schema (Parquet-compatible)

  Reference Data Cache:
    - Caffeine local cache (10M entries, 15min TTL)
    - Backed by Cassandra reference.instrument_master table
    - Refreshed via Kafka: reference.data.updates.v1 topic

  State Management:
    - Stateless transformation (no local mutable state)
    - Reference data via read-only cache
    - Exactly-once via Kafka transactions

  Kafka Consumer Config:
    group.id=data-transform-cg
    isolation.level=read_committed (exactly-once)
    max.poll.records=500
    max.poll.interval.ms=300000
    enable.auto.commit=false
    auto.offset.reset=earliest
    session.timeout.ms=45000

  Cassandra Persistence Service

  Responsibility:
    - Consume transformed.data.v1
    - Async batch writes to Cassandra (unlogged batches per partition)
    - Idempotent writes (USING TIMESTAMP idempotency)
    - Write path monitoring (latency, error rate)
    - TTL management per data type
    - Compaction monitoring and triggering

  Write Strategy:
    - Batch size: 100 records per unlogged batch
    - Batching only within same partition (avoid cross-partition batches!)
    - Async execution: ListenableFuture chain
    - Retry: 3 attempts with 100ms, 200ms, 400ms backoff
    - On final failure: publish to cassandra.write.dlq

  Spring Data Cassandra Config:
    spring:
      data:
        cassandra:
          contact-points: cassandra-0,cassandra-1,cassandra-2
          local-datacenter: plano
          consistency-level: LOCAL_QUORUM
          serial-consistency-level: LOCAL_SERIAL
          request:
            timeout: 2000ms
            throttler:
              type: concurrency-based
              max-concurrent-requests: 10000

  Consistency Strategy:
    - Writes: LOCAL_QUORUM (2 of 3 replicas in local DC)
    - Reads: LOCAL_ONE (cache hot data) or LOCAL_QUORUM (stale tolerance = 0)
    - LWT (lightweight transactions): SERIAL for CAS operations

  ---
  7. KAFKA EVENT FLOW DIAGRAM

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                         KAFKA TOPIC TOPOLOGY                                        │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  UPSTREAM SOURCES                                                                   │
  │  ════════════════                                                                   │
  │  [Market Data Svc] ──────────────────────────────► raw.market-data.v1              │
  │  [Trade Svc]       ──────────────────────────────► raw.trades.v1                   │
  │  [Reference Svc]   ──────────────────────────────► raw.reference.v1                │
  │  [Order Mgmt Svc]  ──────────────────────────────► raw.orders.v1                   │
  │                                                                                     │
  │  TOPIC SPECIFICATIONS                                                               │
  │  ════════════════════                                                               │
  │  Topic: raw.market-data.v1                                                          │
  │    Partitions: 64      ← parallelism for 64 consumers max                          │
  │    Replication: 3      ← survive 2 broker failures                                 │
  │    Retention: 7 days   ← 7-day replay window                                       │
  │    Segment: 1GB        ← log segment size                                           │
  │    Partition Key: symbol (lexicographic distribution)                               │
  │    Schema: Avro v1 (Schema Registry ID: 1001)                                       │
  │                                                                                     │
  │  Topic: raw.trades.v1                                                               │
  │    Partitions: 128     ← trades need highest parallelism                           │
  │    Replication: 3                                                                   │
  │    Retention: 30 days  ← regulatory requirement                                     │
  │    Compression: lz4                                                                 │
  │    Partition Key: symbol + exchange_code                                            │
  │                                                                                     │
  │  PROCESSING TOPOLOGY                                                                │
  │  ════════════════════                                                               │
  │                                                                                     │
  │  raw.market-data.v1 ─────────────────────────────────────────────────────────┐     │
  │                       Consumer Group: transform-cg (64 consumers)            │     │
  │                                  │                                           │     │
  │                    ┌─────────────▼─────────────┐                             │     │
  │                    │  Data Transform Service   │                             │     │
  │                    │  - Validate schema         │                             │     │
  │                    │  - Normalize fields        │                             │     │
  │                    │  - Enrich with ref data    │                             │     │
  │                    │  - Apply business rules    │                             │     │
  │                    └─────────────┬─────────────┘                             │     │
  │                                  │                                           │     │
  │                    ┌─────────────▼─────────────┐                             │     │
  │                    │  transformed.data.v1       │                             │     │
  │                    │  Partitions: 64, RF: 3     │                             │     │
  │                    │  Retention: 7 days         │                             │     │
  │                    └────────────┬──────────────┘                             │     │
  │                                 │                                            │     │
  │          ┌──────────────────────┼──────────────────────┐                    │     │
  │          │                      │                      │                    │     │
  │   ┌──────▼──────┐      ┌────────▼──────┐      ┌───────▼────────┐           │     │
  │   │ cassandra   │      │  es.index     │      │  dw.sync       │           │     │
  │   │ write cg   │      │  cg           │      │  cg            │           │     │
  │   └──────┬──────┘      └────────┬──────┘      └───────┬────────┘           │     │
  │          │                      │                      │                    │     │
  │   [Cassandra]            [Elasticsearch]         [S3 / DW]                  │     │
  │                                                                              │     │
  │  DEAD LETTER QUEUE FLOW                                                      │     │
  │  ═══════════════════════                                                     │     │
  │  Any failure → retry.market-data.v1 (3 retries, exponential backoff)        │     │
  │  After 3 retries → dlq.market-data.v1 (human review + alerting)             │     │
  │                                                                              │     │
  │  DLQ Topic Config:                                                           │     │
  │    Retention: 30 days (for investigation)                                    │     │
  │    Alerting: PagerDuty on DLQ lag > 100 messages                             │     │
  │    Replay: Automated replay service (DLQReplayService) with manual trigger  │     │
  │                                                                              └─────│
  │  AUDIT FLOW                                                                        │
  │  ══════════                                                                        │
  │  All topics ──► audit.events.v1 (Kafka Streams forward-all pattern)               │
  │  audit.events.v1 ──► Cassandra audit_log table (immutable, TTL=7 years)           │
  │  audit.events.v1 ──► S3 (long-term cold storage for compliance)                   │
  │                                                                                    │
  │  EXACTLY-ONCE SEMANTICS IMPLEMENTATION                                             │
  │  ════════════════════════════════════                                              │
  │  Producer side:                                                                    │
  │    enable.idempotence=true → assigns ProducerID + SequenceNumber                  │
  │    transactional.id=market-data-producer-{pod-name}                               │
  │    Kafka deduplicates within: max.in.flight.requests = 5                          │
  │                                                                                    │
  │  Consumer side:                                                                    │
  │    isolation.level=read_committed                                                  │
  │    Manual offset commit after successful DB write                                 │
  │    Transactional: Kafka offset + DB write in same transaction                     │
  │                                                                                    │
  │  ORDERING GUARANTEE                                                                │
  │  ══════════════════                                                                │
  │  - All events for same symbol go to same partition (key=symbol)                   │
  │  - Within partition: strict ordering guaranteed                                   │
  │  - Across partitions: event time ordering via watermarks (Kafka Streams)          │
  │  - Sequence number validation: reject out-of-order within same source             │
  │                                                                                    │
  └────────────────────────────────────────────────────────────────────────────────────┘

  Kafka Consumer Lag Monitoring

  Metric Pipeline:
    Kafka JMX ──► Prometheus JMX Exporter ──► Prometheus ──► Grafana

    Critical Metrics:
      kafka_consumer_fetch_manager_records_lag_max  (by group, topic, partition)
      kafka_producer_record_send_rate
      kafka_producer_record_error_rate
      kafka_network_io_wait_time_ns_avg

    Alerting Rules:
      ALERT HighConsumerLag
        IF kafka_consumer_lag_sum{group="transform-cg"} > 10000
        FOR 2m
        → PagerDuty P1

      ALERT DLQMessagesIncreasing
        IF rate(kafka_messages_in_total{topic=~"dlq.*"}[5m]) > 0
        FOR 1m
        → Slack + PagerDuty

  ---
  8. CASSANDRA CLUSTER ARCHITECTURE

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                      CASSANDRA CLUSTER TOPOLOGY                                     │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  PLANO DATACENTER (DC1)              HAZELWOOD DATACENTER (DC2)                    │
  │  ═══════════════════════             ═══════════════════════════                   │
  │                                                                                     │
  │  ┌──────────────────────┐            ┌──────────────────────┐                      │
  │  │  Rack 1 (AZ-a)       │            │  Rack 1 (AZ-a)       │                      │
  │  │  ┌────────────────┐  │            │  ┌────────────────┐  │                      │
  │  │  │  cass-plano-1  │  │            │  │  cass-hzw-1    │  │                      │
  │  │  │  256GB RAM     │  │            │  │  256GB RAM     │  │                      │
  │  │  │  32 vCPU       │  │            │  │  32 vCPU       │  │                      │
  │  │  │  10TB NVMe SSD │  │            │  │  10TB NVMe SSD │  │                      │
  │  │  └────────────────┘  │            │  └────────────────┘  │                      │
  │  │                      │            │                      │                      │
  │  │  Rack 2 (AZ-b)       │            │  Rack 2 (AZ-b)       │                      │
  │  │  ┌────────────────┐  │            │  ┌────────────────┐  │                      │
  │  │  │  cass-plano-2  │  │            │  │  cass-hzw-2    │  │                      │
  │  │  └────────────────┘  │ ◄──────────► │  └────────────────┘  │                    │
  │  │                      │  Multi-DC   │                      │                      │
  │  │  Rack 3 (AZ-c)       │  Replication│  Rack 3 (AZ-c)       │                      │
  │  │  ┌────────────────┐  │            │  ┌────────────────┐  │                      │
  │  │  │  cass-plano-3  │  │            │  │  cass-hzw-3    │  │                      │
  │  │  └────────────────┘  │            │  └────────────────┘  │                      │
  │  └──────────────────────┘            └──────────────────────┘                      │
  │                                                                                     │
  │  Replication Factor: 3 per DC (total RF=6 across both DCs)                        │
  │  Strategy: NetworkTopologyStrategy {'plano': 3, 'hazelwood': 3}                   │
  │                                                                                     │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  Cassandra Data Modeling

  -- KEYSPACE CREATION
  CREATE KEYSPACE market_data
    WITH replication = {
      'class': 'NetworkTopologyStrategy',
      'plano': '3',
      'hazelwood': '3'
    }
    AND durable_writes = true;

  -- TABLE 1: Market Quotes (Hot read path)
  -- Access pattern: "Get all quotes for symbol AAPL in last 24 hours"
  CREATE TABLE market_data.quotes_by_symbol (
      symbol          TEXT,
      trade_date      DATE,           -- bucket by day (avoids partition explosion)
      event_time      TIMEUUID,       -- unique, time-sortable
      source_id       TEXT,
      bid_price       DECIMAL,
      ask_price       DECIMAL,
      last_price      DECIMAL,
      volume          BIGINT,
      exchange_code   TEXT,
      currency        TEXT,
      condition_code  TEXT,
      sequence_num    BIGINT,
      ingested_at     TIMESTAMP,
      PRIMARY KEY ((symbol, trade_date), event_time)
  ) WITH CLUSTERING ORDER BY (event_time DESC)
    AND default_time_to_live = 7776000  -- 90 days TTL
    AND compaction = {
      'class': 'TimeWindowCompactionStrategy',
      'compaction_window_unit': 'DAYS',
      'compaction_window_size': 1
    }
    AND gc_grace_seconds = 86400        -- 1 day (short because time-series)
    AND caching = {'keys': 'ALL', 'rows_per_partition': '100'};

  -- TABLE 2: Trade Events (Compliance + Analytics)
  CREATE TABLE market_data.trades_by_symbol (
      symbol          TEXT,
      trade_date      DATE,
      trade_id        UUID,
      event_time      TIMEUUID,
      trade_price     DECIMAL,
      trade_volume    BIGINT,
      buyer_id        TEXT,
      seller_id       TEXT,
      exchange_code   TEXT,
      trade_type      TEXT,           -- REGULAR, BLOCK, SWEEP
      settlement_date DATE,
      status          TEXT,
      PRIMARY KEY ((symbol, trade_date), event_time, trade_id)
  ) WITH CLUSTERING ORDER BY (event_time DESC, trade_id ASC)
    AND default_time_to_live = 31536000 -- 1 year
    AND compaction = {'class': 'TimeWindowCompactionStrategy',
                      'compaction_window_unit': 'DAYS',
                      'compaction_window_size': 1};

  -- TABLE 3: Trade by Trade ID (lookup by ID)
  -- Denormalized for different access pattern
  CREATE TABLE market_data.trades_by_id (
      trade_id        UUID,
      symbol          TEXT,
      trade_date      DATE,
      event_time      TIMESTAMP,
      trade_price     DECIMAL,
      trade_volume    BIGINT,
      buyer_id        TEXT,
      seller_id       TEXT,
      exchange_code   TEXT,
      status          TEXT,
      PRIMARY KEY (trade_id)
  );

  -- TABLE 4: Order Book Snapshots
  CREATE TABLE market_data.order_book_snapshots (
      symbol          TEXT,
      snapshot_time   TIMEUUID,
      bids            LIST<FROZEN<price_level>>,  -- UDT
      asks            LIST<FROZEN<price_level>>,
      sequence_num    BIGINT,
      source          TEXT,
      PRIMARY KEY (symbol, snapshot_time)
  ) WITH CLUSTERING ORDER BY (snapshot_time DESC)
    AND default_time_to_live = 86400    -- 24h TTL (order books are transient)
    AND compaction = {'class': 'TimeWindowCompactionStrategy',
                      'compaction_window_unit': 'HOURS',
                      'compaction_window_size': 1};

  -- UDT for price levels
  CREATE TYPE market_data.price_level (
      price    DECIMAL,
      quantity BIGINT,
      orders   INT
  );

  -- TABLE 5: Daily Summary (pre-aggregated for performance)
  CREATE TABLE market_data.daily_summary (
      symbol          TEXT,
      trade_date      DATE,
      open_price      DECIMAL,
      high_price      DECIMAL,
      low_price       DECIMAL,
      close_price     DECIMAL,
      total_volume    BIGINT,
      trade_count     INT,
      vwap            DECIMAL,
      prev_close      DECIMAL,
      change_pct      DECIMAL,
      last_updated    TIMESTAMP,
      PRIMARY KEY (symbol, trade_date)
  ) WITH CLUSTERING ORDER BY (trade_date DESC);

  -- TABLE 6: Audit Log (Immutable, Wide rows)
  CREATE TABLE market_data.audit_log (
      service_name    TEXT,
      audit_date      DATE,
      event_id        TIMEUUID,
      event_type      TEXT,
      user_id         TEXT,
      entity_type     TEXT,
      entity_id       TEXT,
      before_state    TEXT,   -- JSON
      after_state     TEXT,   -- JSON
      ip_address      TEXT,
      correlation_id  TEXT,
      PRIMARY KEY ((service_name, audit_date), event_id)
  ) WITH CLUSTERING ORDER BY (event_id DESC)
    AND default_time_to_live = 220752000  -- 7 years (SEC Rule 17a-4)
    AND compaction = {'class': 'TimeWindowCompactionStrategy',
                      'compaction_window_unit': 'DAYS',
                      'compaction_window_size': 7};

  Cassandra Write/Read Path

  WRITE PATH:
  ═══════════
  Client Write Request
         │
         ▼
  Coordinator Node (hash token ring, find target nodes)
         │
         ├──► Node A (owns partition) ── Commit Log (WAL) ──► Memtable ──► Flush ──► SSTable
         ├──► Node B (replica 1)     ── Commit Log         ──► Memtable
         └──► Node C (replica 2)     ── Commit Log         ──► Memtable

  Consistency LOCAL_QUORUM = wait for 2/3 nodes to ACK before returning to client

  COMPACTION (TWCS - Time Window Compaction Strategy):
  - Merges SSTables within same time window
  - Older windows become read-only (no further compaction across windows)
  - Minimizes read amplification for time-series
  - Window size = 1 day matches our daily query pattern

  READ PATH:
  ══════════
  Client Read Request
         │
         ▼
  Coordinator Node
         │
         ├──► Check Row Cache (hit → return immediately)
         │
         ├──► If miss: query N replicas (LOCAL_QUORUM = 2)
         │         ├──► Node A: check Memtable → Row Cache → Bloom Filter → Key Cache → SSTable
         │         └──► Node B: same
         │
         ├──► Digest comparison (repair if mismatch → Read Repair async)
         │
         └──► Return newest data based on timestamp

  Tombstone and GC Grace Management

  Problem: Tombstones accumulate on DELETE operations.
           Reading through tombstones = query slowdown.
           tombstone_warn_threshold: 1000
           tombstone_failure_threshold: 100000

  Mitigation Strategies:
    1. Use TTL instead of explicit DELETE (TTL creates expiring tombstones that auto-GC)
    2. TWCS ensures tombstone GC happens within 1 window
    3. gc_grace_seconds=86400 (1 day) for time-series tables (fast tombstone reaping)
    4. Monitor: nodetool tpstats | grep Tombstone
    5. Alert: Prometheus metric cassandra_table_tombstone_scanned_histogram > 5000

  Hot Partition Detection and Mitigation:
    Problem: All AAPL quotes hit the same partition → one node becomes bottleneck

    Solution 1: Time Bucketing
      PRIMARY KEY ((symbol, trade_date), event_time)
      -- Spreads AAPL across 252 partitions/year (one per trading day)

    Solution 2: Salting for extreme cases
      salt = sequence_num % 4  -- 4 sub-partitions
      PRIMARY KEY ((symbol, trade_date, salt), event_time)
      -- Application reads from all 4 salts and merges

    Detection:
      nodetool toppartitions market_data quotes_by_symbol 60
      — shows hottest partitions over 60 seconds

  ---
  9. ELASTICSEARCH CLUSTER ARCHITECTURE

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                    ELASTICSEARCH CLUSTER TOPOLOGY                                   │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  PLANO REGION CLUSTER                                                               │
  │  ════════════════════                                                               │
  │                                                                                     │
  │  ┌───────────────────────────────────────────────────────────────────────────┐     │
  │  │                     Dedicated Master Nodes (3)                            │     │
  │  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                │     │
  │  │  │  es-master-1 │    │  es-master-2 │    │  es-master-3 │                │     │
  │  │  │  16GB RAM    │    │  16GB RAM    │    │  16GB RAM    │                │     │
  │  │  │  4 vCPU      │    │  4 vCPU      │    │  4 vCPU      │                │     │
  │  │  │  No data     │    │  No data     │    │  No data     │                │     │
  │  │  └──────────────┘    └──────────────┘    └──────────────┘                │     │
  │  │  (3 masters = quorum 2 → survive 1 master failure)                       │     │
  │  └───────────────────────────────────────────────────────────────────────────┘     │
  │                                                                                     │
  │  ┌───────────────────────────────────────────────────────────────────────────┐     │
  │  │                     Hot Data Nodes (3) — Active trading data              │     │
  │  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                │     │
  │  │  │  es-hot-1    │    │  es-hot-2    │    │  es-hot-3    │                │     │
  │  │  │  128GB RAM   │    │  128GB RAM   │    │  128GB RAM   │                │     │
  │  │  │  16 vCPU     │    │  16 vCPU     │    │  16 vCPU     │                │     │
  │  │  │  2TB NVMe    │    │  2TB NVMe    │    │  2TB NVMe    │                │     │
  │  │  │  node.attr   │    │  node.attr   │    │  node.attr   │                │     │
  │  │  │  .temp=hot   │    │  .temp=hot   │    │  .temp=hot   │                │     │
  │  │  └──────────────┘    └──────────────┘    └──────────────┘                │     │
  │  │  Holds: current month + last month indices                               │     │
  │  └───────────────────────────────────────────────────────────────────────────┘     │
  │                                                                                     │
  │  ┌───────────────────────────────────────────────────────────────────────────┐     │
  │  │                     Warm Data Nodes (3) — Historical data                 │     │
  │  │  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                │     │
  │  │  │  es-warm-1   │    │  es-warm-2   │    │  es-warm-3   │                │     │
  │  │  │  64GB RAM    │    │  64GB RAM    │    │  64GB RAM    │                │     │
  │  │  │  16 vCPU     │    │  16 vCPU     │    │  16 vCPU     │                │     │
  │  │  │  20TB HDD    │    │  20TB HDD    │    │  20TB HDD    │                │     │
  │  │  │  node.attr   │    │  node.attr   │    │  node.attr   │                │     │
  │  │  │  .temp=warm  │    │  .temp=warm  │    │  .temp=warm  │                │     │
  │  │  └──────────────┘    └──────────────┘    └──────────────┘                │     │
  │  │  Holds: 2+ months historical, read-only, replicas=0 (cost saving)       │     │
  │  └───────────────────────────────────────────────────────────────────────────┘     │
  │                                                                                     │
  │  ┌───────────────────────────────────────────────────────────────────────────┐     │
  │  │               Coordinating-Only Nodes (2) — Query routing                 │     │
  │  │  ┌──────────────────────────┐    ┌──────────────────────────┐            │     │
  │  │  │  es-coord-1              │    │  es-coord-2              │            │     │
  │  │  │  32GB RAM, 8 vCPU        │    │  32GB RAM, 8 vCPU        │            │     │
  │  │  │  No data, just routing   │    │  No data, just routing   │            │     │
  │  │  └──────────────────────────┘    └──────────────────────────┘            │     │
  │  └───────────────────────────────────────────────────────────────────────────┘     │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  Elasticsearch Index Design

  // Index Template: market-data-*
  PUT _index_template/market_data_template
  {
    "index_patterns": ["market-data-*"],
    "template": {
      "settings": {
        "number_of_shards": 5,
        "number_of_replicas": 1,
        "refresh_interval": "1s",
        "index.routing.allocation.require.temp": "hot",
        "index.codec": "best_compression",
        "index.translog.durability": "async",
        "index.translog.sync_interval": "5s",
        "analysis": {
          "analyzer": {
            "symbol_analyzer": {
              "type": "custom",
              "tokenizer": "standard",
              "filter": ["lowercase", "asciifolding"]
            }
          }
        }
      },
      "mappings": {
        "dynamic": "strict",
        "properties": {
          "symbol": {
            "type": "keyword",
            "doc_values": true
          },
          "exchange_code": { "type": "keyword" },
          "trade_date": { "type": "date", "format": "yyyy-MM-dd" },
          "event_time": { "type": "date", "format": "epoch_millis" },
          "bid_price": { "type": "double" },
          "ask_price": { "type": "double" },
          "last_price": { "type": "double" },
          "volume": { "type": "long" },
          "trade_type": { "type": "keyword" },
          "currency": { "type": "keyword" },
          "source_id": { "type": "keyword" },
          "correlation_id": { "type": "keyword" },
          "description": {
            "type": "text",
            "analyzer": "standard",
            "search_analyzer": "standard",
            "fields": {
              "keyword": { "type": "keyword", "ignore_above": 256 }
            }
          }
        }
      }
    },
    "composed_of": ["market_data_ilm_settings"]
  }

  // ILM Policy: Time-based index lifecycle
  PUT _ilm/policy/market_data_ilm
  {
    "policy": {
      "phases": {
        "hot": {
          "min_age": "0ms",
          "actions": {
            "rollover": {
              "max_age": "1d",
              "max_size": "50gb",
              "max_docs": 100000000
            },
            "set_priority": { "priority": 100 }
          }
        },
        "warm": {
          "min_age": "30d",
          "actions": {
            "allocate": {
              "require": { "temp": "warm" },
              "number_of_replicas": 0
            },
            "forcemerge": { "max_num_segments": 1 },
            "set_priority": { "priority": 50 }
          }
        },
        "cold": {
          "min_age": "90d",
          "actions": {
            "freeze": {},
            "set_priority": { "priority": 0 }
          }
        },
        "delete": {
          "min_age": "365d",
          "actions": { "delete": {} }
        }
      }
    }
  }

  Elasticsearch Query Patterns

  // Spring Data Elasticsearch — Complex query example
  @Repository
  public class MarketDataSearchRepository {

      @Autowired
      private ElasticsearchOperations elasticsearchOperations;

      public Page<MarketDataDocument> searchWithAggregations(
              String symbol, DateRange range, Pageable pageable) {

          NativeQuery query = NativeQuery.builder()
              .withQuery(q -> q
                  .bool(b -> b
                      .filter(f -> f.term(t -> t.field("symbol").value(symbol)))
                      .filter(f -> f.range(r -> r
                          .field("event_time")
                          .from(range.getFrom().toEpochMilli())
                          .to(range.getTo().toEpochMilli())
                      ))
                      .mustNot(m -> m.term(t -> t.field("trade_type").value("CANCEL")))
                  )
              )
              .withAggregation("price_stats",
                  Aggregation.of(a -> a.stats(s -> s.field("last_price"))))
              .withAggregation("volume_per_hour",
                  Aggregation.of(a -> a.dateHistogram(dh -> dh
                      .field("event_time")
                      .calendarInterval(CalendarInterval.Hour)
                      .subAggregations(Map.of(
                          "total_volume", Aggregation.of(a2 ->
                              a2.sum(s -> s.field("volume")))
                      ))
                  )))
              .withPageable(pageable)
              .withSort(s -> s.field(f -> f
                  .field("event_time").order(SortOrder.Desc)))
              .build();

          SearchHits<MarketDataDocument> hits =
              elasticsearchOperations.search(query, MarketDataDocument.class);

          return SearchHitSupport.searchPageFor(hits, pageable);
      }
  }

  ---
  10. KUBERNETES DEPLOYMENT ARCHITECTURE

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                      EKS CLUSTER ARCHITECTURE                                       │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  CONTROL PLANE (AWS Managed)                                                        │
  │  ┌─────────────────────────────────────────────────────────────────────────┐       │
  │  │  API Server  │  etcd (3-node)  │  Scheduler  │  Controller Manager     │       │
  │  └─────────────────────────────────────────────────────────────────────────┘       │
  │                                                                                     │
  │  NODE GROUPS                                                                        │
  │  ┌──────────────────────────────────────────────────────────────────────────┐      │
  │  │  Node Group: system  (3 nodes, m5.xlarge)                                │      │
  │  │  Workloads: ingress, cert-manager, external-dns, cluster-autoscaler      │      │
  │  │  Taints: role=system:NoSchedule                                          │      │
  │  └──────────────────────────────────────────────────────────────────────────┘      │
  │                                                                                     │
  │  ┌──────────────────────────────────────────────────────────────────────────┐      │
  │  │  Node Group: application  (9 nodes, m5.4xlarge, 16 vCPU / 64GB RAM)     │      │
  │  │  Workloads: all microservices                                             │      │
  │  │  Auto Scaling: min=9, max=30 (Cluster Autoscaler + Karpenter)            │      │
  │  └──────────────────────────────────────────────────────────────────────────┘      │
  │                                                                                     │
  │  ┌──────────────────────────────────────────────────────────────────────────┐      │
  │  │  Node Group: compute-intensive  (3 nodes, c5.9xlarge, 36 vCPU / 72GB)  │      │
  │  │  Workloads: data-transform, kafka-consumers (CPU intensive)              │      │
  │  │  Node Affinity: only transform pods                                      │      │
  │  └──────────────────────────────────────────────────────────────────────────┘      │
  │                                                                                     │
  │  NETWORKING                                                                         │
  │  ┌──────────────────────────────────────────────────────────────────────────┐      │
  │  │  CNI: AWS VPC CNI (native VPC IPs for pods)                              │      │
  │  │  Service Mesh: Istio 1.19 (mTLS, traffic management, observability)      │      │
  │  │  Network Policies: Calico (deny-all default, explicit allow rules)        │      │
  │  └──────────────────────────────────────────────────────────────────────────┘      │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  Sample Kubernetes Manifests

  # Deployment: Market Data Ingestion Service
  apiVersion: apps/v1
  kind: Deployment
  metadata:
    name: market-data-ingestion
    namespace: data-ingestion
    labels:
      app: market-data-ingestion
      version: "1.5.0"
      tier: ingestion
  spec:
    replicas: 5
    revisionHistoryLimit: 5
    selector:
      matchLabels:
        app: market-data-ingestion
    strategy:
      type: RollingUpdate
      rollingUpdate:
        maxSurge: 2
        maxUnavailable: 0       # Zero downtime: never kill before new is ready
    template:
      metadata:
        labels:
          app: market-data-ingestion
          version: "1.5.0"
        annotations:
          prometheus.io/scrape: "true"
          prometheus.io/port: "8080"
          prometheus.io/path: "/actuator/prometheus"
          sidecar.istio.io/inject: "true"
      spec:
        serviceAccountName: market-data-sa
        terminationGracePeriodSeconds: 60     # Allow in-flight requests to complete

        affinity:
          podAntiAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  app: market-data-ingestion
              topologyKey: kubernetes.io/hostname   # One pod per node
          nodeAffinity:
            requiredDuringSchedulingIgnoredDuringExecution:
              nodeSelectorTerms:
              - matchExpressions:
                - key: node-group
                  operator: In
                  values: [application, compute-intensive]

        topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              app: market-data-ingestion

        containers:
        - name: market-data-ingestion
          image: 123456789.dkr.ecr.us-east-1.amazonaws.com/market-data-ingestion:1.5.0
          imagePullPolicy: IfNotPresent
          ports:
          - name: http
            containerPort: 8080
            protocol: TCP
          - name: management
            containerPort: 8081
            protocol: TCP

          resources:
            requests:
              cpu: "2"
              memory: "4Gi"
            limits:
              cpu: "4"
              memory: "8Gi"

          env:
          - name: SPRING_PROFILES_ACTIVE
            value: "kubernetes,plano"
          - name: POD_NAME
            valueFrom:
              fieldRef:
                fieldPath: metadata.name
          - name: POD_NAMESPACE
            valueFrom:
              fieldRef:
                fieldPath: metadata.namespace
          - name: KAFKA_BOOTSTRAP_SERVERS
            valueFrom:
              secretKeyRef:
                name: kafka-credentials
                key: bootstrap-servers

          envFrom:
          - configMapRef:
              name: market-data-config
          - secretRef:
              name: market-data-secrets

          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: 8081
            initialDelaySeconds: 30
            periodSeconds: 10
            failureThreshold: 3
            successThreshold: 1

          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: 8081
            initialDelaySeconds: 60
            periodSeconds: 20
            failureThreshold: 3

          startupProbe:
            httpGet:
              path: /actuator/health/liveness
              port: 8081
            initialDelaySeconds: 10
            periodSeconds: 5
            failureThreshold: 30    # 30 * 5s = 150s startup budget

          lifecycle:
            preStop:
              exec:
                command: ["sh", "-c", "sleep 5"]  # Allow load balancer to drain

          volumeMounts:
          - name: app-config
            mountPath: /config
            readOnly: true
          - name: tmp-dir
            mountPath: /tmp

        volumes:
        - name: app-config
          configMap:
            name: market-data-config
        - name: tmp-dir
          emptyDir: {}

  ---
  # HPA: Horizontal Pod Autoscaler
  apiVersion: autoscaling/v2
  kind: HorizontalPodAutoscaler
  metadata:
    name: market-data-ingestion-hpa
    namespace: data-ingestion
  spec:
    scaleTargetRef:
      apiVersion: apps/v1
      kind: Deployment
      name: market-data-ingestion
    minReplicas: 5
    maxReplicas: 20
    metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 65
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 75
    - type: External
      external:
        metric:
          name: kafka_consumer_lag_sum
          selector:
            matchLabels:
              kafka_consumer_group: "market-data-ingest-cg"
        target:
          type: AverageValue
          averageValue: "1000"        # Scale when per-replica lag > 1000
    behavior:
      scaleUp:
        stabilizationWindowSeconds: 60
        policies:
        - type: Pods
          value: 3
          periodSeconds: 60         # Max add 3 pods per minute
      scaleDown:
        stabilizationWindowSeconds: 300  # Wait 5 min before scale-down
        policies:
        - type: Pods
          value: 1
          periodSeconds: 120

  ---
  # PodDisruptionBudget: Ensure availability during node drain
  apiVersion: policy/v1
  kind: PodDisruptionBudget
  metadata:
    name: market-data-ingestion-pdb
    namespace: data-ingestion
  spec:
    minAvailable: 3                   # Always maintain 3 running pods
    selector:
      matchLabels:
        app: market-data-ingestion

  Blue-Green Deployment Strategy

  # Argo Rollouts — Blue-Green
  apiVersion: argoproj.io/v1alpha1
  kind: Rollout
  metadata:
    name: market-data-ingestion
    namespace: data-ingestion
  spec:
    replicas: 5
    strategy:
      blueGreen:
        activeService: market-data-active-svc
        previewService: market-data-preview-svc
        autoPromotionEnabled: false         # Manual promotion after testing
        scaleDownDelaySeconds: 30
        prePromotionAnalysis:
          templates:
          - templateName: success-rate-analysis
          args:
          - name: service-name
            value: market-data-preview-svc
    selector:
      matchLabels:
        app: market-data-ingestion
    template:
      # ... (pod template same as above)

  ---
  # Canary Deployment with traffic shifting
  apiVersion: argoproj.io/v1alpha1
  kind: Rollout
  metadata:
    name: trade-processing-svc
  spec:
    strategy:
      canary:
        canaryService: trade-processing-canary-svc
        stableService: trade-processing-stable-svc
        trafficRouting:
          istio:
            virtualService:
              name: trade-processing-vsvc
              routes:
              - primary
        steps:
        - setWeight: 5           # 5% to canary
        - pause: {duration: 5m}
        - analysis:
            templates:
            - templateName: canary-analysis
        - setWeight: 25
        - pause: {duration: 10m}
        - setWeight: 50
        - pause: {duration: 10m}
        - setWeight: 100

  ---
  11. AWS CLOUD ARCHITECTURE

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                          AWS MULTI-REGION ARCHITECTURE                              │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  GLOBAL LAYER                                                                       │
  │  ┌───────────────────────────────────────────────────────────────────────────┐     │
  │  │  Route53 (Latency-based routing + Health checks)                         │     │
  │  │    → us-east-1 (Plano Region)    → us-west-2 (Hazelwood Region)          │     │
  │  │  CloudFront (Global CDN, edge caching for static API responses)           │     │
  │  │  AWS Shield Advanced (DDoS protection Layer 3/4/7)                       │     │
  │  │  AWS WAF (OWASP rule sets, IP reputation, rate limiting)                 │     │
  │  └───────────────────────────────────────────────────────────────────────────┘     │
  │                                                                                     │
  │  US-EAST-1 (PLANO) — PRIMARY                                                        │
  │  ┌───────────────────────────────────────────────────────────────────────────┐     │
  │  │                                                                           │     │
  │  │  VPC: 10.0.0.0/16                                                        │     │
  │  │  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐  │     │
  │  │  │ Public Subnet AZ-a │  │ Public Subnet AZ-b │  │ Public Subnet AZ-c │  │     │
  │  │  │ 10.0.1.0/24        │  │ 10.0.2.0/24        │  │ 10.0.3.0/24        │  │     │
  │  │  │ [ALB / NAT GW]     │  │ [ALB / NAT GW]     │  │ [ALB / NAT GW]     │  │     │
  │  │  └────────────────────┘  └────────────────────┘  └────────────────────┘  │     │
  │  │                                                                           │     │
  │  │  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐  │     │
  │  │  │ App Subnet AZ-a    │  │ App Subnet AZ-b    │  │ App Subnet AZ-c    │  │     │
  │  │  │ 10.0.11.0/24       │  │ 10.0.12.0/24       │  │ 10.0.13.0/24       │  │     │
  │  │  │ [EKS Worker Nodes] │  │ [EKS Worker Nodes] │  │ [EKS Worker Nodes] │  │     │
  │  │  └────────────────────┘  └────────────────────┘  └────────────────────┘  │     │
  │  │                                                                           │     │
  │  │  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐  │     │
  │  │  │ Data Subnet AZ-a   │  │ Data Subnet AZ-b   │  │ Data Subnet AZ-c   │  │     │
  │  │  │ 10.0.21.0/24       │  │ 10.0.22.0/24       │  │ 10.0.23.0/24       │  │     │
  │  │  │ [Cassandra Nodes]  │  │ [ES Nodes]         │  │ [MSK Brokers]      │  │     │
  │  │  └────────────────────┘  └────────────────────┘  └────────────────────┘  │     │
  │  │                                                                           │     │
  │  │  AWS SERVICES:                                                            │     │
  │  │  ┌─────────────────────────────────────────────────────────────────┐     │     │
  │  │  │ MSK (Kafka): 3 broker instances, msk.m5.4xlarge, Multi-AZ      │     │     │
  │  │  │ EKS: Managed control plane, 20 worker nodes across 3 AZs        │     │     │
  │  │  │ Cassandra: EC2 i3en.3xlarge (NVMe) in StatefulSets on EKS       │     │     │
  │  │  │ OpenSearch: 3 master + 6 data nodes (managed)                   │     │     │
  │  │  │ ElastiCache Redis: Cluster mode, 3 shards x 2 replicas          │     │     │
  │  │  │ RDS Aurora: Multi-AZ, auth service DB                           │     │     │
  │  │  │ S3: Datalake (versioned, cross-region replication)              │     │     │
  │  │  │ Secrets Manager: All credentials, auto-rotation                 │     │     │
  │  │  │ KMS: CMK for all encryption at rest                             │     │     │
  │  │  │ CloudWatch: Logs, metrics, alarms                               │     │     │
  │  │  │ X-Ray: Distributed tracing                                      │     │     │
  │  │  │ SQS: Overflow buffer for Lambda triggers                        │     │     │
  │  │  │ Lambda: DLQ processing, scheduled tasks, compliance reports     │     │     │
  │  │  │ ECR: Container registry (immutable tags, vulnerability scanning)│     │     │
  │  │  │ IAM: IRSA (IAM Roles for Service Accounts) — no static keys     │     │     │
  │  │  │ Systems Manager Parameter Store: Non-sensitive config           │     │     │
  │  │  │ Certificate Manager: TLS certificates (auto-renewed)            │     │     │
  │  │  └─────────────────────────────────────────────────────────────────┘     │     │
  │  └───────────────────────────────────────────────────────────────────────────┘     │
  │                                                                                     │
  │  US-WEST-2 (HAZELWOOD) — SECONDARY                                                  │
  │  ┌───────────────────────────────────────────────────────────────────────────┐     │
  │  │  Mirror of Plano (reduced capacity: 8 EKS nodes)                         │     │
  │  │  Active for reads, passive for writes                                     │     │
  │  │  Cassandra: NetworkTopologyStrategy replication from Plano               │     │
  │  │  MSK: MirrorMaker2 replicating all topics (async, <1s lag)               │     │
  │  │  RDS: Aurora Global Database (cross-region replica <1s lag)              │     │
  │  │  S3: Cross-region replication (CRR) enabled                              │     │
  │  └───────────────────────────────────────────────────────────────────────────┘     │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  ---
  12. SECURITY ARCHITECTURE

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                        SECURITY ARCHITECTURE (DEFENSE IN DEPTH)                     │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  LAYER 1: PERIMETER                                                                  │
  │  ════════════════════                                                               │
  │  [Internet] ──► CloudFront ──► AWS Shield Advanced (L3/L4 DDoS)                    │
  │             ──► AWS WAF (L7):                                                       │
  │                 - OWASP Core Rule Set 3.2                                           │
  │                 - SQL Injection prevention                                          │
  │                 - XSS prevention                                                    │
  │                 - IP reputation (AWS Managed rule group)                            │
  │                 - Rate-based rules: 10K req/5min per IP                            │
  │                 - Bot control managed rules                                         │
  │                 - Geo-blocking: restrict to allowed countries                      │
  │                                                                                     │
  │  LAYER 2: NETWORK                                                                    │
  │  ════════════════                                                                   │
  │  - VPC Security Groups: Least privilege (allow only necessary ports/protocols)     │
  │  - Network ACLs: Subnet-level stateless firewall                                   │
  │  - VPC Flow Logs: All traffic logged to S3 + CloudWatch                            │
  │  - PrivateLink: No public internet for internal service communication              │
  │  - Transit Gateway: Controlled inter-VPC routing (no full mesh)                    │
  │  - VPN/Direct Connect: For upstream partner connectivity                           │
  │                                                                                     │
  │  LAYER 3: SERVICE MESH (mTLS)                                                        │
  │  ════════════════════════════                                                       │
  │  - Istio: All pod-to-pod communication encrypted with mTLS                         │
  │  - SPIFFE/SPIRE: Cryptographic service identity                                    │
  │  - Certificate rotation: Automatic every 24h                                       │
  │  - Authorization Policies: ALLOW only known service accounts                       │
  │                                                                                     │
  │  LAYER 4: APPLICATION (Authentication + Authorization)                              │
  │  ═══════════════════════════════════════════════════════                            │
  │  Authentication:                                                                    │
  │    Client ──► API Gateway ──► Validate JWT (RS256)                                  │
  │    Check: exp, iss, aud, jti not in revocation list (Redis)                         │
  │    Extract: sub, roles, scope                                                       │
  │                                                                                     │
  │  Authorization (OPA — Open Policy Agent):                                           │
  │    API Gateway calls OPA sidecar:                                                   │
  │    {                                                                                │
  │      "input": {                                                                     │
  │        "user": { "id": "u123", "roles": ["ROLE_TRADER"] },                         │
  │        "resource": { "method": "POST", "path": "/api/v1/trades" },                 │
  │        "action": "write"                                                            │
  │      }                                                                              │
  │    }                                                                                │
  │    OPA Policy (Rego):                                                               │
  │    allow {                                                                          │
  │      input.user.roles[_] == "ROLE_TRADER"                                          │
  │      input.resource.method == "POST"                                                │
  │      glob.match("/api/v1/trades*", [], input.resource.path)                        │
  │    }                                                                                │
  │                                                                                     │
  │  LAYER 5: DATA SECURITY                                                             │
  │  ════════════════════════                                                           │
  │  Encryption at Rest:                                                                │
  │    - Cassandra: Transparent Data Encryption (TDE) with KMS CMK                     │
  │    - Elasticsearch: Node-to-node + at-rest encryption (OpenSearch managed)         │
  │    - S3: SSE-KMS (AES-256)                                                          │
  │    - EBS volumes: Encrypted with KMS                                                │
  │    - RDS: Storage encrypted with KMS                                                │
  │                                                                                     │
  │  Encryption in Transit:                                                             │
  │    - TLS 1.3 minimum everywhere                                                     │
  │    - Certificate pinning for internal services                                      │
  │    - Kafka: SSL + SASL/SCRAM authentication                                         │
  │    - Cassandra: SSL client-to-node + node-to-node                                  │
  │                                                                                     │
  │  Secrets Management:                                                                │
  │    - AWS Secrets Manager: DB passwords, API keys (auto-rotate every 30 days)       │
  │    - External Secrets Operator: Sync secrets to Kubernetes Secrets                 │
  │    - IRSA: Pods get AWS permissions via IAM roles (no static access keys)          │
  │    - Vault (HashiCorp): Dynamic secrets for Cassandra/Kafka credentials            │
  │                                                                                     │
  │  LAYER 6: COMPLIANCE + AUDIT                                                        │
  │  ════════════════════════════                                                       │
  │  - AWS CloudTrail: All API calls logged (S3, immutable, encrypted)                 │
  │  - AWS Config: Compliance rules (CIS Benchmark, PCI-DSS checks)                   │
  │  - GuardDuty: Threat detection (anomalous API calls, data exfiltration)            │
  │  - Security Hub: Aggregated security findings                                      │
  │  - Macie: PII detection in S3                                                       │
  │                                                                                     │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  Authentication Flow (Detailed Sequence)

  ┌──────┐         ┌─────────┐      ┌──────────┐      ┌────────────┐     ┌──────────┐
  │Client│         │API GW   │      │Auth Svc  │      │OPA Sidecar │     │Target Svc│
  └──┬───┘         └────┬────┘      └────┬─────┘      └─────┬──────┘     └────┬─────┘
     │                  │               │                   │                  │
     │ POST /oauth2/token│               │                   │                  │
     │ (client_id, secret│               │                   │                  │
     │  username, pwd)  │               │                   │                  │
     │─────────────────►│               │                   │                  │
     │                  │ Verify client credentials         │                  │
     │                  │──────────────►│                   │                  │
     │                  │               │ Check DB           │                  │
     │                  │               │ (bcrypt verify)    │                  │
     │                  │               │ Check MFA if req'd │                  │
     │                  │               │◄──────────────────│                  │
     │                  │◄──────────────│                   │                  │
     │ 200 {access_token│               │                   │                  │
     │  refresh_token}  │               │                   │                  │
     │◄─────────────────│               │                   │                  │
     │                  │               │                   │                  │
     │ GET /api/v1/market│               │                   │                  │
     │ Authorization:   │               │                   │                  │
     │  Bearer <JWT>    │               │                   │                  │
     │─────────────────►│               │                   │                  │
     │                  │ Validate JWT  │                   │                  │
     │                  │ (local verify │                   │                  │
     │                  │  with cached  │                   │                  │
     │                  │  public key)  │                   │                  │
     │                  │               │                   │                  │
     │                  │ Check jti revocation (Redis)      │                  │
     │                  │──────────────►│                   │                  │
     │                  │◄──────────────│                   │                  │
     │                  │               │                   │                  │
     │                  │ POST /v1/data/allow               │                  │
     │                  │ {user, resource, action}          │                  │
     │                  │──────────────────────────────────►│                  │
     │                  │◄──────────────────────────────────│                  │
     │                  │  {result: true}                   │                  │
     │                  │                                   │                  │
     │                  │ Forward request (X-User-ID header)│                  │
     │                  │──────────────────────────────────────────────────────►
     │                  │                                   │                  │
     │                  │◄──────────────────────────────────────────────────────
     │◄─────────────────│               │                   │                  │
  │  200 Response       │               │                   │                  │

  ---
  13. OBSERVABILITY ARCHITECTURE

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                        OBSERVABILITY ARCHITECTURE (THREE PILLARS)                   │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  PILLAR 1: METRICS                                                                  │
  │  ══════════════════                                                                 │
  │                                                                                     │
  │  [Spring Actuator] ─► Micrometer ─► Prometheus JVM metrics                         │
  │  [JVM JMX]         ─► JMX Exporter ─►  Kafka metrics                               │
  │  [Cassandra]       ─► JMX Exporter ─►  Cassandra metrics                          │
  │  [Kubernetes]      ─► kube-state-metrics + node-exporter                           │
  │                                                                                     │
  │  Prometheus (per cluster) ─► Thanos (global query + long-term storage)             │
  │  Thanos ─► Grafana Dashboards                                                       │
  │                                                                                     │
  │  Key Dashboards:                                                                    │
  │    - Service RED metrics (Rate, Error, Duration) per service                       │
  │    - Kafka consumer lag per consumer group                                          │
  │    - Cassandra: read/write latency, compaction queue, GC pauses                    │
  │    - Elasticsearch: indexing rate, search latency, cluster health                  │
  │    - Infrastructure: CPU, memory, disk I/O, network per node                       │
  │    - Business metrics: events/sec, trade volume, API success rate                  │
  │                                                                                     │
  │  PILLAR 2: LOGGING                                                                  │
  │  ══════════════════                                                                 │
  │                                                                                     │
  │  All services ─► JSON structured logs ─► Fluent Bit (DaemonSet)                    │
  │                                          ─► Kafka topic: app.logs.v1               │
  │                                          ─► Logstash / OpenSearch Ingest            │
  │                                          ─► OpenSearch (ELK)                        │
  │                                          ─► S3 (cold storage, Athena queryable)    │
  │                                                                                     │
  │  Log Format (JSON):                                                                 │
  │  {                                                                                  │
  │    "timestamp": "2026-05-22T10:30:00.123Z",                                        │
  │    "level": "INFO",                                                                 │
  │    "service": "market-data-ingestion",                                             │
  │    "pod": "market-data-ingestion-7d4f9b-xyz",                                      │
  │    "traceId": "4bf92f3577b34da6a3ce929d0e0e4736",                                 │
  │    "spanId": "00f067aa0ba902b7",                                                   │
  │    "correlationId": "req-uuid-123",                                                │
  │    "userId": "u456",                                                               │
  │    "message": "Market data event processed",                                       │
  │    "symbol": "AAPL",                                                               │
  │    "processingTimeMs": 3,                                                          │
  │    "eventCount": 500                                                               │
  │  }                                                                                  │
  │                                                                                     │
  │  PILLAR 3: DISTRIBUTED TRACING                                                      │
  │  ═════════════════════════════                                                      │
  │                                                                                     │
  │  OpenTelemetry SDK (Java agent) ─► OTEL Collector ─► Jaeger / AWS X-Ray           │
  │                                                                                     │
  │  Spring Boot config:                                                                │
  │  management:                                                                        │
  │    tracing:                                                                         │
  │      sampling:                                                                      │
  │        probability: 0.1          # 10% sampling (100% for errors)                  │
  │  otel:                                                                              │
  │    exporter:                                                                        │
  │      otlp:                                                                          │
  │        endpoint: http://otel-collector:4317                                         │
  │                                                                                     │
  │  Trace Propagation:                                                                 │
  │    - W3C TraceContext headers (traceparent, tracestate)                             │
  │    - Propagated through Kafka message headers                                       │
  │    - Injected into all downstream calls (Feign, RestTemplate, WebClient)           │
  │                                                                                     │
  │  ALERTING STRATEGY                                                                  │
  │  ═════════════════                                                                  │
  │                                                                                     │
  │  Severity Levels:                                                                   │
  │  P1 (Critical, 5 min response):                                                     │
  │    - API error rate > 1% for 2 min                                                 │
  │    - Kafka consumer lag > 100K messages                                             │
  │    - Cassandra write failure rate > 0.1%                                           │
  │    - Any service with 0 healthy pods                                                │
  │    → PagerDuty on-call rotation                                                    │
  │                                                                                     │
  │  P2 (High, 30 min response):                                                        │
  │    - API P99 latency > 500ms                                                        │
  │    - CPU > 85% sustained 5 min                                                     │
  │    - Kafka lag > 10K messages                                                       │
  │    → Slack #platform-alerts + PagerDuty                                            │
  │                                                                                     │
  │  P3 (Medium, 2h response):                                                          │
  │    - Memory > 80%                                                                   │
  │    - Disk > 70%                                                                     │
  │    - DLQ receiving messages                                                         │
  │    → Slack #platform-alerts                                                        │
  │                                                                                     │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  ---
  14. CI/CD PIPELINE ARCHITECTURE

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                       COMPLETE CI/CD PIPELINE                                       │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  DEVELOPER WORKFLOW                                                                 │
  │  ══════════════════                                                                 │
  │  git push (feature branch)                                                          │
  │       │                                                                             │
  │       ▼                                                                             │
  │  ┌────────────────────────────────────────────────────────────────────────┐        │
  │  │  GitHub Actions / GitLab CI (Trigger: push, PR, merge)                │        │
  │  └────────────────┬───────────────────────────────────────────────────────┘        │
  │                   │                                                                 │
  │  STAGE 1: CODE QUALITY                                                              │
  │  ┌────────────────▼───────────────────────────────────────────────────────┐        │
  │  │  Parallel Jobs:                                                        │        │
  │  │  ├── Compile + Unit Tests (JUnit 5 + Mockito)                          │        │
  │  │  │     mvn test -pl market-data-ingestion                              │        │
  │  │  │     Target: 80%+ line coverage, 70%+ branch coverage                │        │
  │  │  │                                                                     │        │
  │  │  ├── Static Analysis                                                   │        │
  │  │  │     SonarQube: Code smells, duplicates, security hotspots           │        │
  │  │  │     SpotBugs: Bug pattern detection                                 │        │
  │  │  │     Checkstyle: Code style (Google Java Style Guide)                │        │
  │  │  │     OWASP Dependency Check: CVE scanning of dependencies            │        │
  │  │  │                                                                     │        │
  │  │  └── License Compliance (FOSSA scan)                                   │        │
  │  └────────────────┬───────────────────────────────────────────────────────┘        │
  │                   │                                                                 │
  │  STAGE 2: BUILD + PACKAGE                                                           │
  │  ┌────────────────▼───────────────────────────────────────────────────────┐        │
  │  │  mvn clean package -DskipTests                                         │        │
  │  │  Docker Build:                                                          │        │
  │  │    FROM amazoncorretto:21-alpine                                        │        │
  │  │    COPY target/app.jar /app.jar                                         │        │
  │  │    RUN adduser -D appuser && chown appuser /app.jar                     │        │
  │  │    USER appuser                                                         │        │
  │  │    ENTRYPOINT ["java", "-XX:+UseG1GC",                                 │        │
  │  │                 "-XX:MaxGCPauseMillis=200",                             │        │
  │  │                 "-XX:+UseContainerSupport",                             │        │
  │  │                 "-jar", "/app.jar"]                                     │        │
  │  │                                                                         │        │
  │  │  Container Scanning: Trivy (CRITICAL/HIGH CVE = fail build)            │        │
  │  │  Push to ECR: {account}.dkr.ecr.us-east-1.amazonaws.com/{svc}:{sha}   │        │
  │  └────────────────┬───────────────────────────────────────────────────────┘        │
  │                   │                                                                 │
  │  STAGE 3: INTEGRATION TESTING                                                       │
  │  ┌────────────────▼───────────────────────────────────────────────────────┐        │
  │  │  TestContainers-based tests (real dependencies):                       │        │
  │  │    - Kafka broker (real broker, not mock)                               │        │
  │  │    - Cassandra (real Cassandra node)                                    │        │
  │  │    - Elasticsearch (real node)                                          │        │
  │  │    - PostgreSQL (for auth service)                                      │        │
  │  │    - Redis (for rate limiting)                                          │        │
  │  │                                                                         │        │
  │  │  Contract Testing (Pact):                                               │        │
  │  │    - Consumer contracts published to Pact Broker                       │        │
  │  │    - Provider verification runs on provider CI                         │        │
  │  │    - Break build if contract violation detected                         │        │
  │  └────────────────┬───────────────────────────────────────────────────────┘        │
  │                   │                                                                 │
  │  STAGE 4: DEPLOY TO DEV                                                             │
  │  ┌────────────────▼───────────────────────────────────────────────────────┐        │
  │  │  ArgoCD (GitOps):                                                       │        │
  │  │    - Helm chart values updated with new image tag                       │        │
  │  │    - PR auto-merged to dev branch                                       │        │
  │  │    - ArgoCD syncs dev cluster within 3 minutes                          │        │
  │  │    - Smoke tests run (Postman/Newman collection)                        │        │
  │  └────────────────┬───────────────────────────────────────────────────────┘        │
  │                   │                                                                 │
  │  STAGE 5: PERFORMANCE TESTING (Staging)                                             │
  │  ┌────────────────▼───────────────────────────────────────────────────────┐        │
  │  │  Gatling load test:                                                     │        │
  │  │    - Ramp: 0 → 50K req/sec over 5 minutes                              │        │
  │  │    - Sustained: 50K req/sec for 15 minutes                             │        │
  │  │    - Pass criteria: P99 < 50ms, error rate < 0.01%                     │        │
  │  │                                                                         │        │
  │  │  Chaos Engineering (LitmusChaos):                                       │        │
  │  │    - Pod kill test                                                      │        │
  │  │    - Network partition test                                             │        │
  │  │    - Kafka broker failure test                                          │        │
  │  └────────────────┬───────────────────────────────────────────────────────┘        │
  │                   │                                                                 │
  │  STAGE 6: DEPLOY TO PRODUCTION (Manual Approval)                                   │
  │  ┌────────────────▼───────────────────────────────────────────────────────┐        │
  │  │  Canary → 5% traffic → monitor 10 min → 25% → 50% → 100%              │        │
  │  │  Auto-rollback: if error rate > 0.1% OR latency P99 > 100ms            │        │
  │  │  ArgoCD Rollout with Istio traffic shifting                            │        │
  │  │  Post-deploy verification: synthetic transactions for 5 minutes        │        │
  │  └────────────────────────────────────────────────────────────────────────┘        │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  ---
  15. SPRING BOOT COMPONENTS

  Core Modules and Configuration

  // 1. Spring WebFlux — Reactive non-blocking ingestion endpoint
  @RestController
  @RequestMapping("/api/v1/market")
  public class MarketDataController {

      private final MarketDataService service;
      private final MeterRegistry meterRegistry;

      @PostMapping("/quotes/bulk")
      @PreAuthorize("hasAuthority('SCOPE_market:write')")
      public Mono<ResponseEntity<BulkIngestResponse>> ingestBulk(
              @RequestBody @Valid Flux<MarketDataRequest> requests,
              @RequestHeader("X-Correlation-ID") String correlationId) {

          return requests
              .limitRate(1000)           // backpressure: process 1K at a time
              .flatMap(req -> service.validate(req)
                  .flatMap(service::transform)
                  .flatMap(service::publish), 50)  // 50 concurrent publishers
              .collectList()
              .map(results -> ResponseEntity.accepted()
                  .body(BulkIngestResponse.of(results)))
              .doOnEach(signal -> {
                  if (signal.hasError()) {
                      meterRegistry.counter("ingest.bulk.error").increment();
                  }
              });
      }
  }

  // 2. Spring Kafka — Consumer with exactly-once semantics
  @Service
  @Slf4j
  public class DataTransformConsumer {

      private final DataTransformService transformService;
      private final KafkaTemplate<String, TransformedData> kafkaTemplate;

      @KafkaListener(
          topics = "raw.market-data.v1",
          groupId = "data-transform-cg",
          containerFactory = "batchKafkaListenerContainerFactory"
      )
      @Transactional("kafkaTransactionManager")
      public void consume(
              List<ConsumerRecord<String, RawMarketData>> records,
              Acknowledgment ack) {

          MDC.put("batchSize", String.valueOf(records.size()));

          try {
              List<TransformedData> transformed = records.stream()
                  .map(r -> transformService.transform(r.value()))
                  .collect(Collectors.toList());

              // Transactional: Kafka offset commit + produce in same transaction
              transformed.forEach(data ->
                  kafkaTemplate.send("transformed.data.v1", data.getSymbol(), data));

              ack.acknowledge();

              meterRegistry.counter("transform.success",
                  "batch_size", String.valueOf(records.size())).increment();

          } catch (TransformException e) {
              log.error("Transform failed, sending to DLQ: {}", e.getMessage());
              records.forEach(r -> kafkaTemplate.send("dlq.market-data.v1", r.value()));
              ack.acknowledge();   // Still ack to move offset forward
          }
      }
  }

  // 3. Spring Data Cassandra — Reactive repository
  @Repository
  public interface QuoteRepository extends
          ReactiveCassandraRepository<QuoteEntity, QuoteKey> {

      @Query("SELECT * FROM quotes_by_symbol WHERE symbol=?0 AND trade_date=?1 " +
             "AND event_time > ?2 ORDER BY event_time DESC LIMIT ?3")
      Flux<QuoteEntity> findBySymbolAndDateRange(
          String symbol, LocalDate date, UUID fromTime, int limit);

      @Query("SELECT * FROM quotes_by_symbol WHERE symbol=?0 AND trade_date=?1 " +
             "ORDER BY event_time DESC LIMIT 1")
      Mono<QuoteEntity> findLatestBySymbol(String symbol, LocalDate date);
  }

  // 4. Resilience4j — Circuit breaker
  @Service
  public class DownstreamDeliveryService {

      @CircuitBreaker(name = "downstream-risk-engine", fallbackMethod = "fallback")
      @Retry(name = "downstream-risk-engine")
      @Bulkhead(name = "downstream-risk-engine")
      @TimeLimiter(name = "downstream-risk-engine")
      public CompletableFuture<DeliveryResponse> deliver(
              String consumerId, TransformedData data) {
          return CompletableFuture.supplyAsync(() ->
              riskEngineClient.push(data));
      }

      public CompletableFuture<DeliveryResponse> fallback(
              String consumerId, TransformedData data, Exception ex) {
          log.warn("CB open for {}, queuing to retry topic", consumerId);
          kafkaTemplate.send("retry.downstream." + consumerId, data);
          return CompletableFuture.completedFuture(
              DeliveryResponse.queued(consumerId));
      }
  }

  // Resilience4j config (application.yml):
  resilience4j:
    circuitbreaker:
      instances:
        downstream-risk-engine:
          slidingWindowSize: 20
          failureRateThreshold: 50
          waitDurationInOpenState: 30s
          permittedNumberOfCallsInHalfOpenState: 5
          automaticTransitionFromOpenToHalfOpenEnabled: true
    retry:
      instances:
        downstream-risk-engine:
          maxAttempts: 3
          waitDuration: 500ms
          enableExponentialBackoff: true
          exponentialBackoffMultiplier: 2
    bulkhead:
      instances:
        downstream-risk-engine:
          maxConcurrentCalls: 50
          maxWaitDuration: 100ms
    timelimiter:
      instances:
        downstream-risk-engine:
          timeoutDuration: 2s

  // 5. Spring Cloud Gateway — Route with filters
  @Configuration
  public class GatewayConfig {

      @Bean
      public RouteLocator customRouteLocator(RouteLocatorBuilder builder) {
          return builder.routes()
              .route("market-data-route", r -> r
                  .path("/api/v1/market/**")
                  .and().method(HttpMethod.POST, HttpMethod.GET)
                  .filters(f -> f
                      .addRequestHeader("X-Service-Version", "v1")
                      .requestRateLimiter(c -> c
                          .setRateLimiter(redisRateLimiter())
                          .setKeyResolver(userKeyResolver()))
                      .circuitBreaker(c -> c
                          .setName("market-data-cb")
                          .setFallbackUri("forward:/fallback/market"))
                      .retry(c -> c
                          .setRetries(2)
                          .setStatuses(HttpStatus.SERVICE_UNAVAILABLE)
                          .setBackoff(Duration.ofMillis(100),
                                     Duration.ofSeconds(2), 2, true))
                      .filter(new CorrelationIdFilter())
                      .filter(new JwtValidationFilter(jwtDecoder))
                  )
                  .uri("lb://market-data-ingestion-svc"))
              .build();
      }

      @Bean
      public RedisRateLimiter redisRateLimiter() {
          return new RedisRateLimiter(1000, 2000, 1);  // replenish, burst, requestedTokens
      }
  }

  ---
  16. MULTI-REGION DISASTER RECOVERY

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                     MULTI-REGION DR ARCHITECTURE                                    │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  TOPOLOGY: ACTIVE-ACTIVE FOR READS, ACTIVE-PASSIVE FOR WRITES                      │
  │  ══════════════════════════════════════════════════════════                         │
  │                                                                                     │
  │  Plano (Primary)              Hazelwood (Secondary)                                 │
  │  ═══════════════              ═══════════════════                                   │
  │  Handles: 100% writes         Handles: 0% writes (follows primary)                  │
  │           70% reads                    30% reads (US West users)                    │
  │                                                                                     │
  │  REPLICATION MECHANISMS                                                             │
  │  ════════════════════════                                                           │
  │                                                                                     │
  │  Cassandra:                                                                         │
  │    NetworkTopologyStrategy: plano=3, hazelwood=3                                   │
  │    Writes to LOCAL_QUORUM in plano → async replicated to hazelwood                 │
  │    Replication lag: typically <1 second for LAN-like connectivity                  │
  │    In DR scenario: promote hazelwood, temporarily accept LOCAL_ONE reads            │
  │                                                                                     │
  │  Kafka (MSK MirrorMaker2):                                                          │
  │    Source: MSK Plano → MirrorMaker2 → Target: MSK Hazelwood                       │
  │    Topics mirrored: all (via regex)                                                 │
  │    Offset translation: consumer offsets also mirrored                              │
  │    Lag: <1 second typically                                                         │
  │    Consumer groups can failover: point to hazelwood bootstrap + same offsets        │
  │                                                                                     │
  │  RDS Aurora Global DB:                                                              │
  │    Writer: Plano → Aurora Global → Reader: Hazelwood                               │
  │    Lag: <1 second (uses Aurora's custom protocol, not logical replication)         │
  │    Failover: Promote hazelwood reader to writer (<30 seconds)                      │
  │                                                                                     │
  │  FAILOVER PROCEDURE (RTO < 30 seconds)                                             │
  │  ══════════════════════════════════════                                             │
  │                                                                                     │
  │  Trigger: Route53 health check fails for Plano 3 consecutive times (30 sec each)  │
  │                                                                                     │
  │  Automated Steps:                                                                   │
  │  1. Route53: Switch DNS to Hazelwood endpoint (TTL = 30s)              [~30s]      │
  │  2. Lambda trigger: Promote Aurora Global to writer in Hazelwood       [~30s]      │
  │  3. Kubernetes: Scale up Hazelwood EKS from 8 → 20 nodes (Karpenter)  [~2min]     │
  │  4. Kafka: Consumers point to Hazelwood MSK (env var change via SSM)  [~1min]     │
  │  5. Cassandra: Application config switches LOCAL_DC to hazelwood       [~30s]      │
  │  6. Canary validation: 10% synthetic traffic to confirm readiness      [~2min]     │
  │                                                                                     │
  │  TOTAL AUTOMATED FAILOVER: ~5 minutes                                              │
  │  (RTO < 30s for read traffic; ~5 minutes for full write capability)                │
  │                                                                                     │
  │  RUNBOOK AUTOMATION (AWS Systems Manager Automation):                               │
  │    SSM Document: "exchange-dr-failover-to-hazelwood"                              │
  │    Steps automated via Lambda + SSM Run Command                                    │
  │    Tested monthly via GameDay exercises                                             │
  │                                                                                     │
  │  BACKUP STRATEGY                                                                    │
  │  ════════════════                                                                   │
  │  Cassandra: Medusa backups to S3 every 6 hours (incremental), daily full           │
  │  Elasticsearch: Snapshot to S3 every 1 hour, daily full snapshot                  │
  │  RDS Aurora: Automated backups 35-day retention + manual snapshot before deploys  │
  │  Kafka: Tiered storage to S3 (retain 30 days for replay)                          │
  │  S3: Versioning + Cross-region replication (CRR) enabled on all buckets          │
  │                                                                                     │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  ---
  17. PRODUCTION ISSUES AND SOLUTIONS

  Issue 1: Kafka Consumer Lag Spike

  SCENARIO: Consumer lag spikes from 1K to 2M messages in 10 minutes
  ════════════════════════════════════════════════════════════════════

  Root Cause Analysis:
    1. Slow downstream dependency (Cassandra latency spike)
    2. Consumer GC pause (long GC stopped consumer polling > max.poll.interval.ms)
    3. Consumer rebalance storm (rolling deploy caused repeated rebalances)
    4. Upstream surge (market open volatility 10x normal volume)

  Detection:
    - Grafana alert: kafka_consumer_lag_sum > 100K (triggers in 2 minutes)
    - JVM heap usage climbing (GC pressure precursor)
    - Cassandra write latency P99 increasing

  Diagnosis Commands:
    kafka-consumer-groups.sh --bootstrap-server <broker> \
      --describe --group data-transform-cg

    kafka-log-dirs.sh --describe --bootstrap-server <broker> \
      --topic-list transformed.data.v1

  Resolution:
    Short-term (immediate):
      1. Scale consumer group: kubectl scale deploy data-transform --replicas=20
      2. If Cassandra is slow: reduce Cassandra write batch size, increase async
      3. If GC: increase heap (-Xmx8g) via rolling restart
      4. If rebalance: set partition.assignment.strategy=CooperativeStickyAssignor
         (cooperative rebalancing — no stop-the-world rebalance)

    Long-term:
      1. Static partition assignment (avoid rebalances during deploy)
      2. Separate consumer threads from poll threads
      3. Implement consumer-side circuit breaker (slow consumer → pause + alert)
      4. Increase max.poll.records for throughput when lag is high

  Prevention:
    - Track consumer lag as HPA metric (scale before lag grows)
    - Use CooperativeStickyAssignor always
    - Tune max.poll.interval.ms > longest expected processing time
    - Alert on lag growth rate, not just absolute lag

  Issue 2: Cassandra Hot Partition

  SCENARIO: One

● symbol (e.g., TSLA during a trading frenzy) generates 10x normal write volume, causing one Cassandra node to hit 95% CPU.

  Detection:
    nodetool toppartitions market_data quotes_by_symbol 60
    Grafana: cassandra_table_coordinator_write_latency_p99 on one node >> others

  Resolution:
    Immediate:
      1. Add salt to partition key:
         salt = event.sequence_num % 8
         PRIMARY KEY ((symbol, trade_date, salt), event_time)
      2. Application reads all 8 salts, merges results in memory
      3. Throttle writes for hot symbol (backpressure at ingestion layer)

    Long-term:
      1. Pre-salt all schemas (migration via parallel write strategy)
      2. Monitor toppartitions in production as Prometheus metric
      3. Dynamic salt factor: 1 normally, 8 during high-volatility events

  Issue 3: Elasticsearch Split Brain

  SCENARIO: Network partition causes two nodes to each believe they are master.

  Root Cause:
    discovery.zen.minimum_master_nodes not set correctly (pre-7.x clusters)
    In ES 7+: cluster.initial_master_nodes misconfigured

  Detection:
    cluster health → RED
    cat/nodes shows two masters

  Resolution:
    1. Verify: GET /_cluster/state?filter_path=master_node
    2. Identify the stale master: lower document count = stale
    3. Restart stale master node with empty data dir
    4. ES auto-elects correct master via Raft-based coordination (ES 7+)

  Prevention:
    - Always 3 dedicated master nodes (quorum = 2)
    - cluster.initial_master_nodes explicitly set
    - Never co-locate master + data roles on same node
    - Monitor: elasticsearch_cluster_health_status != 1 → P1 alert

  Issue 4: Memory Leak / GC Pauses

  Root Cause Patterns:
    1. Off-heap Cassandra driver buffers not released
    2. Caffeine cache unbounded (no maximumSize set)
    3. ThreadLocal leak in Spring Security context
    4. Netty ByteBuf not released in WebFlux pipeline

  Detection:
    - JVM heap usage grows monotonically (never returns to baseline)
    - GC duration increasing (minor GC > 500ms, full GC > 5s)
    - Spring Actuator: /actuator/metrics/jvm.memory.used

  Diagnosis:
    kubectl exec <pod> -- jmap -dump:format=b,file=/tmp/heap.hprof <pid>
    kubectl cp <pod>:/tmp/heap.hprof ./heap.hprof
    Analyze with Eclipse MAT or VisualVM

  Resolution:
    1. Caffeine: always set maximumSize AND expireAfterAccess
    2. Spring WebFlux: ensure Mono/Flux chains release resources (doFinally)
    3. JVM tuning:
       -XX:+UseG1GC
       -XX:MaxGCPauseMillis=200
       -XX:G1HeapRegionSize=16m
       -XX:+HeapDumpOnOutOfMemoryError
       -XX:HeapDumpPath=/dumps/

  JVM Settings best practice for Kafka consumers:
    -Xms4g -Xmx4g           (equal min/max avoids resize pauses)
    -XX:+UseG1GC
    -XX:MaxGCPauseMillis=100
    -XX:+ExplicitGCInvokesConcurrent

  Issue 5: Duplicate Event Processing

  Scenario: Network timeout causes producer retry → duplicate message in Kafka

  Root Cause:
    Producer sends message, broker persists it, ACK lost in network
    Producer retries → broker receives duplicate

  Resolution Architecture:
    Layer 1 — Idempotent Producer:
      enable.idempotence=true
      Kafka assigns ProducerID + monotonic SequenceNumber
      Broker deduplicates within a single producer session

    Layer 2 — Application-level idempotency (Redis):
      key = sourceId + sequenceNum + symbol
      SET key "1" NX EX 3600  (set if not exists, expire in 1h)
      If key already exists → skip processing, log, increment duplicate counter

    Layer 3 — Cassandra upsert semantics:
      INSERT INTO ... IF NOT EXISTS  (LWT)
      OR: Use USING TIMESTAMP — higher timestamp wins (last-write-wins)

    Layer 4 — Kafka Streams exactly-once:
      processing.guarantee=exactly_once_v2
      Combines producer idempotence + consumer offset commit in same transaction

  ---
  18. ADVANCED PATTERNS

  CQRS + Event Sourcing

  COMMAND SIDE (Write):
    Client ──► Command API ──► Validate ──► Publish Event to Kafka
                                           (event is the source of truth)

    Kafka Topic: domain.events.v1
    Event Types: QuoteReceived, TradeExecuted, OrderPlaced, PriceUpdated

    Events are immutable, append-only
    Current state = replay of all events from beginning

  QUERY SIDE (Read):
    Kafka Consumer ──► Projection Builder ──► Cassandra (optimized read model)
                                          ──► Elasticsearch (search model)
                                          ──► Redis (hot cache model)

    Each query model is independently optimized for its access pattern
    No joins required at query time

  SAGA PATTERN — Distributed Transaction across services:
    Choreography-based Saga (preferred for simplicity):
      1. TradeService publishes: TradeInitiated
      2. RiskService consumes, validates, publishes: RiskApproved / RiskRejected
      3. SettlementService consumes RiskApproved, publishes: SettlementScheduled
      4. If any step fails: compensating events rolled back by each service

    Orchestration-based Saga (for complex flows):
      SagaOrchestrator controls flow:
      State machine: INITIATED → RISK_CHECKING → RISK_APPROVED → SETTLING → COMPLETED
      On failure: explicitly calls compensating actions

  OUTBOX PATTERN — Prevent dual-write problem:
    Problem: Writing to DB + publishing to Kafka is not atomic

    Solution:
      1. Write domain event to outbox table IN SAME DB TRANSACTION as entity write
         INSERT INTO outbox (id, event_type, payload, created_at) VALUES (...)

      2. Outbox Poller (Debezium CDC or scheduled job):
         SELECT * FROM outbox WHERE published=false ORDER BY created_at LIMIT 100
         For each: kafkaTemplate.send(...)
         UPDATE outbox SET published=true WHERE id=...

      3. Exactly-once delivery guaranteed: Kafka idempotent producer handles retries

  CAP Theorem Applied

  Our System's CAP Choices:

  Cassandra: AP (Availability + Partition Tolerance)
    - LOCAL_QUORUM: strong consistency within a DC
    - EACH_QUORUM: global strong consistency (but higher latency)
    - We choose: CP for audit writes, AP for market data writes
      (audit must be accurate; market data can be eventually consistent)

  Kafka: CP (Consistency + Partition Tolerance)
    - acks=all: no message lost during broker failure
    - Committed messages are always in the log, never lost

  Elasticsearch: AP
    - During network partition, search may return stale results
    - Acceptable for our use case (analytics can be eventually consistent)

  Redis (rate limiting): AP
    - Rate limits may be briefly inconsistent during Redis failover
    - Acceptable: a few extra requests get through during failover

  ---
  19. DATABASE SCHEMA DESIGN (COMPLETE)

  Elasticsearch Mapping (Additional Indices)

  // Trade search index
  PUT /trades-{YYYY-MM}/_mapping
  {
    "properties": {
      "trade_id": { "type": "keyword" },
      "symbol": { "type": "keyword" },
      "trade_price": { "type": "double" },
      "trade_volume": { "type": "long" },
      "buyer_id": { "type": "keyword" },
      "seller_id": { "type": "keyword" },
      "trade_type": { "type": "keyword" },
      "exchange_code": { "type": "keyword" },
      "event_time": { "type": "date" },
      "settlement_date": { "type": "date" },
      "status": { "type": "keyword" },
      "full_text": {
        "type": "text",
        "analyzer": "standard"
      }
    }
  }

  PostgreSQL Schema (Auth Service — RDS Aurora)

  -- Users table
  CREATE TABLE users (
      id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      username        VARCHAR(100) UNIQUE NOT NULL,
      email           VARCHAR(255) UNIQUE NOT NULL,
      password_hash   VARCHAR(255) NOT NULL,   -- bcrypt(cost=12)
      mfa_secret      VARCHAR(255),            -- TOTP secret (encrypted at app layer)
      status          VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
      created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_login      TIMESTAMPTZ,
      failed_attempts INT NOT NULL DEFAULT 0,
      locked_until    TIMESTAMPTZ
  );

  -- Roles and permissions
  CREATE TABLE roles (
      id          SERIAL PRIMARY KEY,
      name        VARCHAR(50) UNIQUE NOT NULL,  -- ROLE_TRADER, ROLE_ADMIN
      description TEXT
  );

  CREATE TABLE user_roles (
      user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
      role_id     INT REFERENCES roles(id),
      granted_at  TIMESTAMPTZ DEFAULT NOW(),
      granted_by  UUID REFERENCES users(id),
      PRIMARY KEY (user_id, role_id)
  );

  CREATE TABLE oauth2_registered_client (
      id                              VARCHAR(100) PRIMARY KEY,
      client_id                       VARCHAR(100) UNIQUE NOT NULL,
      client_secret                   VARCHAR(255),
      client_name                     VARCHAR(200),
      authorization_grant_types       TEXT NOT NULL,
      redirect_uris                   TEXT,
      scopes                          TEXT NOT NULL,
      token_settings                  TEXT,
      client_settings                 TEXT
  );

  -- Refresh token store (also in Redis for O(1) lookup)
  CREATE TABLE refresh_tokens (
      token_hash      VARCHAR(64) PRIMARY KEY,  -- SHA-256 of token
      user_id         UUID REFERENCES users(id),
      client_id       VARCHAR(100),
      issued_at       TIMESTAMPTZ NOT NULL,
      expires_at      TIMESTAMPTZ NOT NULL,
      revoked         BOOLEAN DEFAULT FALSE,
      revoked_at      TIMESTAMPTZ
  );
  CREATE INDEX idx_refresh_tokens_user ON refresh_tokens(user_id);
  CREATE INDEX idx_refresh_tokens_expires ON refresh_tokens(expires_at)
      WHERE NOT revoked;

  ---
  20. CAPACITY PLANNING AND SCALING STRATEGY

  CURRENT STATE ANALYSIS
  ════════════════════════

  Data Volume:    2 Billion records
    Cassandra:    2B records × avg 500 bytes = ~1 TB compressed (LZ4)
    Elasticsearch: 2B documents × avg 1KB = ~2TB (with replicas: 4TB)
    Kafka:        7-day retention × 10M events/day × avg 2KB = ~140GB

  Write Throughput:
    Peak: 1M events/sec
    Per partition (64 partitions): 15,625 events/sec
    Per Cassandra node (6 nodes): 166K writes/sec
    Cassandra write benchmark: i3en.3xlarge handles 200K writes/sec ✓

  Read Throughput:
    API reads: 100K req/sec
    Cassandra: LOCAL_ONE reads → 500K reads/sec per node (cache-assisted)
    Elasticsearch: coordinating nodes handle fan-out

  SCALING TRIGGERS AND ACTIONS
  ══════════════════════════════

  Kafka:
    Trigger: Producer throughput > 80% of broker capacity
    Action: Add brokers (MSK scales in-place), increase partition count
    Note: Partition increase requires consumer group restart for rebalance

  Cassandra:
    Trigger: Disk usage > 60% OR p99 write latency > 10ms
    Action: Add nodes (tokens redistribute via virtual nodes)
    Process: nodetool status → add node → wait for streaming → remove old token ranges
    Time: ~2 hours for full token rebalance on 1TB node

  Elasticsearch:
    Trigger: Indexing queue depth > 1000 OR search latency > 200ms
    Action: Add hot data nodes → reroute shards automatically
    Time: Shard rebalancing ~30min depending on shard size

  Application (EKS):
    Immediate: HPA triggers within 2 minutes of metric breach
    Node-level: Karpenter provisions new EC2 node in ~60 seconds
    Limit: EKS cluster autoscaler max=30 nodes per node group

  GROWTH PROJECTIONS (3-YEAR)
  ═════════════════════════════

  Year 1: 2B → 5B records (+150%)
    Cassandra: 1TB → 2.5TB → Add 4 nodes (10 total)
    Kafka: Add 2 brokers (5 total), increase partitions to 128

  Year 2: 5B → 12B records
    Cassandra: Archive cold data to S3 (via DataStax DSE Tiered Storage)
    Elasticsearch: Add warm nodes, enforce ILM delete at 2 years

  Year 3: Multi-tenant / multi-exchange
    Separate keyspaces per exchange (logical isolation)
    Separate Kafka clusters per tier (market data vs trade reporting)
    Consider Apache Flink for real-time stream processing at scale

  COST OPTIMIZATION
  ══════════════════

  Reserved Instances: Cassandra and Kafka nodes (1-3 year RI = 40-60% savings)
  Spot Instances: Non-critical processing workloads (data transform consumers)
  S3 Intelligent Tiering: Automatically moves infrequent data to cheaper storage
  Elasticsearch ILM: Delete indices > 1 year (cold data archived to S3 Parquet)
  Kafka Tiered Storage: Offload old log segments to S3 (broker disk = hot data only)

  ---
  21. TESTING STRATEGY

  // Unit Test — Data Transformation
  @ExtendWith(MockitoExtension.class)
  class DataTransformServiceTest {

      @Mock private ReferenceDataCache referenceDataCache;
      @Mock private MeterRegistry meterRegistry;
      @InjectMocks private DataTransformService transformService;

      @Test
      void shouldNormalizeCurrencyAndEnrichSymbol() {
          RawMarketData raw = RawMarketData.builder()
              .symbol("AAPL").price(new BigDecimal("150.25"))
              .currency("USD").sequenceNum(1001L).build();

          InstrumentMaster master = InstrumentMaster.builder()
              .symbol("AAPL").exchangeCode("NASDAQ")
              .isin("US0378331005").build();

          when(referenceDataCache.get("AAPL")).thenReturn(Optional.of(master));

          TransformedData result = transformService.transform(raw);

          assertThat(result.getSymbol()).isEqualTo("AAPL");
          assertThat(result.getExchangeCode()).isEqualTo("NASDAQ");
          assertThat(result.getIsin()).isEqualTo("US0378331005");
          assertThat(result.getNormalizedPrice()).isEqualByComparingTo("150.25");
      }
  }

  // Integration Test — Kafka + Cassandra (TestContainers)
  @SpringBootTest
  @Testcontainers
  @EmbeddedKafka(partitions = 4, topics = {"raw.market-data.v1", "transformed.data.v1"})
  class MarketDataPipelineIntegrationTest {

      @Container
      static CassandraContainer<?> cassandra = new CassandraContainer<>("cassandra:4.1")
          .withInitScript("schema.cql");

      @Container
      static ElasticsearchContainer elasticsearch =
          new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.11.0");

      @Autowired private KafkaTemplate<String, RawMarketData> kafkaTemplate;
      @Autowired private QuoteRepository quoteRepository;

      @Test
      void endToEndPipelineShouldPersistToAllStores() throws Exception {
          RawMarketData event = buildTestEvent("AAPL", new BigDecimal("175.50"));

          kafkaTemplate.send("raw.market-data.v1", "AAPL", event).get(5, SECONDS);

          await().atMost(30, SECONDS).untilAsserted(() -> {
              // Verify Cassandra persistence
              Optional<QuoteEntity> quote = quoteRepository
                  .findLatestBySymbol("AAPL", LocalDate.now()).blockOptional();
              assertThat(quote).isPresent();
              assertThat(quote.get().getLastPrice()).isEqualByComparingTo("175.50");
          });
      }
  }

  // Contract Test (Pact — Consumer side)
  @ExtendWith(PactConsumerTestExt.class)
  @PactTestFor(providerName = "market-data-api")
  class MarketDataApiContractTest {

      @Pact(consumer = "risk-engine")
      public RequestResponsePact createPact(PactDslWithProvider builder) {
          return builder
              .given("AAPL quotes exist for today")
              .uponReceiving("a request for AAPL latest quote")
                  .path("/api/v1/market/quotes/AAPL/latest")
                  .method("GET")
              .willRespondWith()
                  .status(200)
                  .body(new PactDslJsonBody()
                      .stringType("symbol", "AAPL")
                      .decimalType("lastPrice", 175.50)
                      .stringType("currency", "USD"))
              .toPact();
      }
  }

  // Performance Test (Gatling)
  class MarketDataLoadSimulation extends Simulation {
    val httpProtocol = http.baseUrl("https://api.exchange.internal")
      .header("Authorization", "Bearer #{token}")

    val ingestScenario = scenario("Market Data Ingest")
      .exec(http("Ingest Quote")
        .post("/api/v1/market/quotes/bulk")
        .body(StringBody("""[{"symbol":"AAPL","price":175.50,"volume":1000}]"""))
        .check(status.is(202)))

    setUp(
      ingestScenario.inject(
        rampUsersPerSec(100).to(10000).during(5.minutes),
        constantUsersPerSec(10000).during(15.minutes)
      )
    ).assertions(
      global.responseTime.percentile3.lt(50),   // P99 < 50ms
      global.failedRequests.percent.lt(0.01)    // Error rate < 0.01%
    )
  }

  ---
  22. TRADEOFFS ANALYSIS

  ┌───────────────┬───────────────────┬────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────┐
  │   Decision    │      Chosen       │  Alternative   │                                              Why                                               │
  ├───────────────┼───────────────────┼────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ DB            │ Cassandra         │ PostgreSQL     │ 2B+ records, linear write scale, no single point of failure. RDBMS can't sustain 1M writes/sec │
  │               │                   │                │  without sharding complexity                                                                   │
  ├───────────────┼───────────────────┼────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Streaming     │ Kafka             │ RabbitMQ       │ Kafka retains messages (replay), higher throughput, better partition model for ordering        │
  │               │                   │                │ guarantees. RabbitMQ deletes on consume                                                        │
  ├───────────────┼───────────────────┼────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Search        │ Elasticsearch     │ Solr           │ ES better ecosystem (Kibana, ILM), better horizontal scaling, richer aggregation API           │
  ├───────────────┼───────────────────┼────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Communication │ Async (Kafka)     │ Sync (REST)    │ Decouples services, handles upstream bursts, allows independent scaling. Sync = cascading      │
  │               │                   │                │ failures                                                                                       │
  ├───────────────┼───────────────────┼────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Protocol      │ REST              │ gRPC           │ REST: better upstream/downstream compatibility, no Protobuf dependency on partners. gRPC used  │
  │               │                   │                │ internally where performance critical                                                          │
  ├───────────────┼───────────────────┼────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Architecture  │ Microservices     │ Monolith       │ Independent deployment, technology heterogeneity, fault isolation. Tradeoff: distributed       │
  │               │                   │                │ systems complexity                                                                             │
  ├───────────────┼───────────────────┼────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ Consistency   │ Eventual          │ Strong         │ Financial data: audit writes are strong (LWT), market data is eventual (performance)           │
  ├───────────────┼───────────────────┼────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────┤
  │ DR Mode       │ Active-Active     │ Active-Passive │ Better utilization of Hazelwood region, lower RTO for read traffic                             │
  │               │ reads             │                │                                                                                                │
  └───────────────┴───────────────────┴────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────┘

  ---
  23. INTERVIEW QUESTIONS — STAFF/PRINCIPAL LEVEL

  SYSTEM DESIGN:
    Q: How do you guarantee exactly-once processing in Kafka?
    A: Three layers: 1) Idempotent producer (enable.idempotence=true + PID+SeqNum),
       2) Application-level idempotency key in Redis, 3) Kafka Streams
       processing.guarantee=exactly_once_v2 (transactional offset commit + produce)

    Q: How do you handle schema evolution in Kafka?
    A: Confluent Schema Registry with Avro/Protobuf. Compatibility modes:
       BACKWARD (new schema can read old data), FORWARD, FULL.
       Evolution rules: add optional fields only, never remove required fields.
       Consumer deserializes with schema-at-write, not schema-at-read.

    Q: Cassandra vs RDBMS for this use case?
    A: Cassandra: write-optimized, linear scale, no SPOF, multi-region native.
       RDBMS: ACID transactions (not needed for time-series), complex queries
       (we use ES for that), vertical scaling. At 2B records + 1M writes/sec,
       RDBMS needs complex sharding. Cassandra gives this natively.

    Q: How would you debug a P99 latency spike at 3 AM?
    A: 1) Check Grafana: which service? which endpoint?
       2) Distributed trace in Jaeger: find slow span
       3) Check Cassandra: nodetool tpstats, proxyhistograms
       4) Check GC logs: GC pause coinciding with latency spike?
       5) Check Kafka lag: slow consumer causing backpressure?
       6) Check resource saturation: CPU steal, disk I/O wait
       7) Check recent deployments: git log --since="3h ago"

    Q: How do you handle 10x traffic spike during market open?
    A: 1) Pre-scale: scheduled HPA scale-up at 9:25 AM (market opens 9:30)
       2) Kafka absorbs burst: producers non-blocking, consumers process at capacity
       3) Circuit breakers prevent cascade: downstream slowness isolated
       4) Rate limiting: token bucket at API Gateway (burst capacity = 2x normal)
       5) Auto-scaling: HPA adds pods within 2 minutes of CPU threshold

  CASSANDRA:
    Q: Why TWCS for time-series?
    A: TimeWindowCompactionStrategy groups SSTables within same time window.
       Old windows become immutable (no cross-window compaction).
       Read amplification minimized (only query current + 1-2 windows).
       Perfect for time-series where you only query recent data.
       LEVELED: good for equal read/write, SIZED: good for write-heavy batch.

  KAFKA:
    Q: How do you prevent a slow consumer from blocking others?
    A: Consumer groups are independent — one slow consumer only affects its group.
       Within a group: slow partition consumer blocks that partition only.
       Solution: separate consumer groups per downstream use case.
       Implement consumer-side circuit breaker: if downstream slow, pause consumption.

  KUBERNETES:
    Q: How do you deploy with zero downtime?
    A: 1) maxUnavailable=0 in RollingUpdate strategy
       2) readinessProbe must pass before traffic routed to new pod
       3) preStop hook sleeps 5s to drain in-flight requests from load balancer
       4) terminationGracePeriodSeconds=60 (enough for long-running requests)
       5) PodDisruptionBudget ensures minimum healthy pods during node drain

  ---
  24. FINAL OPTIMIZED ARCHITECTURE SUMMARY

  ┌─────────────────────────────────────────────────────────────────────────────────────┐
  │                    FINAL PRODUCTION-READY ARCHITECTURE                              │
  ├─────────────────────────────────────────────────────────────────────────────────────┤
  │                                                                                     │
  │  REQUEST PATH (read):     <10ms P99                                                 │
  │  Route53 → CloudFront → WAF → ALB → API Gateway → Eureka → Query Service           │
  │  → Redis L1 cache (hit: 1ms) → Cassandra LOCAL_ONE (miss: 5ms)                     │
  │  → Elasticsearch (search: 8ms)                                                      │
  │                                                                                     │
  │  WRITE PATH (ingest):     <50ms end-to-end                                          │
  │  Upstream → ALB → API Gateway → Ingestion Service → Kafka (2ms)                    │
  │  → Transform Consumer (10ms) → Cassandra (5ms) + ES (15ms) + DW (async)            │
  │  → Downstream push (async, decoupled)                                               │
  │                                                                                     │
  │  RELIABILITY:                                                                       │
  │  - Every service: min 3 replicas, spread across 3 AZs                              │
  │  - Every external call: Resilience4j CB + retry + bulkhead                         │
  │  - Every write: idempotent (Redis dedup + Kafka idempotent producer)                │
  │  - Every failure: DLQ + alerting + automated retry                                  │
  │  - Every night: DR drill automated (synthetic failover + recovery)                  │
  │                                                                                     │
  │  KEY NUMBERS:                                                                       │
  │  Throughput:    1M events/sec sustained, 5M peak                                   │
  │  Latency P99:   <10ms API reads, <50ms pipeline                                    │
  │  Availability:  99.999% (automated failover in <5 min)                             │
  │  Data Safety:   RPO=0 (Kafka acks=all + Cassandra LOCAL_QUORUM)                    │
  │  Compliance:    7-year immutable audit log (Cassandra + S3)                        │
  │                                                                                     │
  │  TEAM OWNERSHIP MODEL:                                                              │
  │  Platform Team:   API Gateway, Auth, K8s cluster, CI/CD, observability             │
  │  Data Team:       Cassandra, Elasticsearch, DW sync, schemas                       │
  │  Ingestion Team:  Market data, trade, reference, order mgmt services                │
  │  Delivery Team:   Downstream connectors, notification, reporting                   │
  │  SRE Team:        On-call, runbooks, capacity, chaos engineering                    │
  │                                                                                     │
  └─────────────────────────────────────────────────────────────────────────────────────┘

  This architecture delivers a production-ready, enterprise-grade Stock Exchange platform. Every design decision maps to a concrete business requirement:
  Cassandra for write throughput, Kafka for decoupling and replay, Elasticsearch for search, multi-region for compliance and DR, CQRS for independent
  scaling of reads and writes.
