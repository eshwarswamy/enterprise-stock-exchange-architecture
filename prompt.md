Act as a Principal Solution Architect, Staff Engineer, and Distributed Systems Expert with deep expertise in Java, Spring Boot Microservices, Apache Kafka, Cassandra, Elasticsearch, Kubernetes, AWS Cloud, and High-Scale Financial/Stock Exchange Systems.

I want you to design a complete Enterprise-Level System Design and Architecture for a Stock Exchange platform handling extremely large-scale distributed data processing.

========================
BUSINESS CONTEXT
========================

We are a Stock Exchange platform processing massive amounts of market/trading data.

Current System Details:

- Around 2 Billion+ records stored in the platform
- 4 Upstream Applications sending market/trading/reference data
- 4 Downstream Applications consuming processed data
- Data must be transformed into Data Warehouse compatible format
- Cassandra is used as the primary distributed datastore
- Elasticsearch is used for searching and analytics
- Kafka is used for event streaming and asynchronous processing
- Java + Spring Boot Microservices architecture
- Application deployed on 20 servers/nodes
- Multi-region deployment:
   1. Plano Region
   2. Hazelwood Region
- APIs are exposed to downstream consumers/end clients
- System is highly available, low latency, fault tolerant, scalable, and distributed

========================
WHAT I NEED
========================

Design the COMPLETE SYSTEM ARCHITECTURE from scratch and explain EVERYTHING step-by-step in extremely detailed manner without missing any component.

I want:

1. High-Level Architecture Diagram
2. Low-Level Architecture Diagram
3. End-to-End Data Flow Diagram
4. Microservices Architecture Diagram
5. Kafka Event Flow Diagram
6. Cassandra Cluster Architecture
7. Elasticsearch Cluster Architecture
8. Kubernetes Deployment Architecture
9. AWS Cloud Architecture
10. CI/CD Architecture Diagram
11. Security Architecture Diagram
12. Multi-region Disaster Recovery Architecture
13. API Communication Flow
14. Authentication & Authorization Flow
15. Client Request Lifecycle
16. Logging/Monitoring/Tracing Architecture
17. Sequence Diagrams for request processing
18. Deployment Pipeline Flow
19. Database Schema Design
20. Capacity Planning and Scaling Strategy

Use:
- ASCII diagrams
- Mermaid diagrams
- Component-level architecture diagrams
- Sequence diagrams
- Deployment topology diagrams

========================
SYSTEM FLOW TO EXPLAIN
========================

Explain step-by-step starting from:

1. Client Request
2. DNS Resolution
3. Load Balancer
4. API Gateway
5. Authentication
6. Authorization
7. Rate Limiting
8. Request Routing
9. Spring Boot Microservice processing
10. Kafka publishing
11. Kafka consumer processing
12. Data transformation
13. Cassandra writes
14. Elasticsearch indexing
15. Data warehouse loading
16. Downstream API consumption
17. Response generation
18. Logging and monitoring
19. Audit tracking
20. Failover handling

========================
MICROSERVICES DETAILS
========================

Design and explain microservices such as:

- API Gateway Service
- Authentication Service
- Authorization Service
- User Service
- Market Data Ingestion Service
- Trade Processing Service
- Data Transformation Service
- Cassandra Persistence Service
- Elasticsearch Indexing Service
- Data Warehouse Sync Service
- Downstream Delivery Service
- Notification Service
- Audit Service
- Reporting Service
- Monitoring Service
- Configuration Service
- Discovery Service

For each microservice explain:

- Responsibility
- APIs exposed
- Database used
- Kafka topics used
- Deployment strategy
- Scaling strategy
- Failure handling
- Circuit breaker implementation
- Retry strategy
- Idempotency handling

========================
SPRING BOOT DETAILS
========================

Explain all Spring Boot modules/components used:

- Spring Boot
- Spring MVC
- Spring WebFlux
- Spring Security
- Spring Data Cassandra
- Spring Data Elasticsearch
- Spring Cloud Gateway
- Spring Cloud Config
- Spring Cloud Eureka
- Spring Kafka
- Spring Retry
- Spring Actuator
- Spring Sleuth / OpenTelemetry
- Resilience4j
- Spring Batch
- Spring Scheduler
- Feign Clients

Explain:
- Why each module is needed
- Internal working
- Real-time usage
- Best practices
- Configuration examples

========================
KAFKA ARCHITECTURE
========================

Explain Kafka in detail:

- Topics
- Partitions
- Replication factor
- Consumer groups
- Offset management
- Dead Letter Queue
- Retry Topics
- Event ordering
- Exactly-once semantics
- Idempotent producer
- High throughput optimization
- Backpressure handling
- Rebalancing issues

Explain:
- Producer flow
- Consumer flow
- Failure scenarios
- Lag monitoring
- Real-time production issues

========================
CASSANDRA ARCHITECTURE
========================

Explain Cassandra in detail:

- Partition key selection
- Clustering columns
- Data modeling
- Denormalization
- Replication strategy
- Consistency levels
- Read repair
- Compaction
- Tombstones
- Hot partitions
- Multi-region replication
- Write path
- Read path

Provide:
- Sample table schemas
- Query optimization strategies
- Real-time production issues
- Capacity planning

========================
ELASTICSEARCH ARCHITECTURE
========================

Explain Elasticsearch in detail:

- Index design
- Sharding
- Replication
- Query optimization
- Aggregations
- Search latency optimization
- Index lifecycle management
- Cluster sizing
- Hot/Warm architecture

Explain:
- Search flow
- Indexing flow
- Reindexing challenges
- Mapping strategies
- Real-time issues

========================
AWS CLOUD ARCHITECTURE
========================

Include AWS services such as:

- Route53
- CloudFront
- WAF
- API Gateway
- ALB/NLB
- EKS
- EC2
- MSK (Kafka)
- Cassandra deployment strategy
- Elasticsearch/OpenSearch
- S3
- IAM
- Secrets Manager
- RDS (if needed)
- CloudWatch
- X-Ray
- SNS/SQS
- Lambda
- Auto Scaling Groups

Explain:
- Why each AWS service is used
- HA and DR strategy
- Multi-region architecture
- Cost optimization
- Security best practices

========================
KUBERNETES & DOCKER
========================

Explain:

- Docker containerization
- Docker image lifecycle
- Kubernetes architecture
- Pods
- ReplicaSets
- Deployments
- StatefulSets
- Services
- Ingress
- ConfigMaps
- Secrets
- Horizontal Pod Autoscaling
- Node affinity
- Rolling deployments
- Blue-Green deployments
- Canary deployments

Provide:
- Sample deployment YAMLs
- Scaling strategies
- Failure handling
- Pod restart scenarios

========================
CI/CD PIPELINE
========================

Design complete CI/CD pipeline using:

- GitHub/GitLab
- Jenkins
- SonarQube
- Nexus/Artifactory
- Docker
- Kubernetes
- Helm
- Terraform
- Ansible

Explain:
- Build pipeline
- Test pipeline
- Security scanning
- Deployment stages
- Rollback strategies
- Environment promotion
- Zero downtime deployment

========================
SECURITY ARCHITECTURE
========================

Explain:

- OAuth2
- JWT
- OpenID Connect
- mTLS
- API Security
- Encryption at rest
- Encryption in transit
- RBAC
- Secrets management
- WAF
- DDoS protection
- Certificate management

Explain authentication and authorization flow in detail.

========================
OBSERVABILITY
========================

Explain:

- Centralized logging
- ELK Stack
- Splunk
- Prometheus
- Grafana
- OpenTelemetry
- Distributed tracing
- Correlation IDs
- Alerting strategies

Explain how production debugging happens.

========================
TESTING STRATEGY
========================

Explain:

- Unit testing
- Integration testing
- Contract testing
- Performance testing
- Chaos testing
- Load testing
- Kafka testing
- Cassandra testing

Include:
- JUnit
- Mockito
- TestContainers
- Gatling/JMeter

========================
REAL-TIME PRODUCTION ISSUES
========================

Explain REAL production issues such as:

- Kafka lag
- Cassandra hot partitions
- Elasticsearch split brain
- Memory leaks
- GC pauses
- Network latency
- Pod crashes
- Data inconsistency
- Duplicate event processing
- API throttling
- Cross-region failures
- Schema evolution problems
- Deployment failures
- Backpressure
- Slow consumers

For each issue explain:
- Root cause
- Detection
- Monitoring
- Resolution
- Prevention strategy

========================
TRADEOFFS
========================

Explain tradeoffs for:

- Cassandra vs RDBMS
- Kafka vs RabbitMQ
- Elasticsearch vs Solr
- Synchronous vs asynchronous communication
- REST vs gRPC
- Monolith vs microservices
- Multi-region active-active vs active-passive
- Event-driven architecture tradeoffs

========================
ADVANCED TOPICS
========================

Explain:

- CAP theorem
- Event sourcing
- CQRS
- Saga pattern
- Outbox pattern
- Distributed transactions
- Idempotency
- Eventual consistency
- Backpressure handling
- Data replay strategy

========================
OUTPUT FORMAT
========================

Provide the answer in the following structure:

1. Executive Summary
2. Requirements Analysis
3. High-Level Architecture
4. Detailed Component Design
5. End-to-End Flow
6. Database Design
7. Kafka Design
8. Cloud Architecture
9. Kubernetes Architecture
10. Security Design
11. Observability Design
12. CI/CD Design
13. Production Challenges
14. Tradeoffs
15. Scaling Strategy
16. Disaster Recovery Strategy
17. Best Practices
18. Interview Questions
19. Real-world Enterprise Recommendations
20. Final Optimized Architecture

Make the explanation extremely detailed, enterprise-grade, production-ready, and suitable for:
- Staff Engineer interviews
- Principal Architect discussions
- Real-world financial systems
- Stock exchange platforms
- High-frequency distributed systems
- Large-scale data platforms

Do NOT skip any internal details.
