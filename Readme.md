# Distributed Election Service

A fault-tolerant distributed system implementing leader election and document processing using Kafka.

## Key Features

- Leader election using Bully algorithm
- Dynamic role assignment (Coordinator/Proposer/Acceptor/Learner)
- Distributed word counting by letter ranges
- Automatic failure detection and recovery
- Visual logging with formatted output

## Technology Stack

- Java 17
- Spring Boot
- Apache Kafka
- SLF4J for logging

## Getting Started

### Prerequisites
- Running Kafka instance
- Java 17+ JDK
- Maven

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/distributed-election-service.git