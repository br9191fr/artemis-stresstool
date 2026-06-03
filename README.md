# Artemis XML Stress Tool

A **multi-threaded** stress and performance testing tool for **Apache ActiveMQ Artemis** brokers. Sends XML documents over JMS with **mutual TLS / X.509 certificate authentication**.

## Features

| Feature | Detail |
|---|---|
| **Protocol** | JMS over Artemis Core (TCP/SSL) |
| **Auth** | X.509 mutual TLS (client certificate) |
| **Concurrency** | Configurable thread pool (1 – N producers) |
| **Payload** | Generated XML *or* user-defined XML template |
| **Rate limiting** | Per-thread token-bucket limiter |
| **Transactional** | Optional batch commits |
| **Metrics** | Live throughput + latency histogram (p50/p90/p99/p99.9) |
| **Report** | Console table + optional CSV file |
| **Config** | CLI flags or JSON config file |

---

## Quick Start

### 1. Build

```bash
mvn clean package -q
# → target/artemis-stress-tool-1.0.0.jar
```

### 2. Generate test certificates

```bash
chmod +x scripts/generate-certs.sh
./scripts/generate-certs.sh
# Creates certs/ with client-keystore.jks, truststore.jks, broker-keystore.jks
```

### 3. Configure Artemis broker

Add an SSL acceptor to `broker.xml`:

```xml
<acceptor name="ssl">
  tcp://0.0.0.0:61617?sslEnabled=true;
  keyStorePath=/path/to/certs/broker-keystore.jks;
  keyStorePassword=changeit;
  trustStorePath=/path/to/certs/truststore.jks;
  trustStorePassword=changeit;
  needClientAuth=true;
  enabledProtocols=TLSv1.3
</acceptor>
```

Add a security setting that grants access to the client CN:

```xml
<security-setting match="STRESS.#">
  <permission type="send"    roles="stress-client"/>
  <permission type="consume" roles="stress-client"/>
</security-setting>
```

### 4. Run the stress tool

**CLI flags:**
```bash
java -jar target/artemis-stress-tool-1.0.0.jar \
  --broker-url ssl://localhost:61617 \
  --queue STRESS.TEST \
  --threads 10 \
  --messages 100000 \
  --message-size 4096 \
  --keystore certs/client-keystore.jks \
  --keystore-password changeit \
  --truststore certs/truststore.jks \
  --truststore-password changeit \
  --report-interval 5 \
  --output results.csv
```

**JSON config file:**
```bash
java -jar target/artemis-stress-tool-1.0.0.jar --config config-example.json
```

---

## Configuration Reference

### CLI Options

| Option | Default | Description |
|---|---|---|
| `--broker-url` | `ssl://localhost:61617` | Artemis broker URL |
| `--queue` | `STRESS.TEST` | Destination queue name |
| `--topic` | false | Use topic instead of queue |
| `--threads` | 4 | Number of producer threads |
| `--messages` | 10000 | Total messages (0 = unlimited) |
| `--duration` | 0 | Max test duration in seconds (0 = no limit) |
| `--rate` | 0 | Target messages/sec per thread (0 = max) |
| `--message-size` | 1024 | Approx. XML payload size in bytes |
| `--xml-template` | — | Path to XML template file |
| `--persistent` | true | Persistent JMS delivery |
| `--batch-size` | 0 | Transacted batch size (0 = AUTO_ACK) |
| `--connection-pool` | 1 | JMS connections shared across threads |
| `--warmup` | 100 | Messages excluded from metrics |
| `--report-interval` | 5 | Live metrics print interval (seconds) |
| `--output` | — | Write final report to CSV file |
| `--keystore` | — | Client keystore path (JKS/PKCS12) |
| `--keystore-password` | — | Keystore password |
| `--key-password` | — | Private key password (defaults to keystore-password) |
| `--truststore` | — | Truststore path |
| `--truststore-password` | — | Truststore password |
| `--keystore-type` | JKS | JKS or PKCS12 |
| `--tls-version` | TLSv1.3 | TLS protocol version |
| `--config` | — | JSON config file (overrides all flags) |

### XML Template Placeholders

When using `--xml-template`, the following placeholders are substituted per message:

| Placeholder | Value |
|---|---|
| `${SEQ}` | Global sequence number (monotonically increasing) |
| `${UUID}` | Random UUID |
| `${TS}` | ISO-8601 timestamp |
| `${PRODUCER_ID}` | Producer thread ID |
| `${RANDOM}` | Random long 0–999999 |

See `template-example.xml` for a full example.

---

## Live Output

```
Time     │       Sent │ Rate msg/s │  Mean ms │   p50 ms │   p90 ms │   p99 ms │ Bytes MB │ Errors
────────────────────────────────────────────────────────────────────────────────────────────────────
10:23:15 │      15248 │    3050.2  │     1.23 │     0.98 │     2.45 │     8.12 │     62.4 │ 0
10:23:20 │      30501 │    3050.6  │     1.21 │     0.97 │     2.41 │     7.98 │    124.8 │ 0
```

---

## Final Report

```
════════════════════════════════════════════════════════════════════════════════════════════════════
  FINAL REPORT
════════════════════════════════════════════════════════════════════════════════════════════════════
  Messages sent                  : 100000
  Errors                         : 0
  Elapsed time                   : 32.81 s
  Overall throughput             : 3047.8 msg/s
  Total data sent                : 409.60 MB
  Data throughput                : 12.48 MB/s

  Latency distribution:
  Min                            : 0.312 ms
  Mean                           : 1.215 ms
  p50 (median)                   : 0.981 ms
  p75                            : 1.344 ms
  p90                            : 2.412 ms
  p95                            : 3.891 ms
  p99                            : 8.122 ms
  p99.9                          : 21.043 ms
  Max                            : 47.231 ms

  ✔ Test completed successfully
════════════════════════════════════════════════════════════════════════════════════════════════════
```

---

## Architecture

```
StressToolMain (CLI)
       │
       ├── StressConfig (validated config model)
       ├── SslContextFactory (X.509 / mTLS setup)
       ├── ConnectionPool (N shared JMS connections, round-robin)
       │
       ├── ProducerOrchestrator
       │      │
       │      ├── XmlProducer (Thread-0)  ─┐
       │      ├── XmlProducer (Thread-1)   ├── shared AtomicLong counter
       │      ├── ...                      ├── shared AtomicBoolean stopFlag
       │      └── XmlProducer (Thread-N)  ─┘
       │
       ├── XmlPayloadGenerator (template or generated)
       ├── MetricsCollector (lock-free, HDR histogram)
       └── ReportPrinter (live + final, CSV export)
```

---

## Project Structure

```
artemis-stress-tool/
├── pom.xml
├── config-example.json
├── template-example.xml
├── scripts/
│   └── generate-certs.sh
└── src/main/java/com/artemis/stress/
    ├── StressToolMain.java          CLI entry point
    ├── config/
    │   └── StressConfig.java        Configuration model
    ├── ssl/
    │   └── SslContextFactory.java   X.509 / mTLS factory
    ├── producer/
    │   ├── ConnectionPool.java      JMS connection pool
    │   ├── ProducerOrchestrator.java Thread lifecycle manager
    │   └── XmlProducer.java         Per-thread producer
    ├── xml/
    │   └── XmlPayloadGenerator.java Template + generated XML
    └── metrics/
        ├── MetricsCollector.java    Lock-free counters + histogram
        └── ReportPrinter.java       Live + final reporting
```

---

## Requirements

- Java 17+
- Maven 3.8+
- Apache Artemis 2.x broker
- `openssl` + `keytool` (for certificate generation)
