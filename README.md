# Quake Pipeline
 
I grew up in Vancouver, so the fear of the **BIG ONE** is real. Quaker is a real-time earthquake "tool" designed to pull USGS seismic data into a visual alert. Quaker uses Java, Kafka (KRaft), Schema Registry with Avro, Docker Compose, and (soon) Kafka Streams, PostGIS, and Grafana. Kafka is the foundation of this project so enrichment, alerting, and analytics are all just consumers of the same stream. Quakes get revised after the fact (a 4.8 becomes a 5.1), so the pipeline is built to handle updates without double counting or double alerting.
 
## What Does it Do?? (Currently)
 
- Run the poller and it checks the USGS live GeoJSON feed every 60 seconds
- Every new or updated quake is converted to an Avro `Quake` record and published to the `quake.raw` Kafka topic, keyed by event ID
- Open Kafka UI and watch quakes show up in real time
- If a quake gets revised (magnitude, status, etc.), it shows up as a second message with a higher `updated` timestamp
 
Requires Docker, JDK 21, and Maven 3.9+.
 
```bash
docker compose up -d                  # Kafka, Schema Registry, topics, Kafka UI
mvn -q package                        # builds common + poller
java -jar poller/target/poller-*.jar  # or just run FeedPoller from your IDE
```
 
Then open http://localhost:8080 and look at `quake.raw` (set the value serde to Schema Registry / Avro).
 
Want data right away? Point the poller at the bigger feed:
 
```bash
FEED_URL=https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson \
  java -jar poller/target/poller-*.jar
```
 
### Configuration
 
| Variable | Default | What it does |
|---|---|---|
| `FEED_URL` | USGS `all_hour.geojson` | Which feed to poll |
| `BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `SCHEMA_REGISTRY` | `http://localhost:8081` | Schema Registry URL |
 
If you run the poller inside Compose, use `BOOTSTRAP=kafka:19092` and `SCHEMA_REGISTRY=http://schema-registry:8081`. Kafka has two listeners: `kafka:19092` for containers and `localhost:9092` for apps running from your IDE.
 
## How alerts will work
 
- Alerts fire on magnitude bands (4.5, 5.5, 6.5, 7.0), once per band per event
- A 4.8 revised to 5.1 crosses a band and alerts once; a 5.1 revised to 5.0 doesn't alert again
- The tsunami flag alerts once per event
- Re-polling the same feed never creates duplicates, because the revision tagger in Streams is the real deduper (the poller's in-memory `seen` map is just an optimization)
## Tech stack & key dependencies
 
| Package | Purpose |
|---|---|
| Java 21 | Language for the poller, Streams app, and alerter |
| Maven | Multi-module build (`common`, `poller`, ...) |
| Apache Kafka (KRaft) | Event backbone, no ZooKeeper |
| Confluent Schema Registry | Stores and enforces the Avro schemas |
| Avro | `Quake` record schema, generated into Java classes |
| kafka-clients | Producer used by the poller |
| Jackson | Parses the USGS GeoJSON |
| Kafka UI | Browse topics, messages, and schemas locally |
| Kafka Streams *(planned)* | Enrichment, alerting, windowed analytics |
| PostGIS *(planned)* | Spatial queries like "all M4+ within 200 km of Vancouver" |
| Grafana *(planned)* | Live map and charts |
| Docker Compose | Runs all the infrastructure locally |
 
## Topics
 
| Topic | Key | Notes |
|---|---|---|
| `quake.raw` | event ID | Compacted, Avro |
| `quake.enriched` | event ID | Adds region, geohash, depth class |
| `quake.alerts` | event ID + rule | 7-day retention |
| `quake.region-stats` | region | Windowed aggregates |
| `quake.mainshocks` | event ID | Compacted, M5.5+ for aftershock linking |
| `quake.sequences` | event ID | Aftershock sequence links |
| `quake.cell-baseline` | geohash | Compacted, historical avg events/day per cell |
 
All topics are created up front with explicit partition counts so the Streams joins stay co-partitioned.
 
## Project Structure
 
```
quake-pipeline/
├── pom.xml                          # Parent pom, lists modules
├── docker-compose.yml               # Kafka, Schema Registry, topic-init, Kafka UI
├── common/
│   └── src/main/avro/quake.avsc     # Quake schema (enrichment fields included, with defaults)
├── poller/
│   └── src/main/java/quake/poller/
│       └── FeedPoller.java          # Polls USGS every 60s, publishes to quake.raw
├── streams/                         # (planned) enrichment, alerts, swarm detection
├── alerter/                         # (planned) reads quake.alerts, posts to Slack/webhook
├── connect/                         # (planned) JDBC sink configs for PostGIS
└── grafana/                         # (planned) provisioned dashboards
```
 
## Considerations
 
- Streams windows run on the quake's own origin time, not ingest time, so backfilled and live data window correctly
- Backfill and live data shouldn't share one Streams app at the same time: old events look "late" and get dropped from closed windows. Run the backfill first, or give it its own topic and `application.id`
- The live feed is small. Don't expect a firehose until the backfill is in
- Swarm and aftershock logic is a simplified take on magnitude-scaled space/time windows. Look up the published declustering tables before calling it scientifically defensible
## Testing
 
Streams logic will be tested with Kafka's `TopologyTestDriver` against recorded feed snapshots: duplicate polls, magnitude revisions across a band, swarm thresholds, aftershocks inside and outside the radius, and out-of-order events.
 
```bash
mvn test
```
 
## Credits
 
Earthquake data from the [U.S. Geological Survey](https://earthquake.usgs.gov/).
