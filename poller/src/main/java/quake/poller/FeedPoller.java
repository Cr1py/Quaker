package quake.poller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import quake.model.Quake;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FeedPoller {
  private static final String FEED = System.getenv().getOrDefault("FEED_URL",
      "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson");
  private static final String TOPIC = "quake.raw";

  private final HttpClient http = HttpClient.newBuilder()
      .connectTimeout(Duration.ofSeconds(10)).build();
  private final ObjectMapper json = new ObjectMapper();
  private final Map<String, Long> seen = new HashMap<>();
  private final Producer<String, Quake> producer;

  FeedPoller() {
    Properties p = new Properties();
    p.put("bootstrap.servers", System.getenv().getOrDefault("BOOTSTRAP", "localhost:9092"));
    p.put("schema.registry.url", System.getenv().getOrDefault("SCHEMA_REGISTRY", "http://localhost:8081"));
    p.put("key.serializer", StringSerializer.class.getName());
    p.put("value.serializer", KafkaAvroSerializer.class.getName());
    p.put("acks", "all");
    p.put("enable.idempotence", "true");
    this.producer = new KafkaProducer<>(p);
  }

  void pollOnce() {
    try {
      HttpResponse<String> res = http.send(
          HttpRequest.newBuilder(URI.create(FEED)).timeout(Duration.ofSeconds(20)).GET().build(),
          HttpResponse.BodyHandlers.ofString());
      if (res.statusCode() != 200) {
        System.err.println("Feed returned " + res.statusCode());
        return;
      }
      int sent = 0;
      for (JsonNode f : json.readTree(res.body()).get("features")) {
        Quake q = toQuake(f);
        Long prev = seen.get(q.getId());
        if (prev != null && prev >= q.getUpdated()) continue;
        seen.put(q.getId(), q.getUpdated());
        producer.send(new ProducerRecord<>(TOPIC, q.getId(), q), (md, ex) -> {
          if (ex != null) ex.printStackTrace();
        });
        sent++;
      }
      producer.flush();
      System.out.printf("Polled: %d new/updated events published%n", sent);
    } catch (Exception e) {
      e.printStackTrace(); 
    }
  }

  static Quake toQuake(JsonNode f) {
    JsonNode p = f.get("properties");
    JsonNode c = f.get("geometry").get("coordinates");   // [lon, lat, depth]
    return Quake.newBuilder()
        .setId(f.get("id").asText())
        .setTime(p.get("time").asLong())
        .setUpdated(p.get("updated").asLong())
        .setMag(p.get("mag").isNull() ? null : p.get("mag").asDouble())
        .setMagType(text(p, "magType"))
        .setPlace(text(p, "place"))
        .setLon(c.get(0).asDouble())
        .setLat(c.get(1).asDouble())
        .setDepth(c.size() > 2 && !c.get(2).isNull() ? c.get(2).asDouble() : null)
        .setTsunami(p.path("tsunami").asInt(0))
        .setFelt(p.get("felt") == null || p.get("felt").isNull() ? null : p.get("felt").asInt())
        .setStatus(text(p, "status"))
        .build();
  }

  private static String text(JsonNode n, String k) {
    return n.get(k) == null || n.get(k).isNull() ? null : n.get(k).asText();
  }

  public static void main(String[] args) {
    FeedPoller poller = new FeedPoller();
    ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
    ses.scheduleWithFixedDelay(poller::pollOnce, 0, 60, TimeUnit.SECONDS);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      ses.shutdown();
      poller.producer.close(Duration.ofSeconds(5));
    }));
  }
}
