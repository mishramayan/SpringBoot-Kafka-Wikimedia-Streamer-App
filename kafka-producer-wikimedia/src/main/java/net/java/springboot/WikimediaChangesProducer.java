//package net.java.springboot;
//
//import com.launchdarkly.eventsource.EventSource;
//import com.launchdarkly.eventsource.background.BackgroundEventHandler;
//import com.launchdarkly.eventsource.background.BackgroundEventSource;
//import okhttp3.Headers;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.stereotype.Service;
//
//import java.net.URI;
//import java.util.concurrent.TimeUnit;
//
//@Service
//public class WikimediaChangesProducer {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesProducer.class);
//
//    private KafkaTemplate<String, String> kafkaTemplate;
//
//    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
//        this.kafkaTemplate = kafkaTemplate;
//    }
//
//    public void sendMessage() throws InterruptedException{
//        String topic = "wikimedia-recentchange";
//
//        // 1. Create the Handler
//        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topic);
//        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
//
//        // 2. Create the URI (This part is correct)
//        URI uri = URI.create(url);
//
//        // 3. WRAP the URI in an EventSource.Builder
//        EventSource.Builder esBuilder = new EventSource.Builder(uri);
//
//        // 4. Pass the 'esBuilder' (not the URI) to the BackgroundEventSource.Builder
//        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, esBuilder);
//
//        // 5. Build and Start the source
//        BackgroundEventSource eventSource = builder.build();
//        eventSource.start();
//
//        TimeUnit.MINUTES.sleep(10);
//    }
//}


package net.java.springboot;

import com.launchdarkly.eventsource.ConnectStrategy; // <--- NEW IMPORT
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
public class WikimediaChangesProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaChangesProducer.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    public WikimediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage() throws InterruptedException {
        String topic = "wikimedia-recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(kafkaTemplate, topic);

        // 1. Configure the Connection Strategy (URL + Headers go here)
        EventSource.Builder esBuilder = new EventSource.Builder(
                ConnectStrategy.http(URI.create(url))
                        .header("User-Agent", "Spring-Boot-Kafka-Student-Project/1.0")
        );

        // 2. Pass to Background Builder
        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, esBuilder);

        // 3. Start
        BackgroundEventSource eventSource = builder.build();
        eventSource.start();

        // 4. Keep alive
        TimeUnit.MINUTES.sleep(10);
    }
}