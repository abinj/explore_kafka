package org.example.kafkastream.pubsub;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafkastream.listeners.FeedListener;
import org.example.kafkastream.models.News;

import java.util.Properties;

public class Publisher implements FeedListener {
    private KafkaProducer<String, String> kafkaProducer;
    private final Gson gson = new Gson();

    public void initialize() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:4242");
        props.put("metadata.broker.list", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProducer = new KafkaProducer<String, String>(props);
    }


    @Override
    public void onNewNews(News news) {
        String newsJson = gson.toJson(news);
        kafkaProducer.send(new ProducerRecord<String, String>("mongo-News", news.getLink(), newsJson));
    }
}
