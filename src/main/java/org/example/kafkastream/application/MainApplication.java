package org.example.kafkastream.application;

import kafka.consumer.SimpleConsumer;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MainApplication {
    private List replicaBrokers = new ArrayList();

    public static void main(String[] args) {
        MainApplication application = new MainApplication();
        long maxReads = 100;
        String topic = "mongo-News";
        int partition = 0;
        List<String> seeds = new ArrayList<String>();
        seeds.add("127.0.0.1");
        int port = 9092;
        try {
            application.run(maxReads, topic, partition, seeds, port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void MongoDBSimpleConsumer() {
        replicaBrokers = new ArrayList();
    }

    private void run(long maxReads, String topic, int partition, List<String> seeds_brockers, int port) {
        PartitionMetadata metadata = findLeader(seeds_brockers, port, topic, partition);

    }

    private PartitionMetadata findLeader(List<String> seeds_brockers, int port, String topic, int partition) {
        PartitionMetadata metadata = null;
        loop:
        for (String seed: seeds_brockers){
             SimpleConsumer consumer = null;

             try {
                 consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
                 List<String> topics = Collections.singletonList(topic);
                 TopicMetadataRequest req = new TopicMetadataRequest(topics);
//                 TopicMetadataResponse respo = consumer.send(req);

             } catch (Exception e) {
                 e.printStackTrace();
             }
        }
    }


}
