package org.example.kafkastream.application;

import org.example.kafkastream.pubsub.Subscriber;

import java.util.*;

public class MainApplication {
    private List<String> replicaBrokers = new ArrayList();

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

    public void MainApplication() {
        replicaBrokers = new ArrayList();
    }

    private void run(long maxReads, String topic, int partition, List<String> seeds_brockers
            , int port) throws Exception {
        Subscriber subscriber = new Subscriber();
        subscriber.initialize(maxReads, topic, partition, seeds_brockers, port, replicaBrokers);
    }




}
