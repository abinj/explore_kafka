package org.example.kafkastream.application;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.sun.javaws.IconUtil;
import kafka.api.FetchRequestBuilder;
import kafka.api.FetchRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.requests.MetadataResponse;
import org.example.kafkastream.models.News;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.*;

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

    public void MainApplication() {
        replicaBrokers = new ArrayList();
    }

    private void run(long maxReads, String topic, int partition, List<String> seeds_brockers, int port) throws Exception {
        PartitionMetadata metadata = findLeader(seeds_brockers, port, topic, partition);

        if (metadata == null) {
            System.out.println("Can't find partition for Topic and Partition. Exiting");
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find partition for Topic and Partition. Exiting");
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + topic + "_" + partition;

        SimpleConsumer consumer= new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
        long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);

        int numErrors = 0;
        while (maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, readOffset, 100000)
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);
            if (fetchResponse.hasError()) {
                numErrors++;
                short code = fetchResponse.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker: " +leadBroker + " Reason: " + code);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime()
                            , clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, topic, partition, port);
                continue;
            }
            numErrors = 0;
            long numRead = 0;

            MongoClient client = new MongoClient();
            MongoDatabase db = client.getDatabase("News");
            MongoCollection cnnCollection = db.getCollection("cnn");
            Gson gson = new Gson();
            Type type = new TypeToken() {}.getType();

            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                News news = gson.fromJson(new String(bytes, "UTF-8"), type);
                System.out.println(news);

                cnnCollection.insertOne(news.getNewsAsDocument());

                numRead++;
                maxReads--;
            }

            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    ie.printStackTrace();
                }
            }
        }
        if (consumer != null) consumer.close();
    }

    private String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(replicaBrokers, port, topic, partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }


    private long getLastOffset(SimpleConsumer consumer, String topic, int partition, long earliestTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map requestInfo = new HashMap();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(earliestTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo,kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data offset Data the Broker. Reason: "
                    + response.errorCode(topic, partition));
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private PartitionMetadata findLeader(List<String> seeds_brockers, int port, String topic, int partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed: seeds_brockers){
             SimpleConsumer consumer = null;

             try {
                 consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
                 List<String> topics = Collections.singletonList(topic);
                 TopicMetadataRequest req = new TopicMetadataRequest(topics);
                 TopicMetadataResponse respo = consumer.send(req);

                 List<TopicMetadata> metaData = respo.topicsMetadata();
                 for (TopicMetadata item : metaData) {
                     for (PartitionMetadata partitionMetadata : item.partitionsMetadata()) {
                         if (partitionMetadata.partitionId() == partition) {
                             returnMetaData = partitionMetadata;
                             break loop;
                         }
                     }
                 }
             } catch (Exception e) {
                 System.out.println("Error Communicating with broker [" + seed + "] to find leader for [" + topic
                         + ", " + partition + "] Reason: " + e);
             } finally {
                 if (consumer != null) consumer.close();
             }
        }
        if (returnMetaData != null) {
            replicaBrokers.clear();
            for (Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }


}
