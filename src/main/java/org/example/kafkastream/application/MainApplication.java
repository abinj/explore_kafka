package org.example.kafkastream.application;

import com.sun.javaws.IconUtil;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.common.requests.MetadataResponse;

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

    public void MongoDBSimpleConsumer() {
        replicaBrokers = new ArrayList();
    }

    private void run(long maxReads, String topic, int partition, List<String> seeds_brockers, int port) {
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
