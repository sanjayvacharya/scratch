package com.welflex.aws.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.google.common.collect.Lists;
import com.welflex.aws.dynamodb.model.Order;
import com.welflex.aws.dynamodb.repository.Util;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class OrderLowLevelStreamIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderLowLevelStreamIT.class);

    private AmazonDynamoDB dynamoDB;
    private AmazonDynamoDBStreamsAdapterClient adapterClient;
    private AmazonCloudWatchClient amazonCloudWatchClient;
    private AmazonDynamoDBStreams dynamoDBStreams;

    private StreamSpecification orderStreamSpecification;
    private String streamArn;

    @BeforeEach
    public void setUp() {
        dynamoDB =  AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", Regions.US_WEST_1.getName()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar"))).build();

        dynamoDBStreams =   AmazonDynamoDBStreamsClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", Regions.US_WEST_1.getName()))
                .build();

        adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreams);


        amazonCloudWatchClient = new AmazonCloudWatchClient(new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")), new ClientConfiguration());
        Util.<Order>createTable(dynamoDB, Order.class, true);

        DescribeTableResult describeTableResult = dynamoDB.describeTable("Orders");
        orderStreamSpecification = describeTableResult.getTable().getStreamSpecification();
        streamArn = describeTableResult.getTable().getLatestStreamArn();
        LOGGER.info("Order Stream ARN {}", streamArn);
    }

    @Test
    public void orderStreamTest() throws InterruptedException {

        DescribeStreamResult describeStreamResult = dynamoDBStreams.describeStream(
                new DescribeStreamRequest()
                        .withStreamArn(streamArn)
                        .withExclusiveStartShardId(null));


        Map<Shard, String> shardToLastProceed = describeStreamResult.getStreamDescription()
            .getShards().stream().collect(Collectors.toMap(shard -> shard, shard -> ""));

        // Run and Collect ShardReader result
        LOGGER.info("Starting Stream Readers and sending orders...");
        List<ShardReader> shardReaders = runAndStreamOrders(shardToLastProceed);

        List<String> originalRunOrders =  shardReaders.stream().map(shardReader -> shardReader.getOrdersProcessed())
                .reduce((strings, strings2) -> {
                    List<String> combined = Lists.newArrayList(strings);
                    combined.addAll(strings2);
                    return combined;
                }).get();

        shardToLastProceed = shardReaders.stream().collect(Collectors.toMap(shardReader -> shardReader.getShard(),
                shardReader -> shardReader.getLastProcessedSequenceNumber()));

        LOGGER.info("Starting Stream Readers and sending orders second time...");
        List<ShardReader> secondRunReaders = runAndStreamOrders(shardToLastProceed);

        List<String> secondRunOrders = secondRunReaders.stream().map(shardReader -> shardReader.getOrdersProcessed())
                .reduce((strings, strings2) -> {
                    List<String> combined = Lists.newArrayList(strings);
                    combined.addAll(strings2);
                    return combined;
                }).get();

        assertFalse(secondRunOrders.stream().anyMatch(originalRunOrders::contains));
    }

    private List<ShardReader> runAndStreamOrders(Map<Shard, String> shardToLastProcessed) throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(100);

        List<ShardReader> shardReaders = Lists.newArrayList();

        shardToLastProcessed.forEach((shard, s) -> shardReaders.add(new ShardReader(shard, streamArn, s, dynamoDBStreams, latch)));;

        shardReaders.forEach(thread -> thread.start());

        OrderTestUtil.createOrders(100, dynamoDB);
        latch.await();

        shardReaders.forEach(thread -> thread.interrupt());

        return shardReaders;
    }


    public static class ShardReader extends Thread {
        private final Shard shard;
        private final String streamArn;
        private final AmazonDynamoDBStreams dynamoDBStreams;
        private final CountDownLatch latch;
        private final List<String> orderIdSet;
        private String lastProcessedSequenceNumber;

        public ShardReader(Shard shard, String streamArn,
                           String lastProcessedSequenceNumber,
                           AmazonDynamoDBStreams dynamoDBStreams, CountDownLatch latch) {
            this.lastProcessedSequenceNumber = lastProcessedSequenceNumber;
            this.shard = shard;
            this.streamArn = streamArn;
            this.dynamoDBStreams = dynamoDBStreams;
            this.latch = latch;
            this.orderIdSet = Lists.newArrayList();
            LOGGER.info("Shard Reader created for Shard {}", shard.getShardId());
        }

        public List<String> getOrdersProcessed() {
            return orderIdSet;
        }

        public String getLastProcessedSequenceNumber() {
            return lastProcessedSequenceNumber;
        }

        public Shard getShard() {
            return shard;
        }

        @Override
        public void run() {
            LOGGER.info("Resuming Shard {} from Sequence {}", shard.getShardId(), lastProcessedSequenceNumber);
            boolean firstRun = "".equals(lastProcessedSequenceNumber);
            LOGGER.info("Is this the first time we are reading from the stream? {}", firstRun);

            GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                    .withStreamArn(streamArn)
                    .withShardId(shard.getShardId())
                    .withSequenceNumber(firstRun? null : lastProcessedSequenceNumber)
                    .withShardIteratorType(firstRun ? ShardIteratorType.TRIM_HORIZON : ShardIteratorType.AFTER_SEQUENCE_NUMBER);

            GetShardIteratorResult getShardIteratorResult =
                    dynamoDBStreams.getShardIterator(getShardIteratorRequest);

            String currentShardIter = getShardIteratorResult.getShardIterator();
            LOGGER.info("Current Shard iterator {}", currentShardIter);

            while (currentShardIter != null && latch.getCount() > 0) {
                // Use the shard iterator to read the stream records
                GetRecordsResult getRecordsResult = dynamoDBStreams.getRecords(new GetRecordsRequest()
                        .withShardIterator(currentShardIter));

                List<Record> records = getRecordsResult.getRecords();

                for (Record record : records) {
                    StreamRecord streamRecord = record.getDynamodb();

                  //  System.out.println("   " + streamRecord);
                    Map<String, AttributeValue> keys = streamRecord.getKeys();
                    AttributeValue id = keys.get("id");
                    orderIdSet.add(id.getS());

                    lastProcessedSequenceNumber = streamRecord.getSequenceNumber();
                    latch.countDown();
                }

                currentShardIter = getRecordsResult.getNextShardIterator();
            }
        }
    }

}
