package com.welflex.aws.dynamodb;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;
import com.amazonaws.services.dynamodbv2.streamsadapter.model.RecordAdapter;
import com.amazonaws.services.dynamodbv2.xspec.L;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.collect.Lists;
import com.welflex.aws.dynamodb.model.Order;
import com.welflex.aws.dynamodb.repository.OrderRepositoryImpl;
import com.welflex.aws.dynamodb.repository.Util;

import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class OrderKinesisStreamIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderKinesisStreamIT.class);

    private AmazonDynamoDB dynamoDB;
    private AmazonDynamoDBStreamsAdapterClient adapterClient;
    private AmazonCloudWatchClient amazonCloudWatchClient;

    private StreamSpecification orderStreamSpecification;
    private String streamArn;

    @BeforeEach
    public void setUp() {
        dynamoDB =  AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", Regions.US_WEST_1.getName()))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar"))).build();

        AmazonDynamoDBStreams dynamoDBStreams =   AmazonDynamoDBStreamsClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", Regions.US_WEST_1.getName()))
                .build();

        adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreams);


        amazonCloudWatchClient = new AmazonCloudWatchClient(new StaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")), new ClientConfiguration());
        Util.<Order>createTable(dynamoDB, Order.class, true);
        DescribeTableResult describeTableResult = dynamoDB.describeTable("Orders");
        orderStreamSpecification = describeTableResult.getTable().getStreamSpecification();
        streamArn = describeTableResult.getTable().getLatestStreamArn();
        LOGGER.info("Order Stream ARN {}", streamArn);
    }

    @AfterEach
    public void tearDown() {
        Util.deleteTable(dynamoDB, "Orders");
    }

    @Test
    public void orderTest() throws InterruptedException {

        KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration("order-stream-processor", streamArn,
                new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")), "order-processor-worker")
                .withMaxRecords(1000)
                .withIdleTimeBetweenReadsInMillis(500)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withMetricsLevel(MetricsLevel.NONE);

        CountDownLatch latch = new CountDownLatch(10);

        Worker worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(new TestOrderProcessorFactory(latch), workerConfig, adapterClient, dynamoDB, amazonCloudWatchClient );

        Thread t = new Thread(worker);
        t.start();
        OrderTestUtil.createOrders(latch.getCount(), dynamoDB);

        latch.await();
        worker.shutdown();
        t.join();

        LOGGER.info("Publishing additional order entries...");
        latch = new CountDownLatch(10);
        worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(new TestOrderProcessorFactory(latch), workerConfig, adapterClient, dynamoDB, amazonCloudWatchClient );

        t = new Thread(worker);
        t.start();
        OrderTestUtil.createOrders(latch.getCount(), dynamoDB);


        latch.await();
        worker.shutdown();
        t.join();
    }

    /**
     * This does not matter. Only one of the Workers will get the lease and run the other fails.
     * Example:
     *
     * INFO: Worker Worker -2 successfully took 1 leases: shardId-00000001593472712187-53afca34
     * Created Order:Order{id='bb88f812-247d-4b4a-bc63-954521399c1d', totalPrice=485.0, itemIds=[1, 2]}
     * Jun 29, 2020 5:18:33 PM com.amazonaws.services.kinesis.leases.impl.LeaseTaker takeLeases
     * INFO: Worker Worker -1 failed to take 1 leases: shardId-00000001593472712187-53afca34
     *
     * @throws InterruptedException
     */
    //@Test
    public void parallelConsumer() throws InterruptedException {
        String[] workerNames = new String[] {"Worker -1", "Worker -2"};

        CountDownLatch latch = new CountDownLatch(workerNames.length);

        Worker[] workers = new Worker[workerNames.length];
        Thread[] threads = new Thread[workerNames.length];

        for (int i = 0; i < workerNames.length; i++) {
            String workerName = workerNames[i];
            LOGGER.info("Creating a Worker:" + workerName);

            KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration("order-stream-processor", streamArn,
                    new AWSStaticCredentialsProvider(new BasicAWSCredentials("foo", "bar")), workerName)
                    .withMaxRecords(10)
                    .withIdleTimeBetweenReadsInMillis(500)
                    .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                    .withMetricsLevel(MetricsLevel.NONE);

            workers[i] = StreamsWorkerFactory.createDynamoDbStreamsWorker(new TestOrderProcessorFactory(latch, workerName), workerConfig, adapterClient, dynamoDB, amazonCloudWatchClient);
            threads[i] = new Thread(workers[i]);

            threads[i].start();
        }

        OrderTestUtil.createOrders(latch.getCount(), dynamoDB);
        latch.await();

        for (int i = 0; i < workerNames.length; i++) {
            workers[i].shutdown();
            threads[i].join();
        }
    }

    private static final class TestOrderProcessorFactory implements IRecordProcessorFactory {
        private final CountDownLatch latch;
        private final String processor;

        public TestOrderProcessorFactory(CountDownLatch latch) {
            this(latch, "DEFAULT_ORDER_PROCESSOR");
        }

        public TestOrderProcessorFactory(CountDownLatch latch, String processor) {

            this.latch = latch;
            this.processor = processor;
        }

        @Override
        public IRecordProcessor createProcessor() {
            return new TestOrderProcessor(latch, processor);
        }
    }

    private static final class TestOrderProcessor implements  IRecordProcessor {
        private static final Logger LOGGER = LoggerFactory.getLogger(TestOrderProcessor.class);

        private int receivedOrderCount = 0;
        private final CountDownLatch latch;
        private final String processor;

        public TestOrderProcessor(CountDownLatch latch) {
            this(latch, "DEFAULT_ORDER_PROCESSOR");
        }

        public TestOrderProcessor(CountDownLatch latch, String processor) {
            this.latch = latch;
            this.processor = processor;
        }

        @Override
        public void initialize(InitializationInput initializationInput) {

        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            for (Record record : processRecordsInput.getRecords()) {

                if (record instanceof RecordAdapter) {
                    com.amazonaws.services.dynamodbv2.model.Record streamRecord
                            = ((RecordAdapter) record).getInternalObject();

                    switch (streamRecord.getEventName()) {
                        case "INSERT":
                        case "UPDATE":
                            Map<String, AttributeValue> values = streamRecord.getDynamodb()
                                    .getNewImage();
                            latch.countDown();
                            receivedOrderCount++;
                            LOGGER.info("Processor {}, Stream Order {}", processor, values.toString());
                            LOGGER.info("Processor {} , Current Received Count {}", processor, String.valueOf(receivedOrderCount));
                            break;
                    }
                }
            }
            checkPoint(processRecordsInput.getCheckpointer());
        }

        @Override
        public void shutdown(ShutdownInput shutdownInput) {
            if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
                try {
                    shutdownInput.getCheckpointer().checkpoint();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }


        private void checkPoint(IRecordProcessorCheckpointer checkpointer) {
            try {
                checkpointer.checkpoint();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
