import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClientBuilder;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import com.amazonaws.services.dynamodbv2.local.shared.access.AmazonDynamoDBLocal;
import com.amazonaws.services.dynamodbv2.model.*;
import com.amazonaws.services.dynamodbv2.streamsadapter.AmazonDynamoDBStreamsAdapterClient;
import com.amazonaws.services.dynamodbv2.streamsadapter.StreamsWorkerFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.google.common.collect.Lists;
import com.welflex.aws.dynamodb.model.Comment;
import com.welflex.aws.dynamodb.model.Note;
import com.welflex.aws.dynamodb.model.Order;
import com.welflex.aws.dynamodb.repository.*;
import com.welflex.aws.dynamodb.repository.stream.OrderStreamRecordProcessorFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel.SUMMARY;

public class Main {
    public static void main(String args[]) throws InterruptedException {
        AmazonDynamoDBLocal amazonDynamoDBLocal = DynamoDBEmbedded.create();


        AmazonDynamoDB dynamoDB = amazonDynamoDBLocal.amazonDynamoDB();
        AmazonDynamoDBStreams dynamoDBStreams = amazonDynamoDBLocal.amazonDynamoDBStreams();

        Util.<Note>createTable(dynamoDB, Note.class, false);
        Util.<Comment>createTable(dynamoDB, Comment.class, false);
        Util.<Order>createTable(dynamoDB, Order.class, true);

        streamDemo(dynamoDB);

        DescribeTableResult describeTableResult = dynamoDB.describeTable("Orders");
        String streamArn = describeTableResult.getTable().getLatestStreamArn();
        StreamSpecification orderStreamSpecification = describeTableResult.getTable().getStreamSpecification();

        AmazonDynamoDBStreamsAdapterClient adapterClient = new AmazonDynamoDBStreamsAdapterClient(dynamoDBStreams);
        AmazonCloudWatchClient amazonCloudWatchClient = new AmazonCloudWatchClient(new StaticCredentialsProvider(new BasicAWSCredentials("NotAnAccessKey", "NotASecretKey")), new ClientConfiguration());

        KinesisClientLibConfiguration workerConfig = new KinesisClientLibConfiguration("order-stream-processor", streamArn,
                new StaticCredentialsProvider(new BasicAWSCredentials("NotAnAccessKey", "NotASecretKey")), "order-processor-worker")
                .withMaxRecords(1000)
                .withIdleTimeBetweenReadsInMillis(500)
                .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON)
                .withMetricsLevel(MetricsLevel.NONE);

        Worker worker = StreamsWorkerFactory.createDynamoDbStreamsWorker(new OrderStreamRecordProcessorFactory(), workerConfig, adapterClient, dynamoDB, amazonCloudWatchClient );

        Thread t = new Thread(worker);
        t.start();

        Thread.sleep(25000);
       // worker.shutdown();
        t.join();

        if (true) {
            return;
        }
        String lastEvaluatedShardId = null;

        do {
            DescribeStreamResult describeStreamResult = dynamoDBStreams.describeStream(
                    new DescribeStreamRequest()
                            .withStreamArn(streamArn)
                            .withExclusiveStartShardId(lastEvaluatedShardId));
            List<Shard> shards = describeStreamResult.getStreamDescription().getShards();

            // Process each shard on this page
            int maxItemCount = 500;

            for (Shard shard : shards) {
                String shardId = shard.getShardId();
                System.out.println("Shard: " + shard);

                // Get an iterator for the current shard

                GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest()
                        .withStreamArn(streamArn)
                        .withShardId(shardId)
                        .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
                GetShardIteratorResult getShardIteratorResult =
                        dynamoDBStreams.getShardIterator(getShardIteratorRequest);
                String currentShardIter = getShardIteratorResult.getShardIterator();

                // Shard iterator is not null until the Shard is sealed (marked as READ_ONLY).
                // To prevent running the loop until the Shard is sealed, which will be on average
                // 4 hours, we process only the items that were written into DynamoDB and then exit.
                int processedRecordCount = 0;
                while (currentShardIter != null && processedRecordCount < maxItemCount) {
                    System.out.println("    Shard iterator: " + currentShardIter.substring(380));

                    // Use the shard iterator to read the stream records

                    GetRecordsResult getRecordsResult = dynamoDBStreams.getRecords(new GetRecordsRequest()
                            .withShardIterator(currentShardIter));
                    List<Record> records = getRecordsResult.getRecords();
                    for (Record record : records) {
                        System.out.println("        " + record.getDynamodb());
                    }
                    processedRecordCount += records.size();
                    currentShardIter = getRecordsResult.getNextShardIterator();
                }
            }

            // If LastEvaluatedShardId is set, then there is
            // at least one more page of shard IDs to retrieve
            lastEvaluatedShardId = describeStreamResult.getStreamDescription().getLastEvaluatedShardId();

        } while (lastEvaluatedShardId != null);





        CommentRepositoryImpl repository = new CommentRepositoryImpl(dynamoDB);
        Comment c1 = repository.save(newComment("1", "Delivered on time", "10", 5));
        Comment c2 = repository.save(newComment("1", "As advertised. Very happy", "11", 5));
        Comment c3 = repository.save(newComment("1", "Not on time", "19", 2));
        Comment c4 = repository.save(newComment("2", "Perfect Product", "10", 10));


        List<Comment> comments = repository.getAllForItem("1");

        comments.stream().forEach(comment -> System.out.println(comment));

        System.out.println("Items with Min Rating of 3 or above");
        comments = repository.getAllForItemWithRatingGE("1", 3);

        comments.stream().forEach(comment -> System.out.println(comment));

        System.out.println("All for user 10");

        comments = repository.getForUser("10");
        comments.stream().forEach(comment -> System.out.println(comment));




    }


    private static void streamDemo(AmazonDynamoDB dynamoDB) throws InterruptedException {
        OrderRepositoryImpl orderRepository = new OrderRepositoryImpl(dynamoDB);
        Random random = new Random();
        for (int i = 0 ; i < 50; i++) {
            Order order = new Order();
            order.setItemIds(Lists.newArrayList("1", "2"));
            order.setTotalPrice(random.nextInt(1000));
            orderRepository.save(order);
            System.out.println("Created Order:" + order);
            //Thread.sleep(1000);
        }
    }

    private static Comment newComment(String itemId, String message, String userId, int rating) {
        Comment comment = new Comment();

        comment.setItemId(itemId);
        comment.setMessage(message);
        comment.setUserId(userId);
        comment.setRating(rating);
        comment.setDateTime(LocalDateTime.now());
        return comment;
    }
}
