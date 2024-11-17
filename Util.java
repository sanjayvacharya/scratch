package com.welflex.aws.dynamodb.repository;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.welflex.aws.dynamodb.model.Note;

import java.util.Arrays;
import java.util.function.Consumer;

public class Util {

    public static <T> void createTable(AmazonDynamoDB dynamoDB, Class<T> clazz, boolean enableStreams) {
        DynamoDBMapper dynamoDBMapper  = new DynamoDBMapper(dynamoDB);
        createTable(clazz, dynamoDBMapper, dynamoDB, enableStreams);
    }

    private static <T> void createTable(Class clazz, DynamoDBMapper dynamoDBMapper, AmazonDynamoDB dynamoDB, boolean enableStreams) {
        System.out.println("Dynamo DB:" + dynamoDB);

        CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(clazz);
        createTableRequest.withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

        if (createTableRequest.getGlobalSecondaryIndexes() != null) {
            for (GlobalSecondaryIndex gsi : createTableRequest.getGlobalSecondaryIndexes()) {
                gsi.withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
                gsi.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
            }
        }

        if (createTableRequest.getLocalSecondaryIndexes() != null) {
            for (LocalSecondaryIndex lsi : createTableRequest.getLocalSecondaryIndexes()) {
                lsi.withProjection(new Projection().withProjectionType(ProjectionType.ALL));
            }
        }

        if (enableStreams) {
            StreamSpecification streamSpecification = new StreamSpecification();
            streamSpecification.setStreamEnabled(true);
            streamSpecification.setStreamViewType(StreamViewType.NEW_IMAGE);
            createTableRequest.withStreamSpecification(streamSpecification);
        }

        if (!tableExists(dynamoDB, createTableRequest)) {
            dynamoDB.createTable(createTableRequest);
            waitForTableCreation(createTableRequest.getTableName(), dynamoDB);
        }
    }

    private static boolean tableExists(AmazonDynamoDB dynamoDB, CreateTableRequest createTableRequest) {
        try {
            dynamoDB.describeTable(createTableRequest.getTableName());
            return true;
        }
        catch (ResourceNotFoundException e) {
            return false;
        }
     }

     public static void deleteTable(final AmazonDynamoDB dynamoDB, String ...tableNames) {
         Arrays.stream(tableNames).forEach(s -> dynamoDB.deleteTable(new DeleteTableRequest().withTableName(s)));
     }

     private static void waitForTableCreation(String tableName, AmazonDynamoDB dynamoDB) {
        while (true) {
            try {
                Thread.sleep(500);
                DescribeTableResult tds = dynamoDB.describeTable(new DescribeTableRequest(tableName));
                if (tds == null) {
                    continue;
                }
                TableDescription td = tds.getTable();
                String tableStatus = td.getTableStatus();

                if (tableStatus.equals(TableStatus.ACTIVE.toString())) {
                    return;
                }
            }
            catch (ResourceNotFoundException e) {
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
     }
}
