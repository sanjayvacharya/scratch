package com.welflex.aws.dynamodb.repository;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.welflex.aws.dynamodb.model.Order;

import java.util.List;

public class OrderRepositoryImpl {


    private final DynamoDBMapper mapper;

    public OrderRepositoryImpl(AmazonDynamoDB dynamoDB) {
        this.mapper = new DynamoDBMapper(dynamoDB);
    }

    public Order save(Order order) {
        mapper.save(order);
        return order;
    }

    public Order get(String id) {
        return mapper.load(Order.class, id);
    }

    public void delete(String id) {
        Order order = new Order();
        order.setId(id);

        mapper.delete(order);
    }

    public List<Order> getAll() {
        return mapper.scan(Order.class, new DynamoDBScanExpression());
    }
}
