package com.welflex.aws.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.google.common.collect.Lists;
import com.welflex.aws.dynamodb.model.Order;
import com.welflex.aws.dynamodb.repository.OrderRepositoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class OrderTestUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderTestUtil.class);

    public static List<String> createOrders(long count, AmazonDynamoDB dynamoDB)  {
        List<String> createdOrders = Lists.newArrayList();

        OrderRepositoryImpl orderRepository = new OrderRepositoryImpl(dynamoDB);
        Random random = new Random();

        for (int i = 0 ; i < count; i++) {
            Order order = new Order();
            order.setItemIds(Lists.newArrayList("1", "2"));
            order.setTotalPrice(random.nextInt(1000));
            orderRepository.save(order);

            createdOrders.add(order.getId());
        }
        return createdOrders;
    }
}
