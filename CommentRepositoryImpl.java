package com.welflex.aws.dynamodb.repository;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.welflex.aws.dynamodb.model.Comment;

import java.util.List;

public class CommentRepositoryImpl {
    private final DynamoDBMapper mapper;

    public CommentRepositoryImpl(AmazonDynamoDB dynamoDB) {
        this.mapper = new DynamoDBMapper(dynamoDB);
    }

    public Comment save(Comment comment) {
        mapper.save(comment);
        return comment;
    }

    public Comment get(String itemId, String messageId) {
        Comment comment = new Comment();

        comment.setItemId(itemId);
        comment.setMessageId(messageId);

        return mapper.load(comment);
    }

    public List<Comment> getAll() {
        return mapper.scan(Comment.class, new DynamoDBScanExpression());
    }

    public List<Comment> getAllForItem(String itemId) {
        Comment comment = new Comment();
        comment.setItemId(itemId);

        DynamoDBQueryExpression<Comment> expression
                = new DynamoDBQueryExpression<Comment>().withHashKeyValues(comment);
        return mapper.query(Comment.class, expression);
    }

    public List<Comment> getAllForItemWithRatingGE(String itemId, int minRating) {
        Comment comment = new Comment();
        comment.setItemId(itemId);

        DynamoDBQueryExpression<Comment> expression
                = new DynamoDBQueryExpression<Comment>().withHashKeyValues(comment)
                .withRangeKeyCondition("rating", new Condition().withComparisonOperator(ComparisonOperator.GE)
                .withAttributeValueList(new AttributeValue().withN(Integer.toString(minRating))));

        return mapper.query(Comment.class, expression);
    }

    public List<Comment> getForUser(String userId) {
        Comment comment = new Comment();
        comment.setUserId(userId);

        DynamoDBQueryExpression<Comment> expression
                = new DynamoDBQueryExpression<Comment>().withHashKeyValues(comment)
                .withConsistentRead(false);

        return mapper.query(Comment.class, expression);
    }
}
