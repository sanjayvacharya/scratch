package com.welflex.aws.dynamodb.model;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.welflex.aws.dynamodb.repository.LocalDateTimeConverter;

import java.time.LocalDateTime;

@DynamoDBTable(tableName = "comments")
public class Comment {
    @DynamoDBHashKey
    private String itemId;

    @DynamoDBRangeKey
    @DynamoDBAutoGeneratedKey
    private String messageId;

    @DynamoDBIndexHashKey(globalSecondaryIndexName = "UserId-index")
    private String userId;

    @DynamoDBAttribute
    private String message;


    @DynamoDBAttribute
    @DynamoDBTypeConverted(converter = LocalDateTimeConverter.class)
    private LocalDateTime dateTime;


    @DynamoDBIndexRangeKey(globalSecondaryIndexName = "UserId-index", localSecondaryIndexName = "Rating-index")
    private int rating;


    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public void setDateTime(LocalDateTime dateTime) {
        this.dateTime = dateTime;
    }

    public int getRating() {
        return rating;
    }

    public void setRating(int rating) {
        this.rating = rating;
    }

    @Override
    public String toString() {
        return "Comment{" +
                "itemId='" + itemId + '\'' +
                ", messageId='" + messageId + '\'' +
                ", userId='" + userId + '\'' +
                ", message='" + message + '\'' +
                ", dateTime=" + dateTime +
                ", rating=" + rating +
                '}';
    }
}
