package com.welflex.aws.dynamodb.repository;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTypeConverter;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class LocalDateTimeConverter implements DynamoDBTypeConverter<String, LocalDateTime> {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    @Override
    public String convert(LocalDateTime localDateTime) {
        return FORMATTER.format(localDateTime);
    }

    @Override
    public LocalDateTime unconvert(String s) {
        return LocalDateTime.parse(s, FORMATTER);
    }
}
