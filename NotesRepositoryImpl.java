package com.welflex.aws.dynamodb.repository;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.welflex.aws.dynamodb.model.Note;

public class NotesRepositoryImpl implements  NotesRepository {
    private final DynamoDBMapper mapper;

    public NotesRepositoryImpl(AmazonDynamoDB dynamoDB) {
        this.mapper = new DynamoDBMapper(dynamoDB);
    }

    @Override
    public Note saveNote(Note note) {
        mapper.save(note);
        return  note;
    }

    @Override
    public Note getNote(String id) {
        return mapper.load(Note.class, id);
    }
}
