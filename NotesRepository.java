package com.welflex.aws.dynamodb.repository;

import com.welflex.aws.dynamodb.model.Note;

public interface NotesRepository {
    Note saveNote(Note note);

    Note getNote(String id);
}
