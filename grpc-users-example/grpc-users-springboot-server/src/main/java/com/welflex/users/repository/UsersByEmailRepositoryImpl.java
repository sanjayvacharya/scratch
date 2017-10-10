package com.welflex.users.repository;

import javax.inject.Inject;

import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.welflex.users.model.UserByEmail;

@Repository
public class UsersByEmailRepositoryImpl implements UsersByEmailRepository {

  @Inject
  private CassandraTemplate cassandraOperations;

  @Override
  public void saveUsingCassandraTemplate(UserByEmail userByEmail) {
    cassandraOperations.insert(userByEmail);

  }

  @Override
  public UserByEmail findOne(String email) {
    return cassandraOperations.selectOneById(UserByEmail.class, email);
  }

  @Override
  public void saveUsingDatastax(String email, String userName, String firstName, String lastName,
    ConsistencyLevel level) {
    Insert insert = QueryBuilder.insertInto("users_by_email");
    insert.setConsistencyLevel(level);
    insert.value("email", email);
    insert.value("username", userName);
    insert.value("first_name", firstName);
    insert.value("last_name", lastName);

    cassandraOperations.execute(insert);
  }

}
