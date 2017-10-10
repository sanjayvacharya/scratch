package com.welflex.users.repository;

import com.datastax.driver.core.ConsistencyLevel;
import com.welflex.users.model.UserByEmail;

public interface UsersByEmailRepository {
  void saveUsingCassandraTemplate(UserByEmail userByEmail);
  
  void saveUsingDatastax(String email, String userName, String firstName, String lastName, ConsistencyLevel level);
  
  UserByEmail findOne(String email);
  
}
