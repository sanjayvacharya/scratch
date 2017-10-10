package com.welflex.users.repository;

import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import com.welflex.users.model.UserByUserName;

@Repository
public interface UsersByUserNameRepository extends CrudRepository<UserByUserName, String>{
}
