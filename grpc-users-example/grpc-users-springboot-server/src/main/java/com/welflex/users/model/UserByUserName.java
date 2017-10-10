package com.welflex.users.model;

import org.pojomatic.Pojomatic;
import org.pojomatic.annotations.AutoProperty;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.cassandra.mapping.Column;
import org.springframework.data.cassandra.mapping.PrimaryKey;
import org.springframework.data.cassandra.mapping.Table;

@Table("users_by_username")
@Persistent
@AutoProperty
public class UserByUserName {
  @PrimaryKey
  private String userName;

  @Column("email")
  private String email;

  @Column("first_name")
  private String firstName;

  @Column("last_name")
  private String lastName;

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getUsername() {
    return userName;
  }

  public void setUsername(String username) {
    this.userName = username;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }
  
  @Override
  public int hashCode() {
    return Pojomatic.hashCode(this);
  }
  
  @Override
  public String toString() {
    return Pojomatic.toString(this);
  }
  
  @Override
  public boolean equals(Object other) {
    return Pojomatic.equals(this, other);
  }
}
