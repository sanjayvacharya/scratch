package com.welflex.users.client;

import com.welflex.users.generated.User;
import com.welflex.users.generated.UserByUserNameRequest;
import com.welflex.users.generated.UserCreateRequest;
import com.welflex.users.generated.UserCreateResponse;
import com.welflex.users.generated.UsersGrpc;
import com.welflex.users.generated.UsersGrpc.UsersBlockingStub;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

public class UsersClientImpl {
  public User getUserByUserName(String userName) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6565).usePlaintext(true)
        .build();
    UsersBlockingStub blockingStub = UsersGrpc.newBlockingStub(channel);
    try {
      return blockingStub
          .getByUserName(UserByUserNameRequest.newBuilder().setUsername(userName).build());

    }
    catch (StatusRuntimeException e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
  }

  public UserCreateResponse createUser(String userName, String email, String firstName, String lastName) {
    ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 6565).usePlaintext(true)
        .build();
    UsersBlockingStub blockingStub = UsersGrpc.newBlockingStub(channel);
    try {
      return blockingStub.create(UserCreateRequest.newBuilder().setUsername(userName).setEmail(email)
          .setFirstName(firstName).setLastName(lastName).build());

    }
    catch (StatusRuntimeException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  public static void main(String args[]) {
    UsersClientImpl impl = new UsersClientImpl();
    System.out.println("Create:" + impl.createUser("foo", "foo@bar.com", "Foo", "Bar"));
    System.out.println(impl.getUserByUserName("foo"));
  }
}
