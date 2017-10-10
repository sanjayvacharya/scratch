package com.welflex.users.server;

import javax.inject.Inject;

import org.lognet.springboot.grpc.GRpcService;
import org.springframework.data.cassandra.core.CassandraTemplate;

import com.welflex.users.generated.User;
import com.welflex.users.generated.UserCreateResponse;
import com.welflex.users.generated.UsersGrpc;
import com.welflex.users.model.UserByEmail;
import com.welflex.users.model.UserByUserName;
import com.welflex.users.repository.UsersByEmailRepository;
import com.welflex.users.repository.UsersByUserNameRepository;

@GRpcService
public class UsersImpl extends UsersGrpc.UsersImplBase {
  @Inject
  private UsersByEmailRepository usersByEmailRepository;

  @Inject
  private UsersByUserNameRepository usersByUserNameRepository;

  @Inject
  private CassandraTemplate cassandraTemplate;

  /**
   */
  public void create(com.welflex.users.generated.UserCreateRequest request,
    io.grpc.stub.StreamObserver<com.welflex.users.generated.UserCreateResponse> responseObserver) {
    UserByEmail userByEmail = new UserByEmail();

    userByEmail.setEmail(request.getEmail());
    userByEmail.setFirstName(request.getFirstName());
    userByEmail.setLastName(request.getLastName());
    userByEmail.setUsername(request.getUsername());

    UserByUserName userByUserName = new UserByUserName();

    userByUserName.setEmail(request.getEmail());
    userByUserName.setFirstName(request.getFirstName());
    userByUserName.setLastName(request.getLastName());
    userByUserName.setUsername(request.getUsername());

    try {

      cassandraTemplate.batchOps().insert(userByEmail, userByUserName).execute();

      responseObserver.onNext(UserCreateResponse.newBuilder().setResult("User Created").build());
      responseObserver.onCompleted();   
    }
    catch (RuntimeException e) {
      responseObserver.onError(e);
    }   
  }

  /**
   */
  public void getByEmail(com.welflex.users.generated.UserByEmailRequest request,
    io.grpc.stub.StreamObserver<com.welflex.users.generated.User> responseObserver) {
    UserByEmail userByEmail = usersByEmailRepository.findOne(request.getEmail());

    if (userByEmail != null) {

      User user = User.newBuilder().setEmail(userByEmail.getEmail())
          .setFirstName(userByEmail.getFirstName()).setLastName(userByEmail.getLastName())
          .setUsername(userByEmail.getUsername()).build();
      responseObserver.onNext(user);
    }

    responseObserver.onCompleted();
  }

  /**
   */
  public void getByUserName(com.welflex.users.generated.UserByUserNameRequest request,
    io.grpc.stub.StreamObserver<com.welflex.users.generated.User> responseObserver) {
    UserByUserName userByUserName = usersByUserNameRepository.findOne(request.getUsername());

    if (userByUserName != null) {

      User user = User.newBuilder().setEmail(userByUserName.getEmail())
          .setFirstName(userByUserName.getFirstName()).setLastName(userByUserName.getLastName())
          .setUsername(userByUserName.getUsername()).build();
      responseObserver.onNext(user);
    }
    responseObserver.onCompleted();
  }

  /**
   */
  public void stream(com.welflex.users.generated.UserStreamRequest request,
    io.grpc.stub.StreamObserver<com.welflex.users.generated.User> responseObserver) {

  }

}
