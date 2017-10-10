package com.welflex.cassandra;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class LocalCassandraServer {
  public static void main(String args[]) throws Exception {
    int port = 9042;
    String keyspace = "users";
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", 30000);

    Cluster cluster = Cluster.builder().addContactPoints("127.0.0.1").withPort(port).build();

    Session sesion = cluster.connect();
    sesion.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH durable_writes=true "
      + " and replication={'class' : 'SimpleStrategy', 'replication_factor':1};");
    sesion.execute("USE " + keyspace);
    System.out.println("Cassandra Started...");
    System.in.read();
    System.exit(0);
  }
}
