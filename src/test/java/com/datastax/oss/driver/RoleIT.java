package com.datastax.oss.driver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class RoleIT {

  // Set up and start a CCM process that will be running for the duration of the whole test class.
  @ClassRule
  public static CustomCcmRule ccmRule =
      CustomCcmRule.builder()
          .withCassandraConfiguration("authenticator", "PasswordAuthenticator")
          .withCassandraConfiguration("authorizer", "CassandraAuthorizer")
          .withJvmArgs("-Dcassandra.superuser_setup_delay_ms=0")
          .build();

  @BeforeClass
  public static void createRole() {
    // Connect with the default admin account to create a role a some test data
    try (CqlSession session =
        SessionUtils.newSession(
            ccmRule,
            "protocol.auth-provider.class = com.datastax.oss.driver.api.core.auth.PlainTextAuthProvider",
            "protocol.auth-provider.username = cassandra",
            "protocol.auth-provider.password = cassandra")) {

      session.execute(
          "CREATE KEYSPACE test1 "
              + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
      session.execute("CREATE TABLE test1.foo (k int PRIMARY KEY)");
      session.execute("INSERT INTO test1.foo (k) VALUES (1)");

      session.execute(
          "CREATE KEYSPACE test2 "
              + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
      session.execute("CREATE TABLE test2.foo (k int PRIMARY KEY)");
      session.execute("INSERT INTO test2.foo (k) VALUES (1)");

      // Note: authorization doesn't work on the `system` keyspace yet
      session.execute("CREATE ROLE user WITH PASSWORD='user' AND login = true");
      session.execute("GRANT ALL ON KEYSPACE test1 TO user");
    }
  }

  @Test
  public void should_connect_using_new_role() {
    try (CqlSession session =
        SessionUtils.newSession(
            ccmRule,
            "protocol.auth-provider.class = com.datastax.oss.driver.api.core.auth.PlainTextAuthProvider",
            "protocol.auth-provider.username = user",
            "protocol.auth-provider.password = user")) {

      // Authorized on test1
      Row row = session.execute("SELECT * FROM test1.foo WHERE k = 1").one();
      assertThat(row.getInt(0)).isEqualTo(1);

      // Not authorized on test2
      assertThatThrownBy(() -> session.execute("SELECT * FROM test2.foo WHERE k = 1"))
          .isInstanceOf(UnauthorizedException.class);
    }
  }
}
