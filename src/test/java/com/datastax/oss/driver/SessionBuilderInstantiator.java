package com.datastax.oss.driver;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;

/**
 * Initializes the session builder for utilities such as {@link SessionRule} and {@link
 * SessionUtils#newSession(CassandraResourceRule, String...)}.
 *
 * @see "pom.xml"
 */
public class SessionBuilderInstantiator {

  public static SessionBuilder<?, ?> builder() {
    // Change this if you have a custom session builder
    return CqlSession.builder();
  }

  public static String configPath() {
    return "datastax-java-driver";
  }
}
