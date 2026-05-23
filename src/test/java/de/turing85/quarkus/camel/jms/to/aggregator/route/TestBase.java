package de.turing85.quarkus.camel.jms.to.aggregator.route;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;

import jakarta.inject.Inject;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;
import jakarta.ws.rs.core.Response;

import io.agroal.api.AgroalDataSource;
import io.quarkus.artemis.test.ArtemisTestResource;
import io.quarkus.logging.Log;
import io.quarkus.test.common.WithTestResource;
import io.restassured.RestAssured;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.camel.quarkus.test.CamelQuarkusTestSupport;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

@WithTestResource(ArtemisTestResource.class)
@RequiredArgsConstructor
@Getter(AccessLevel.PROTECTED)
class TestBase extends CamelQuarkusTestSupport {
  public static final String IN_QUEUE = "in::in";
  public static final String OUT_QUEUE = "out::out";

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  ConnectionFactory connectionFactory;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  AgroalDataSource dataSource;

  @BeforeEach
  void teardown() {
    cleanupQueues(List.of(IN_QUEUE, OUT_QUEUE));
    cleanupTables(List.of("camel_aggregation", "camel_aggregation_completed"));
    awaitHealthUp(Duration.ofSeconds(10));
  }

  private void cleanupQueues(final List<String> queues) {
    try (final JMSContext context = connectionFactory().createContext()) {
      for (final String queue : queues) {
        int count = -1;
        final JMSConsumer consumer = context.createConsumer(context.createQueue(queue));
        Message message;
        do {
          ++count;
          message = consumer.receive(Duration.ofSeconds(1).toMillis());
        } while (message != null);
        Log.infof("Deleted %d message from queue %s", count, queue);
      }
    }
  }

  private void cleanupTables(List<String> tables) {
    try (final Connection connection = dataSource().getConnection()) {
      for (final String table : tables) {
        final String sql = "DELETE FROM %s".formatted(table);
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
          Log.infof("Deleted %d rows form table %s", statement.executeUpdate(), table);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected static TextMessage createTextMessage(JMSContext context, String text, String refId)
      throws JMSException {
    TextMessage textMessage = context.createTextMessage();
    textMessage.setText(text);
    textMessage.setStringProperty("refId", refId);
    return textMessage;
  }

  protected static void awaitHealthDown(Duration timeout) {
    // @formatter:off
    Awaitility.await()
        .atMost(timeout)
        .untilAsserted(() -> RestAssured
            .when().get("/q/health")
            .then().statusCode(Response.Status.SERVICE_UNAVAILABLE.getStatusCode()));
    // @formatter:on
  }

  protected static void awaitHealthUp(Duration timeout) {
    // @formatter:off
    Awaitility.await()
        .atMost(timeout)
        .untilAsserted(() -> RestAssured
            .when().get("/q/health")
            .then().statusCode(Response.Status.OK.getStatusCode()));
    // @formatter:on
  }

  protected void assertEntriesInCamelAggregationCompleted(int expectedCount) {
    final String sql = "SELECT COUNT(*) AS count FROM camel_aggregation_completed";

    try (final Connection connection = dataSource().getConnection();
        final PreparedStatement statement = connection.prepareStatement(sql)) {

      final int count;
      try (final ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          count = resultSet.getInt("count");
        } else {
          count = 0;
        }
      }
      Assertions.assertEquals(expectedCount, count);
    } catch (SQLException e) {
      throw new RuntimeException("Database query failed", e);
    }
  }

  protected void assertEntriesInCamelAggregationForRefId(String refId, int expectedCount) {
    final String sql = "SELECT COUNT(*) AS count FROM camel_aggregation WHERE id = ?";

    try (final Connection connection = dataSource().getConnection();
        final PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, refId);

      final int count;
      try (final ResultSet resultSet = statement.executeQuery()) {
        if (resultSet.next()) {
          count = resultSet.getInt("count");
        } else {
          count = 0;
        }
      }
      Assertions.assertEquals(expectedCount, count);
    } catch (SQLException e) {
      throw new RuntimeException("Database query failed", e);
    }
  }
}
