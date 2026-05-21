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

import io.agroal.api.AgroalDataSource;
import io.quarkus.logging.Log;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.camel.CamelContext;
import org.apache.camel.quarkus.test.CamelQuarkusTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;

@Getter(AccessLevel.PROTECTED)
class TestBase extends CamelQuarkusTestSupport {
  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  ConnectionFactory connectionFactory;

  @Inject
  CamelContext camelContext;

  @Inject
  @SuppressWarnings("CdiInjectionPointsInspection")
  AgroalDataSource dataSource;

  @AfterEach
  void teardown() throws Exception {
    cleanupQueues(List.of("in::in", "out::out"));
    cleanupTables(List.of("camel_aggregation", "camel_aggregation_completed"));
    camelContext.getRouteController().reloadAllRoutes();
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

  protected void assertEntriesInCamelAggregationForRefIdEquals(String refId, int expectedCount) {
    final String sql = "SELECT * FROM camel_aggregation WHERE id = ?";

    try (final Connection connection = dataSource().getConnection();
        final PreparedStatement statement = connection.prepareStatement(sql)) {

      statement.setString(1, refId);

      int count = 0;
      try (final ResultSet resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          ++count;
        }
      }

      Assertions.assertEquals(expectedCount, count);
    } catch (SQLException e) {
      // Handle or log your database exceptions cleanly here
      throw new RuntimeException("Database query failed", e);
    }
  }
}
