package de.turing85.quarkus.camel.jms.to.aggregator.route;

import java.time.Duration;

import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

import io.quarkus.test.junit.QuarkusTest;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
@Getter
class MyRouteTest extends TestBase {

  @Test
  void goodTest() throws Exception {
    final String refId = "666";

    // WHEN
    try (final JMSContext context = connectionFactory().createContext()) {
      JMSProducer producer = context.createProducer();
      producer.send(context.createQueue("in::in"), createTextMessage(context, "1", refId));
      TextMessage finalMessage = createTextMessage(context, "2", refId);
      finalMessage.setBooleanProperty("isLast", true);
      producer.send(context.createQueue("in::in"), finalMessage);
    }

    // THEN
    Thread.sleep(Duration.ofSeconds(5).toMillis());
    assertEntriesInCamelAggregationForRefIdEquals(refId, 0);

    try (final JMSContext context = connectionFactory().createContext();
        final JMSConsumer inConsumer = context.createConsumer(context.createQueue("in::in"));
        final JMSConsumer outConsumer = context.createConsumer(context.createQueue("out::out"))) {

      Assertions.assertNull(inConsumer.receive(Duration.ofSeconds(1).toMillis()));

      final Message outMessage = outConsumer.receive(Duration.ofSeconds(1).toMillis());
      Assertions.assertNotNull(outMessage);
      Assertions.assertEquals("3", outMessage.getBody(String.class));
      Assertions.assertEquals(refId, outMessage.getStringProperty("refId"));
      Assertions.assertTrue(outMessage.getBooleanProperty("isLast"));
    }
  }
}
