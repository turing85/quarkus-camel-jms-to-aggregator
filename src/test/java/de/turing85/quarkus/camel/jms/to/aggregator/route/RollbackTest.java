package de.turing85.quarkus.camel.jms.to.aggregator.route;

import java.time.Duration;

import jakarta.jms.JMSConsumer;
import jakarta.jms.JMSContext;
import jakarta.jms.JMSProducer;
import jakarta.jms.Message;
import jakarta.jms.TextMessage;

import io.quarkus.test.junit.QuarkusTest;
import lombok.Getter;
import org.apache.camel.builder.AdviceWith;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.camel.builder.Builder.header;

@QuarkusTest
@Getter
class RollbackTest extends TestBase {
  @Test
  void rollbackTest() throws Exception {
    // GIVEN
    AdviceWith.adviceWith(camelContext, MyRoute.ROUTE_ID,
        route -> route.weaveAddLast().id("thrower").filter(header("isLast").isEqualTo(true))
            .throwException(new RuntimeException("Rollback test")).end());
    final String refId = "1337";

    // WHEN
    try (final JMSContext context = connectionFactory().createContext()) {
      final JMSProducer producer = context.createProducer();
      producer.send(context.createQueue(TestBase.IN_QUEUE), createTextMessage(context, "1", refId));
      final TextMessage finalMessage = createTextMessage(context, "2", refId);
      finalMessage.setBooleanProperty("isLast", true);
      producer.send(context.createQueue(TestBase.IN_QUEUE), finalMessage);
    }

    // THEN
    Thread.sleep(Duration.ofSeconds(5).toMillis());
    assertEntriesInCamelAggregationForRefIdEquals(refId, 1);

    try (final JMSContext context = connectionFactory().createContext();
        final JMSConsumer inConsumer = context.createConsumer(context.createQueue(TestBase.IN_QUEUE));
        final JMSConsumer outConsumer = context.createConsumer(context.createQueue(TestBase.OUT_QUEUE))) {

      final Message inMessage = inConsumer.receive(Duration.ofSeconds(1).toMillis());
      Assertions.assertNotNull(inMessage);
      Assertions.assertEquals("2", inMessage.getBody(String.class));
      Assertions.assertEquals(refId, inMessage.getStringProperty("refId"));
      Assertions.assertTrue(inMessage.getBooleanProperty("isLast"));

      Assertions.assertNull(inConsumer.receive(Duration.ofSeconds(1).toMillis()));

      Assertions.assertNull(outConsumer.receive(Duration.ofSeconds(1).toMillis()));
    }
  }
}
