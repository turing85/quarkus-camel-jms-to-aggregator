package de.turing85.quarkus.camel.jms.to.aggregator.config;

import io.quarkus.logging.Log;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;

public class SumAggregationStrategy implements AggregationStrategy {
  @Override
  public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
    if (oldExchange != null) {
      final int sum =
          oldExchange.getIn().getBody(Integer.class) + newExchange.getIn().getBody(Integer.class);
      newExchange.getIn().setBody(sum);
    }
    Log.debugf("Sum is now: %d", newExchange.getIn().getBody(int.class));
    return newExchange;
  }
}
