package de.turing85.quarkus.camel.jms.to.aggregator.route;

import java.util.concurrent.ExecutorService;

import jakarta.inject.Singleton;
import jakarta.jms.ConnectionFactory;

import de.turing85.quarkus.camel.jms.to.aggregator.config.SumAggregationStrategy;
import io.quarkus.logging.Log;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.processor.aggregate.jdbc.JdbcAggregationRepository;
import org.apache.camel.util.concurrent.SynchronousExecutorService;
import org.springframework.transaction.PlatformTransactionManager;

import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.direct;
import static org.apache.camel.builder.endpoint.StaticEndpointBuilders.jms;

@Singleton
@RequiredArgsConstructor
@Getter(AccessLevel.PRIVATE)
public class MyRoute extends RouteBuilder {
  public static final String IN_ROUTE_ID = "in-route";
  public static final String OUT_ROUTE_ID = "out-route";

  private final ConnectionFactory connectionFactory;
  private final PlatformTransactionManager transactionManager;
  private final JdbcAggregationRepository aggregationRepository;
  private final ExecutorService executor;

  @Override
  public void configure() {
    // @formatter:off
    onException(Exception.class)
        .process(exchange -> {
          if (!getContext().isSuspended() && !getContext().isSuspending()) {
            Log.warn("Shutting down due to",
                exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class));
            executor.execute(getContext()::suspend);
          }
        })
        .handled(true)
        .markRollbackOnly();

    from(
        jms("queue:in")
            .connectionFactory(connectionFactory())
            .advanced()
                .transactionManager(transactionManager()))
        .routeId(IN_ROUTE_ID)
        .log(LoggingLevel.DEBUG, "Received: ${body}, refId: ${headers.refId}, isLast: ${headers.isLast}")
        .aggregate(header("refId"), new SumAggregationStrategy())
                .aggregationRepository(aggregationRepository())
                .completionPredicate(header("isLast").isEqualTo(true))
                .executorService(new SynchronousExecutorService())
            .convertBodyTo(String.class)
            .to(direct(OUT_ROUTE_ID))
        .end();

    from(direct(OUT_ROUTE_ID))
        .routeId(OUT_ROUTE_ID)
        .transacted()
        .to(jms("queue:out")
            .connectionFactory(connectionFactory())
            .advanced()
                .transactionManager(transactionManager()));
    // @formatter:on
  }
}
