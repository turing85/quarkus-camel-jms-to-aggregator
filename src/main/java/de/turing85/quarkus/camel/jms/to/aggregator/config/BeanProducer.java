package de.turing85.quarkus.camel.jms.to.aggregator.config;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;

import io.agroal.api.AgroalDataSource;
import org.apache.camel.processor.aggregate.jdbc.JdbcAggregationRepository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.jta.JtaTransactionManager;

public class BeanProducer {
  @Produces
  @Singleton
  PlatformTransactionManager transactionManager(UserTransaction userTransaction,
      @SuppressWarnings("CdiInjectionPointsInspection") TransactionManager transactionManager) {
    return new JtaTransactionManager(userTransaction, transactionManager);
  }

  @Produces
  @Singleton
  JdbcAggregationRepository jdbcAggregationRepository(PlatformTransactionManager transactionManager,
      @SuppressWarnings("CdiInjectionPointsInspection") AgroalDataSource dataSource) {
    return new JdbcAggregationRepository(transactionManager, "camel_aggregation", dataSource);
  }
}
