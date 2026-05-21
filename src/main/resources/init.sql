CREATE TABLE camel_aggregation
(
    id       VARCHAR(255) NOT NULL,
    exchange BYTEA        NOT NULL,
    version  BIGINT       NOT NULL,
    CONSTRAINT camel_aggregation_pk PRIMARY KEY (id)
);

-- Table for completed aggregations (used for recovery)
CREATE TABLE camel_aggregation_completed
(
    id       VARCHAR(255) NOT NULL,
    exchange BYTEA        NOT NULL,
    version  BIGINT       NOT NULL,
    CONSTRAINT camel_aggregation_completed_pk PRIMARY KEY (id)
);