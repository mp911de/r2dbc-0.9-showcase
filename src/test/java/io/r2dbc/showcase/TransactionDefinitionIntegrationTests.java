/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.r2dbc.showcase;

import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresTransactionDefinition;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Option;
import io.r2dbc.spi.TransactionDefinition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Flux;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * Examples using {@link Connection#beginTransaction(TransactionDefinition) extensible transaction definitions}.
 *
 * @author Mark Paluch
 */
class TransactionDefinitionIntegrationTests {

	private final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:13.4")
			.withReuse(true);

	@BeforeEach
	void setUp() {
		container.start();
	}

	@Test
	void shouldStartTransactionUsingDefinition() {

		PostgresqlConnectionFactory connectionFactory = ConnectionUtils.createConnectionFactory(container);

		Flux.usingWhen(connectionFactory.create(), conn -> {

			// PostgresTransactionDefinition
			return Flux.empty();







		}, Connection::close)
				.blockLast();
	}

	@Test
	void shouldStartTransactionUsingIsolationLevel() {

		PostgresqlConnectionFactory connectionFactory = ConnectionUtils.createConnectionFactory(container);

		Flux.usingWhen(connectionFactory.create(), conn -> {

			// IsolationLevel.SERIALIZABLE




			return Flux.empty();




		}, Connection::close)
				.blockLast();
	}

	@Test
	void shouldStartTransactionAsFramework() {

		PostgresqlConnectionFactory connectionFactory = ConnectionUtils.createConnectionFactory(container);

		Flux.usingWhen(connectionFactory.create(), conn -> {

			return conn.beginTransaction(new TransactionDefinition() {
				@Override
				public <T> T getAttribute(Option<T> option) {

					if (option.equals(TransactionDefinition.ISOLATION_LEVEL)) {
						return (T) IsolationLevel.SERIALIZABLE;
					}

					if (option.equals(PostgresTransactionDefinition.READ_ONLY)) {
						return (T) Boolean.TRUE;
					}

					return null;
				}
			});
		}, Connection::close)
				.blockLast();
	}

}
