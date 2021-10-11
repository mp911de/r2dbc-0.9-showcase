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

import java.time.Duration;

import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

/**
 * Examples using {@link Connection#setStatementTimeout(Duration) statement timeout}.
 *
 * @author Mark Paluch
 */
class StatementTimeoutIntegrationTests {

	private final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:13.4")
			.withReuse(true);

	@BeforeEach
	void setUp() {
		container.start();
	}

	@Test
	void shouldRespectTimeout() {

		PostgresqlConnectionFactory connectionFactory = ConnectionUtils.createConnectionFactory(container);

		Flux.usingWhen(connectionFactory.create(), conn -> {

			return conn.setStatementTimeout(Duration.ofSeconds(1))
					.thenMany(conn.createStatement("SELECT pg_sleep(10)")
							.execute()
							.concatMap(PostgresqlResult::getRowsUpdated));
		}, Connection::close)
				.as(StepVerifier::create)
				.verifyError(R2dbcNonTransientResourceException.class);
	}

}
