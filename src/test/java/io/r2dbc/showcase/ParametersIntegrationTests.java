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
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.postgresql.codec.PostgresTypes;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Parameters;
import reactor.core.publisher.Flux;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Examples using {@link Parameters parameter definitions}.
 *
 * @author Mark Paluch
 */
class ParametersIntegrationTests {

	private final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:13.4")
			.withReuse(true);

	@BeforeEach
	void setUp() {

		container.start();

		JdbcTemplate template = ConnectionUtils.createJdbcTemplate(container);

		try {
			template.execute("CREATE TYPE my_enum AS ENUM ('HELLO', 'WORLD')");
		}
		catch (DataAccessException e) {
			// ignore duplicate types
		}

		template.execute("""
				DROP TABLE IF EXISTS enum_test;
				CREATE TABLE enum_test (the_value my_enum);
				""");
	}

	private Flux<?> showcase(PostgresqlConnection conn) {

		return PostgresTypes.from(conn).lookupType("my_enum").flatMapMany(type -> {

			return conn.createStatement("INSERT INTO enum_test VALUES($1)")
					.bind("$1", Parameters.in(type, "HELLO"))
					.execute()
					.concatMap(PostgresqlResult::getRowsUpdated);

		});
	}

	@Test
	void shouldApplyEnumType() {

		PostgresqlConnectionFactory connectionFactory = ConnectionUtils.createConnectionFactory(container);

		Flux.usingWhen(connectionFactory.create(), this::showcase, Connection::close)
				.blockLast();
	}

}
