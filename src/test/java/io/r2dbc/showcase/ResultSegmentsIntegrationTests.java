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

import io.r2dbc.mssql.MssqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.R2dbcNonTransientException;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.jdbc.core.JdbcTemplate;

import org.testcontainers.containers.MSSQLServerContainer;

/**
 * Examples for consuming result {@link io.r2dbc.spi.Result.Segment segments}.
 *
 * @author Mark Paluch
 */
class ResultSegmentsIntegrationTests {

	private final MSSQLServerContainer<?> container = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2017-CU12")
			.withReuse(true);

	@BeforeEach
	void setUp() {

		container.start();

		JdbcTemplate template = ConnectionUtils.createJdbcTemplate(container);

		template.execute("DROP PROCEDURE IF EXISTS proc_with_segments");

		template.execute("""
				CREATE PROCEDURE proc_with_segments
				    @TheName nvarchar(50),
				    @Greeting nvarchar(255) OUTPUT
				AS

				    SET @Greeting = CONCAT('Hello ', @TheName);
				    SELECT 1;
				    RAISERROR ('Booh!', 16, 1)
				""");
	}

	@Test
	void consumeProcedureAsSegments() {

		MssqlConnectionFactory connectionFactory = ConnectionUtils.createConnectionFactory(container);

		Flux.usingWhen(connectionFactory.create(), conn -> {

			return conn.createStatement("EXEC proc_with_segments @P0, @Greeting OUTPUT")
					.bind("@P0", "Walter")
					.bind("@Greeting", Parameters.out(R2dbcType.VARCHAR))
					.execute()
					.flatMap(it -> it.flatMap(segment -> {

						if (segment instanceof Result.UpdateCount) {
							return Mono.just("Count: " + ((Result.UpdateCount) segment)
									.value());
						}

						if (segment instanceof Result.OutSegment) {
							return Mono.just("Out: " + ((Result.OutSegment) segment)
									.outParameters().get("@Greeting"));
						}

						if (segment instanceof Result.RowSegment) {
							return Mono.just("Row: " + ((Result.RowSegment) segment)
									.row().get(0));
						}

						if (segment instanceof Result.Message) {
							return Mono.just("Message: " + ((Result.Message) segment)
									.message());
						}

						return Mono.empty();
					}));

		}, Connection::close)
				.as(StepVerifier::create)
				.expectNext("Count: 1")
				.expectNext("Row: 1")
				.expectNext("Count: 1")
				.expectNext("Message: Booh!")
				.expectNext("Out: Hello Walter")
				.verifyComplete();
	}

	@Test
	void consumeProcedureWithMap() {

		MssqlConnectionFactory connectionFactory = ConnectionUtils.createConnectionFactory(container);

		Flux.usingWhen(connectionFactory.create(), conn -> {

			return conn.createStatement("EXEC proc_with_segments @P0, @Greeting OUTPUT")
					.bind("@P0", "Walter")
					.bind("@Greeting", Parameters.out(R2dbcType.VARCHAR))
					.execute()
					.flatMap(it -> it.map(r -> r.getClass().getSimpleName()));

		}, Connection::close)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyError(R2dbcNonTransientException.class);
	}

	@Test
	void consumeProcedureWithFilterAndMap() {

		MssqlConnectionFactory connectionFactory = ConnectionUtils.createConnectionFactory(container);

		Flux.usingWhen(connectionFactory.create(), conn -> {

			return conn.createStatement("EXEC proc_with_segments @P0, @Greeting OUTPUT")
					.bind("@P0", "Walter")
					.bind("@Greeting", Parameters.out(R2dbcType.VARCHAR))
					.execute()
					.flatMap(it -> it
							.filter(segment -> segment instanceof Result.RowSegment)
							.map(r -> r.getClass().getSimpleName()));

		}, Connection::close)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();
	}

}
