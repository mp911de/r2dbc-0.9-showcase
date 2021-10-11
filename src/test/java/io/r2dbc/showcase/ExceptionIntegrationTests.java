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

import static org.junit.jupiter.api.Assertions.*;

import io.r2dbc.mssql.MssqlConnectionFactory;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcBadGrammarException;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MSSQLServerContainer;


/**
 * Examples {@link io.r2dbc.spi.R2dbcException exceptions}.
 *
 * @author Mark Paluch
 */
class ExceptionIntegrationTests {

	private final MSSQLServerContainer<?> container = new MSSQLServerContainer<>("mcr.microsoft.com/mssql/server:2017-CU12")
			.withReuse(true);

	@BeforeEach
	void setUp() {
		container.start();
	}

	@Test
	void shouldProduceException() {

		MssqlConnectionFactory connectionFactory = ConnectionUtils.createConnectionFactory(container);

		Flux.usingWhen(connectionFactory.create(), conn -> {










			

			return Flux.empty();

		}, Connection::close)
				.as(StepVerifier::create)
				.consumeErrorWith(e -> {


					// assertEquals("…", ex);

				}).verify();
	}

}
