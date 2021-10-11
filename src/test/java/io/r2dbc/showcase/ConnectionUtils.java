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
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Utility methods for database connectivity.
 *
 * @author Mark Paluch
 */
class ConnectionUtils {

	/**
	 * Create {@link io.r2dbc.spi.ConnectionFactory} to connect a database running in {@link MSSQLServerContainer}.
	 *
	 * @param container
	 * @return
	 */
	static MssqlConnectionFactory createConnectionFactory(MSSQLServerContainer<?> container) {

		ConnectionFactoryOptions options = ConnectionFactoryOptions
				.parse(String.format("r2dbc:mssql://%s:%d", container.getHost(), container.getFirstMappedPort())) //
				.mutate() //
				.option(ConnectionFactoryOptions.USER, container.getUsername())
				.option(ConnectionFactoryOptions.PASSWORD, container.getPassword()).build();

		return (MssqlConnectionFactory) ConnectionFactories.get(options);
	}

	/**
	 * Create {@link io.r2dbc.spi.ConnectionFactory} to connect a database running in {@link PostgreSQLContainer}.
	 *
	 * @param container
	 * @return
	 */
	static PostgresqlConnectionFactory createConnectionFactory(PostgreSQLContainer<?> container) {

		ConnectionFactoryOptions options = ConnectionFactoryOptions
				.parse(String.format("r2dbc:postgres://%s:%d/%s", container.getHost(), container.getFirstMappedPort(),
						container.getDatabaseName()))
				.mutate() //
				.option(ConnectionFactoryOptions.USER, container.getUsername())
				.option(ConnectionFactoryOptions.PASSWORD, container.getPassword()).build();

		return (PostgresqlConnectionFactory) ConnectionFactories.get(options);
	}

	static JdbcTemplate createJdbcTemplate(JdbcDatabaseContainer<?> container) {

		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(container.getDriverClassName());
		dataSource.setUrl(container.getJdbcUrl());
		dataSource.setUsername(container.getUsername());
		dataSource.setPassword(container.getPassword());

		return new JdbcTemplate(dataSource);
	}
}
