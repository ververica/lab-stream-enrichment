/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.enrich.util;

import com.ververica.enrich.record.Customer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DatabaseClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClient.class);

    /**
     * Use HikariCP to manage database connection pooling.
     * See: <a href="https://github.com/brettwooldridge/HikariCP">HikariCP</a>
     */
    private static HikariDataSource dataSource;
    private static HikariConfig hikariConfig;

    public static void logConnectionPoolStats(HikariDataSource dataSource) {
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        executorService.scheduleAtFixedRate(() -> {
            System.out.print("Active: " + dataSource.getHikariPoolMXBean().getActiveConnections() + ", ");
            System.out.print("Idle: " + dataSource.getHikariPoolMXBean().getIdleConnections() + ", ");
            System.out.print("Total: " + dataSource.getHikariPoolMXBean().getTotalConnections() + ", ");
            System.out.println("Threads Awaiting: " + dataSource.getHikariPoolMXBean().getThreadsAwaitingConnection());
        }, 0, 10, TimeUnit.SECONDS); // Adjust the period according to your needs

    }
    
    // Connection, Statement, ResultSet are not thread safe!
    private static final ThreadLocal<Connection> connThreadLocal = new ThreadLocal<>();
    private static final ThreadLocal<PreparedStatement> stmtThreadLocal = new ThreadLocal<>();

    public synchronized static DatabaseClient createDatabaseClient(DatabaseClientParameter dbClientParameter) {
        return new DatabaseClient(dbClientParameter);
    }

    protected DatabaseClient(DatabaseClientParameter dbClientParameter) {
        if ( dataSource == null ) {
            // Configure HikariCP
            hikariConfig = new HikariConfig();
            hikariConfig.setJdbcUrl("jdbc:mysql://"
                    + dbClientParameter.getHost()
                    + ":"
                    + dbClientParameter.getPort()
                    + "/"
                    + dbClientParameter.getDatabase());
            hikariConfig.setUsername(dbClientParameter.getUsername());
            hikariConfig.setPassword(dbClientParameter.getPassword());

            // Optional: Configure additional settings
            // Max number of connections in the pool.
            // When using per record sync look up enrichment, make sure this is greater or equal than the parallelism of
            // the task that connects to database
            hikariConfig.setMaximumPoolSize(12);
            hikariConfig.setMinimumIdle(2); // Min number of idle connections in the pool
            hikariConfig.setIdleTimeout(30000); // 30 seconds
            hikariConfig.setConnectionTimeout(3000); // 3 seconds, set this to a small value to quickly detect insufficient pool size
            hikariConfig.setPoolName("HikariCP");

            // Enable JMX
            hikariConfig.setRegisterMbeans(true);

            dataSource = new HikariDataSource(hikariConfig);

            // logConnectionPoolStats(dataSource);
        }
    }

    public void connect() {
        if (connThreadLocal.get() == null) {
            try {
                connThreadLocal.set( dataSource.getConnection() );
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void prepareFetchCustomerById() throws SQLException {
        String sql = "select * from customers where id = ?";
        if ( stmtThreadLocal.get() == null ) {
            stmtThreadLocal.set( connThreadLocal.get().prepareStatement(sql) );
        }
    }

    public Customer fetchCustomerById(int id) throws SQLException {
        PreparedStatement stmt = stmtThreadLocal.get();
        stmt.setInt(1, id);
        ResultSet rs = stmt.executeQuery();
        List<Customer> customers = parseResultSet(rs);
        return customers.stream().findFirst().orElse(Customer.NOT_FOUND);
    }

    public List<Customer> fetchAllCustomers() throws SQLException {
        String sql = "select * from customers";
        Statement stmt = connThreadLocal.get().createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        return parseResultSet(rs);
    }

    public List<Customer> fetchCustomersByCondition(String condition) throws SQLException {
        String sql = String.format("select * from customers where %s;", condition);

        LOGGER.info("fetchCustomersByIdList() sql = " + sql);

        Statement stmt = connThreadLocal.get().createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        return parseResultSet(rs);
    }

    public List<Customer> fetchCustomersByIdList(List<Integer> idList) throws SQLException {
        String sql = "select * from customers where id in (";
        sql += idList.stream().map(String::valueOf).collect(Collectors.joining(","));
        sql += ");";

        LOGGER.info("fetchCustomersByIdList() sql = " + sql);

        Statement stmt = connThreadLocal.get().createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        return parseResultSet(rs);
    }

    private List<Customer> parseResultSet(ResultSet rs) {
        List<Customer> customers = new ArrayList<>();

        // try rs and close it
        try (rs) {
            while (rs.next()) {
                customers.add(
                        Customer.builder()
                                .id(rs.getInt("id"))
                                .name(rs.getString("name"))
                                .country(rs.getString("country"))
                                .zipcode(rs.getString("zipcode"))
                                .status(rs.getString("status"))
                                .updatedAt(rs.getString("updatedAt"))
                                .build()
                );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return customers;
    }


    public void close() {
        // Close resources
        try {
            if (stmtThreadLocal.get() != null) stmtThreadLocal.get().close();
            if (connThreadLocal.get() != null) connThreadLocal.get().close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        DatabaseClientParameter dbClientParameter = DatabaseClientParameter.builder()
                .host("localhost")
                .port(3306)
                .database("crmdb")
                .username("root")
                .password("password")
                .build();

        DatabaseClient client = new DatabaseClient(dbClientParameter);
        client.connect();
        client.fetchAllCustomers().forEach(
                System.out::println
        );
        client.close();

    }

}
