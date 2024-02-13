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

import java.sql.SQLException;
import java.util.concurrent.*;

public class DatabaseClientAsync extends DatabaseClient {

    public DatabaseClientAsync(DatabaseClientParameter dbClientParameter) {
        super(dbClientParameter);
    }
    private static final ExecutorService pool =
            Executors.newFixedThreadPool(
                    5,
                    new ThreadFactory() {
                        private final ThreadFactory threadFactory = Executors.defaultThreadFactory();

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread thread = threadFactory.newThread(r);
                            thread.setName("database-client-" + thread.getName());
                            return thread;
                        }
                    });


    public CompletableFuture<Customer> fetchCustomerByIdAsync(int id) {
        return CompletableFuture.supplyAsync( () -> {
            try {
                /*
                 When called in the first time, a Database Connection will be created and stored in
                 ThreadLocal<Connection>.
                */
                connect();
                /*
                 When called in the first time, a PreparedStatement will be created and stored in
                 ThreadLocal<PreparedStatement>. We do want to share PreparedStatement in subsequence calls to
                 fetchCustomerById(), so we do not want to close them. They will stay during the entire life of this
                 streaming app
                */
                prepareFetchCustomerById();
                return fetchCustomerById(id);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                /*
                 We do NOT close here: this is a streaming app, we want to reuse the created Connection and
                 PreparedStatement. Ideally, the close should be called when the thread pool is shutdown.
                */
                // close();
            }
        }, pool);
    }
}
