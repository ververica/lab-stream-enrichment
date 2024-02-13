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

package com.ververica.enrich.solution;

import com.ververica.enrich.record.Customer;
import com.ververica.enrich.record.EnrichedOrder;
import com.ververica.enrich.record.Order;
import com.ververica.enrich.util.DatabaseClientAsync;
import com.ververica.enrich.util.DatabaseClientParameter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class EnrichOrderAsyncFunction extends RichAsyncFunction<Order, EnrichedOrder> {

    private transient DatabaseClientAsync dbClient;

    protected DatabaseClientParameter dbClientParameter;

    public EnrichOrderAsyncFunction(DatabaseClientParameter dbClientParameter) {
        this.dbClientParameter = dbClientParameter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dbClient = new DatabaseClientAsync(dbClientParameter);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if ( dbClient != null ) dbClient.close();
    }

    @Override
    public void asyncInvoke(Order order, ResultFuture<EnrichedOrder> resultFuture) throws Exception {
        final Future<Customer> result = dbClient.fetchCustomerByIdAsync(order.getCustomerId());

        CompletableFuture.supplyAsync(() -> {
            try {
                return result.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return Customer.FAILED;
            }
        }).thenAccept( customer -> resultFuture.complete(
                Collections.singleton(
                        new EnrichedOrder(order, customer)
                )
        ));
    }

    @Override
    public void timeout(Order order, ResultFuture<EnrichedOrder> resultFuture) throws Exception {
        resultFuture.complete(
                Collections.singleton(
                        new EnrichedOrder(order, Customer.TIME_OUT)
                )
        );
    }
}
