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

package com.ververica.enrich.function;

import com.ververica.enrich.record.Customer;
import com.ververica.enrich.record.EnrichedOrder;
import com.ververica.enrich.record.Order;
import com.ververica.enrich.util.DatabaseClient;
import com.ververica.enrich.util.DatabaseClientParameter;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

public class EnrichOrderSyncFunction extends RichMapFunction<Order, EnrichedOrder> {

    protected transient DatabaseClient dbClient;
    protected DatabaseClientParameter dbClientParameter;

    public EnrichOrderSyncFunction(DatabaseClientParameter dbClientParameter) {
        this.dbClientParameter = dbClientParameter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dbClient = DatabaseClient.createDatabaseClient(dbClientParameter);
        dbClient.connect();
        dbClient.prepareFetchCustomerById();
    }

    @Override
    public EnrichedOrder map(Order order) throws Exception {
        Customer customer = dbClient.fetchCustomerById(order.getCustomerId());
        return new EnrichedOrder(order, customer);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if ( dbClient != null ) dbClient.close();
    }
}
