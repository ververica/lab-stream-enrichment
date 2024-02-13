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
import com.ververica.enrich.util.DatabaseClient;
import com.ververica.enrich.util.DatabaseClientParameter;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;

public class LoadCustomPartitionThenEnrichOrderFunction extends RichMapFunction<Order, EnrichedOrder> {
    protected transient DatabaseClient dbClient;
    protected transient HashMap<Integer, Customer> customersInPartition;

    protected DatabaseClientParameter dbClientParameter;

    public LoadCustomPartitionThenEnrichOrderFunction(DatabaseClientParameter dbClientParameter) {
        this.dbClientParameter = dbClientParameter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        customersInPartition = new HashMap<>();

        dbClient = DatabaseClient.createDatabaseClient(dbClientParameter);
        dbClient.connect();

        // load only the customers that are needed by this subtask
        RuntimeContext ctx = getRuntimeContext();
        dbClient.fetchCustomersByCondition(
                    // the logic here is specific to {@link CustomerIdPartitioner#partition()}
                    String.format("id %% %d = %d",ctx.getNumberOfParallelSubtasks(), ctx.getIndexOfThisSubtask())
                )
                .forEach(customer -> customersInPartition.put(customer.getId(), customer) );
    }

    @Override
    public void close() throws Exception {
        super.close();
        if ( dbClient != null) dbClient.close();
    }

    @Override
    public EnrichedOrder map(Order order) throws Exception {
        int customerId = order.getCustomerId();
        Customer customer = customersInPartition.get(customerId);
        if (customer == null) customer = Customer.NOT_FOUND;
        return new EnrichedOrder(order, customer);
    }

    public static class CustomerIdPartitioner implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
        }
    }

}
