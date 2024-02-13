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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LoadKeyGroupPartitionThenEnrichOrderFunction extends RichMapFunction<Order, EnrichedOrder>  {

    private static final int MAX_CUSTOMER_ID = 1000;

    protected transient DatabaseClient dbClient;
    protected DatabaseClientParameter dbClientParameter;
    protected transient HashMap<Integer, Customer> customersInPartition;

    public LoadKeyGroupPartitionThenEnrichOrderFunction(DatabaseClientParameter dbClientParameter) {
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
        List<Integer> customerIdList = getCustomerIdsOfThisSubtask(
                ctx.getMaxNumberOfParallelSubtasks(),
                ctx.getNumberOfParallelSubtasks(),
                ctx.getIndexOfThisSubtask()
        );

        dbClient.fetchCustomersByIdList(customerIdList)
                .forEach(customer -> customersInPartition.put(customer.getId(), customer) );

    }

    private List<Integer> getCustomerIdsOfThisSubtask(int maxParallelism, int numParallelSubtasks, int subtaskIndex) {
        List<Integer> customerIdList = new ArrayList<>();

        for (int customerId=0; customerId<MAX_CUSTOMER_ID; customerId++) {
            if ( subtaskIndex ==
                    KeyGroupRangeAssignment.assignKeyToParallelOperator(
                        customerId,
                        maxParallelism,
                        numParallelSubtasks) ) {
                customerIdList.add(customerId);
            }

        }

        return customerIdList;
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

}
