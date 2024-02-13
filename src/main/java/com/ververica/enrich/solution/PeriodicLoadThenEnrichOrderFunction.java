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
import com.ververica.enrich.util.TimeUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PeriodicLoadThenEnrichOrderFunction extends KeyedProcessFunction<Integer, Order, EnrichedOrder> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PeriodicLoadThenEnrichOrderFunction.class);

    private static final int MAX_CUSTOMER_ID = 1000;

    // ISO-8601 duration format PnDTnHnMn.nS with days considered to be exactly 24 hours.
    private final String reloadInterval;

    protected transient DatabaseClient dbClient;
    protected DatabaseClientParameter dbClientParameter;
    protected transient List<Integer> customerIdList;
    protected transient HashMap<Integer, Customer> customersInPartition;

    /**
     *
     * @param dbClientParameter database client connection parameter
     * @param reloadInterval ISO-8601 duration format PnDTnHnMn.nS with days considered to be exactly 24 hours.
     *                       For example, "PT1M" (one minute).
     *                       See <a href="https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-">Java doc</a>
     */
    public PeriodicLoadThenEnrichOrderFunction(DatabaseClientParameter dbClientParameter, String reloadInterval) {
        this.dbClientParameter = dbClientParameter;
        this.reloadInterval = reloadInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        customersInPartition = new HashMap<>();

        dbClient = DatabaseClient.createDatabaseClient(dbClientParameter);
        dbClient.connect();

        // load only the customers that are needed by this subtask
        RuntimeContext ctx = getRuntimeContext();
        customerIdList = getCustomerIdsOfThisSubtask(
                ctx.getMaxNumberOfParallelSubtasks(),
                ctx.getNumberOfParallelSubtasks(),
                ctx.getIndexOfThisSubtask()
        );

        dbClient.fetchCustomersByIdList(customerIdList)
                .forEach(customer -> customersInPartition.put(customer.getId(), customer) );

    }

    private List<Integer> getCustomerIdsOfThisSubtask(int maxParallelism, int numParallelSubtasks, int subtaskIndex) {
        List<Integer> customerIdList = new ArrayList<>();

        for (int i=0; i<MAX_CUSTOMER_ID; i++) {
            if ( subtaskIndex ==
                    KeyGroupRangeAssignment.assignKeyToParallelOperator(
                        i,
                        maxParallelism,
                        numParallelSubtasks) ) {
                customerIdList.add(i);
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
    public void processElement(Order order, Context ctx, Collector<EnrichedOrder> out) throws Exception {
        int customerId = order.getCustomerId();
        Customer customer = customersInPartition.get(customerId);
        if (customer == null) customer = Customer.NOT_FOUND;
        out.collect(new EnrichedOrder(order, customer));

        scheduleNextReload(ctx);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedOrder> out) throws Exception {
        dbClient.fetchCustomersByIdList(customerIdList)
                .forEach(customer -> customersInPartition.put(customer.getId(), customer) );

        scheduleNextReload(ctx);
    }

    private void scheduleNextReload(Context ctx) {
        long tsNextTimeUnit = TimeUtils.nextFull( Duration.parse(reloadInterval).toMillis() );

        LOGGER.info("schedule next reload at: " + Instant.ofEpochMilli(tsNextTimeUnit) );
        ctx.timerService().registerProcessingTimeTimer(tsNextTimeUnit);
    }

}
