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
import com.ververica.enrich.record.CustomerChangeLog;
import com.ververica.enrich.record.EnrichedOrder;
import com.ververica.enrich.record.Order;
import com.ververica.enrich.util.TimeUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

public class OrderCustomerCoProcessOnEventTimeFunction extends KeyedCoProcessFunction<Integer, Order, CustomerChangeLog, EnrichedOrder> {

    private transient MapState<Long, Customer> customerMapState;

    private transient MapState<Long, Order> orderMapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<Long, Customer> customerMapStateDescriptor = new MapStateDescriptor<>(
                "customer",
                Long.class, Customer.class);
        customerMapState = getRuntimeContext().getMapState(customerMapStateDescriptor);

        MapStateDescriptor<Long, Order> orderMapStateDescriptor = new MapStateDescriptor<>(
                "order",
                Long.class, Order.class
        );
        orderMapState = getRuntimeContext().getMapState(orderMapStateDescriptor);

    }

    @Override
    public void processElement1(Order order, Context ctx, Collector<EnrichedOrder> out) throws Exception {
        long orderTimestamp = ctx.timestamp();
        // Assuming there is no more than two orders having the same timestamp placed by a single customer
        orderMapState.put(orderTimestamp, order);
        ctx.timerService().registerEventTimeTimer(orderTimestamp);

    }

    @Override
    public void processElement2(CustomerChangeLog customerChangeLog, Context ctx, Collector<EnrichedOrder> out) throws Exception {
        Customer customer = customerChangeLog.getOp() == RowKind.DELETE ?
                Customer.IS_DELETED : customerChangeLog.getCustomer();
        customerMapState.put(ctx.timestamp(), customer);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedOrder> out) throws Exception {
        // event timer triggered
        enrichBasedOnEventTime(timestamp, out);
    }

    private void enrichBasedOnEventTime(long timestamp, Collector<EnrichedOrder> out) throws Exception {
        // Since we register a timer for every order, we can get the order from the state directly
        Order order = orderMapState.get(timestamp);
        Long customerTimestamp = findLatestCustomerChangelogTimestamp(timestamp);
        Customer customer = customerTimestamp == null ?
                Customer.NOT_FOUND : customerMapState.get(customerTimestamp);
        out.collect(new EnrichedOrder(order, customer));
        orderMapState.remove(timestamp);

        // clean up old customer from the state
        removeCustomerChangelogTimestamp(timestamp);
    }

    private Long findLatestCustomerChangelogTimestamp(long timestamp) throws Exception {
        TreeSet<Long> timestampSet = new TreeSet<>();
        customerMapState.keys().forEach(timestampSet::add);
        return timestampSet.floor(timestamp);
    }

    private void removeCustomerChangelogTimestamp(long timestamp) throws Exception {
        TreeSet<Long> timestampSet = new TreeSet<>();
        customerMapState.keys().forEach(timestampSet::add);
        Iterator<Long> timestampIterator = timestampSet.iterator();
        while (timestampIterator.hasNext()) {
            Long customerTimestamp = timestampIterator.next();
            if ( customerTimestamp <= timestamp) {
                if ( timestampIterator.hasNext() ) {
                    // remove only if there are at least one more, i.e., keep always the latest one
                    timestampIterator.remove();
                }
            }
        }
    }

}
