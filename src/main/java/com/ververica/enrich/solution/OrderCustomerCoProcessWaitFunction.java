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
import com.ververica.enrich.record.CustomerChangeLog;
import com.ververica.enrich.record.EnrichedOrder;
import com.ververica.enrich.record.Order;
import com.ververica.enrich.util.TimeUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class OrderCustomerCoProcessWaitFunction extends KeyedCoProcessFunction<Integer, Order, CustomerChangeLog, EnrichedOrder> {

    // ISO-8601 duration format PnDTnHnMn.nS with days considered to be exactly 24 hours.
    private String waitDuration;
    private transient ValueState<Customer> customerState;
    private transient ListState<Order> orderListState;

    /**
     * @param delayDuration ISO-8601 duration format PnDTnHnMn.nS with days considered to be exactly 24 hours.
     *                      For example, "PT10S" (ten seconds).
     *                      See <a href="https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html#parse-java.lang.CharSequence-">Java doc</a>
     */
    public OrderCustomerCoProcessWaitFunction(String delayDuration) {
        this.waitDuration = delayDuration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Customer> customerValueStateDescriptor = new ValueStateDescriptor<>(
                "customer",
                Customer.class);
        customerState = getRuntimeContext().getState(customerValueStateDescriptor);

        ListStateDescriptor<Order> orderListStateDescriptor = new ListStateDescriptor<>(
                "orderList",
                Order.class
        );
        orderListState = getRuntimeContext().getListState(orderListStateDescriptor);
    }

    @Override
    public void processElement1(Order order, Context ctx, Collector<EnrichedOrder> out) throws Exception {
        Customer customer = customerState.value();
        if ( customer == null ) {
            orderListState.add(order);
            ctx.timerService().registerProcessingTimeTimer(
                TimeUtils.timestampIn(Duration.parse(waitDuration).toMillis())
            );
        } else {
            out.collect(new EnrichedOrder(order, customer));
        }
    }

    @Override
    public void processElement2(CustomerChangeLog customerChangeLog, Context ctx, Collector<EnrichedOrder> out) throws Exception {
        if (customerChangeLog.getOp() == RowKind.DELETE ) {
            customerState.clear();
        } else {
            customerState.update(customerChangeLog.getCustomer());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<EnrichedOrder> out) throws Exception {
        Customer customer = customerState.value();
        if ( customer != null ) {
            for (Order order : orderListState.get()) {
                out.collect(new EnrichedOrder(order, customer));
            }
            orderListState.clear();
        } else {
            // wait again
            ctx.timerService().registerProcessingTimeTimer(
                    TimeUtils.timestampIn(Duration.parse(waitDuration).toMillis())
            );
        }

    }
}
