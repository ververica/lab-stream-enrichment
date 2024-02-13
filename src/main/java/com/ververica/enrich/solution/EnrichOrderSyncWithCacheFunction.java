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

import com.ververica.enrich.function.EnrichOrderSyncFunction;
import com.ververica.enrich.record.Customer;
import com.ververica.enrich.record.EnrichedOrder;
import com.ververica.enrich.record.Order;
import com.ververica.enrich.util.DatabaseClientParameter;
import com.ververica.enrich.util.EnrichmentCache;
import org.apache.flink.configuration.Configuration;

public class EnrichOrderSyncWithCacheFunction extends EnrichOrderSyncFunction {
    private transient EnrichmentCache<Integer, Customer> cache;
    private final int cacheExpiration;

    protected DatabaseClientParameter dbClientParameter;

    /**
     *
     * @param dbClientParameter database client connection parameter
     * @param cacheExpiration cache expiration time in milliseconds
     */
    public EnrichOrderSyncWithCacheFunction(DatabaseClientParameter dbClientParameter, int cacheExpiration) {
        super(dbClientParameter);
        this.cacheExpiration = cacheExpiration;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.cache = new EnrichmentCache<>(cacheExpiration);
    }

    @Override
    public EnrichedOrder map(Order order) throws Exception {
        int customerId = order.getCustomerId();
        Customer customer = this.cache.get(customerId);
        if ( customer == null ) {
            customer = this.dbClient.fetchCustomerById(customerId);
            this.cache.put(customerId, customer);
        }

        return new EnrichedOrder(order, customer);
    }

}