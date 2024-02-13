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

package com.ververica.enrich.record;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EnrichedOrder {
    private String timestamp;
    private int id;
    private int customerId;
    private float amount;

    private String name;
    private String country;
    private String zipcode;
    private String status;

    public EnrichedOrder(Order order, Customer customer) {
        timestamp = order.getTimestamp();
        id = order.getId();
        customerId = order.getCustomerId();
        amount = order.getAmount();

        name = customer.getName();
        country = customer.getCountry();
        zipcode = customer.getZipcode();
        status = customer.getStatus();
    }

}
