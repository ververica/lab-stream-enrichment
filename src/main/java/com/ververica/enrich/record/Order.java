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

import com.ververica.enrich.util.TimeUtils;
import lombok.*;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {
    private String timestamp;
    private int id;
    private int customerId;
    private float amount;

    public static Order DUMMY_ORDER = Order.builder()
            .timestamp("1970-01-01 00:00:00.000")
            .id(-1)
            .customerId(-1)
            .amount(0)
            .build();

    public long getEventTimestamp () {
        return TimeUtils.dateTimeToEpochMs(timestamp);
    }
}
