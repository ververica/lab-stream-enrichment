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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CustomerChangeLog {
    private RowKind op;

    private Customer customer;

    public long getEventTimestamp() {
        return TimeUtils.dateTimeToEpochMs(customer.getUpdatedAt());
    }

    public static CustomerChangeLog fromRow(Row row) {
        return CustomerChangeLog.builder()
                .op(row.getKind())
                .customer(Customer.fromRow(row))
                .build();
    }

}