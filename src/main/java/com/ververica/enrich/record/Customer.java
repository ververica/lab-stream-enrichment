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
import org.apache.flink.types.Row;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Customer {
    private static Customer createStaticCustomer(String name) {
        return Customer.builder()
                .name(name)
                .id(-1)
                .country("Unknown")
                .zipcode("000000")
                .status("Unknown")
                .updatedAt("1970-01-01 00:00:00.000")
        .build();
    }

    public static Customer fromRow(Row row) {
        return Customer.builder()
                .name((String)row.getField("name"))
                .id((int)row.getField("id"))
                .country((String)row.getField("country"))
                .zipcode((String)row.getField("zipcode"))
                .status((String)row.getField("status"))
                .updatedAt((String)row.getField("updatedAt"))
                .build();
    }

    public static Customer NOT_FOUND = createStaticCustomer("Not Found");
    public static Customer IS_DELETED = createStaticCustomer("Is Deleted");
    public static Customer TIME_OUT =  createStaticCustomer("Timed Out");
    public static Customer FAILED =  createStaticCustomer("Enrich Failed");

    private int id;
    private String name;
    private String country;
    private String zipcode;
    // free, basic, business, enterprise
    private String status;
    private String updatedAt;
}
