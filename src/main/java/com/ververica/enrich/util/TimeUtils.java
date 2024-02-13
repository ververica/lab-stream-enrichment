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

package com.ververica.enrich.util;

import java.time.Instant;

public class TimeUtils {

    public static long timestampIn(long ms) {
        long epoch = Instant.now().toEpochMilli();
        return epoch + ms;
    }

    /**
     * Get the timestamp of the next full time unit, e.g., next full hour, next full minute, next full 5 minutes, etc.
     * @param ms the duration in milliseconds. For the next full minute, call with ms=60000
     * @return the calculated timestamp T such that T is divisible by ms and T - Now <= ms
     */
    public static long nextFull(long ms) {
        long epoch = Instant.now().toEpochMilli();
        return (epoch / ms) * ms + ms;
    }

    /**
     * Get the timestamp of the next of the next full time unit
     * @param ms the duration in milliseconds. For the next full minute, call with ms=60000
     * @return the calculated timestamp T such that T is divisible by ms and ms <= T - Now <= 2 * ms
     */
    public static long nextNextFull(long ms) {
        return nextFull(ms) + ms;
    }


    public static long dateTimeToEpochMs(String dateTime) {
        // "2024-03-06 12:45:48.782" -> "2024-03-06T12:45:48.782Z"
        if ( !dateTime.contains("T") ) {
            dateTime = dateTime.replace(' ', 'T');
            dateTime += "Z";
        }

        return Instant.parse(dateTime).toEpochMilli();
    }
}
