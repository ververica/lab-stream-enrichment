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

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;

/**
 * This class deals with caching of customers for enrichment.
 *
 * TODO: limit cache size
 *
 * @param <K> Type of key
 * @param <V> Type of value
 */

public class EnrichmentCache<K, V> {
    private final HashMap<K, Tuple2<V, Long>> cache;
    // cache expiration in milliseconds
    private final int expiration;

    public EnrichmentCache(int expiration) {
        this.cache = new HashMap<>();
        this.expiration = expiration;
    }

    /**
     *
     * @param id Customer id
     * @return null if not found or expired. Otherwise, return V
     */
    public V get(K id) {
        Tuple2<V, Long> tuple = this.cache.get(id);

        if ( tuple == null ) return null;

        if ( (System.currentTimeMillis() - tuple.f1) > this.expiration ) {
            this.cache.remove(id);
            return null;
        }

        return tuple.f0;
    }

    public void put(K key, V value) {
        this.cache.put(
                key,
                new Tuple2<>( value, System.currentTimeMillis() )
        );
    }
}
