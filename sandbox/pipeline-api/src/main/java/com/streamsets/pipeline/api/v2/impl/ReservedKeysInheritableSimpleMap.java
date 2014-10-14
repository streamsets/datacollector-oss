/**
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
package com.streamsets.pipeline.api.v2.impl;

import com.google.common.base.Preconditions;

public class ReservedKeysInheritableSimpleMap<V> extends InheritableSimpleMap<String, V> {
  private String reservedNamespace;

  public ReservedKeysInheritableSimpleMap(String reservedNamespace) {
    this.reservedNamespace = Preconditions.checkNotNull(reservedNamespace,
                                                        "reservedNamespace cannot be NULL");
  }

  public ReservedKeysInheritableSimpleMap(ReservedKeysInheritableSimpleMap<V> parent) {
    super(parent);
    this.reservedNamespace = parent.getReservedNamespace();
  }

  public String getReservedNamespace() {
    return reservedNamespace;
  }

  private void validateKey(String key) {
    Preconditions.checkNotNull(key, "key cannot be NULL");
    Preconditions.checkArgument(key.startsWith(reservedNamespace),
                                String.format("'The '%s' key prefix is reserved", reservedNamespace));
  }

  @Override
  public V put(String key, V value) {
    validateKey(key);
    return super.put(key, value);
  }

  @Override
  public V remove(String key) {
    validateKey(key);
    return super.remove(key);
  }

}
