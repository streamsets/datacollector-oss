/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
