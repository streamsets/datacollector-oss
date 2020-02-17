/*
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.restapi.rbean.lang;

public abstract class RValue<V extends RValue, T> extends RType<V> {

  private final Type type;
  private T value;
  private boolean scrubbed;

  public RValue(Type type) {
    this.type = type;
  }

  public RValue(Type type, T value) {
    this.type = type;
    this.value = value;
  }

  public Type getType() {
    return type;
  }

  public T getValue() {
    return value;
  }

  @SuppressWarnings("unchecked")
  public V setValue(T value) {
    if (!scrubbed) {
      this.value = value;
    }
    return (V) this;
  }

  public boolean isScrubbed() {
    return scrubbed;
  }

  @SuppressWarnings("unchecked")
  public V setScrubbed(boolean scrubbed) {
    this.scrubbed = scrubbed;
    if (scrubbed) {
      value = null;
    }
    return (V) this;
  }

  //AG
  @Override
  @SuppressWarnings("unchecked")
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RValue<V, ?> rValue = (RValue<V, ?>) o;

    return getValue() != null ? getValue().equals(rValue.getValue()) : rValue.getValue() == null;
  }

  //AG
  @Override
  public int hashCode() {
    return getValue() != null ? getValue().hashCode() : 0;
  }

  //AG
  @Override
  public String toString() {
    return getClass().getSimpleName() + "{ value=" + getValue() + '}';
  }

}
