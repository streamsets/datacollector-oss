/*
 * Copyright 2017 StreamSets Inc.
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
package com.streamsets.pipeline.stage.origin.oracle.cdc;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class InMemoryHashQueue<E> implements HashQueue<E> {

  private E tail;
  private final LinkedHashSet<E> underlying;

  InMemoryHashQueue() {
    underlying = new LinkedHashSet<>();
  }

  InMemoryHashQueue(int initialSize) {
    underlying = new LinkedHashSet<>(initialSize);
  }

  @Override
  public E tail() {
    return tail;
  }

  @Override
  public int size() {
    return underlying.size();
  }

  @Override
  public boolean isEmpty() {
    return underlying.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return underlying.contains(o);
  }

  @NotNull
  @Override
  public Iterator<E> iterator() {
    return new QueueIterator();
  }

  @NotNull
  @Override
  public Object[] toArray() {
    return underlying.toArray();
  }

  @NotNull
  @Override
  @SuppressWarnings("unchecked")
  public <T> T[] toArray(@NotNull T[] a) {
    return underlying.toArray(a);
  }

  @Override
  public boolean add(E val) {
    boolean ret = underlying.add(val);
    if (ret) {
      tail = val;
    }
    return ret;
  }

  @Override
  public boolean offer(E e) {
    return underlying.add(e);
  }

  @Override
  public E remove() {
    Iterator<E> iter = underlying.iterator();
    E ret = iter.next(); // throws expected exception if queue is empty
    iter.remove();
    if (underlying.isEmpty()) {
      tail = null;
    }
    return ret;
  }

  @Override
  public E poll() {
    return underlying.isEmpty() ? null : remove();
  }

  @Override
  public E element() {
    return underlying.iterator().next(); // throws expected exception if queue is empty
  }

  @Override
  public E peek() {
    return underlying.isEmpty() ? null : element();
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsAll(@NotNull Collection<?> c) {
    return underlying.containsAll(c);
  }

  @Override
  public boolean addAll(@NotNull Collection<? extends E> c) {
    final AtomicBoolean added = new AtomicBoolean(false);
    c.forEach(e -> {
          boolean addedE = underlying.add(e);
          added.set(added.get() || addedE);
          if (addedE) {
            tail = e;
          }
        }
    );
    return added.get();
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    underlying.clear();
    tail = null;
  }

  @Override
  public boolean equals(Object o) {
    return o != null && o instanceof InMemoryHashQueue && underlying.equals(((InMemoryHashQueue)o).underlying);
  }

  public void close() {
    underlying.clear();
  }

  @Override
  public void completeInserts() {
    // no op
  }

  @Override
  public int hashCode() {
    return underlying.hashCode();
  }

  private class QueueIterator implements Iterator<E> {

    private Iterator<E> underlyingIterator = underlying.iterator();

    @Override
    public boolean hasNext() {
      return underlyingIterator.hasNext();
    }

    @Override
    public E next() {
      return underlyingIterator.next();
    }

    public void remove() {
      underlyingIterator.remove();
      if (underlying.isEmpty()) {
        tail = null;
      }
    }
  }

}
