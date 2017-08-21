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
package com.streamsets.pipeline.lib.queue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ForwardingQueue;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This is a copy of Guava's EvictingQueue (cannot subclass because it is final) that
 * adds the method {@link #addAndGetEvicted}
 */
public final class XEvictingQueue<E> extends ForwardingQueue<E> implements Serializable {

  private final Queue<E> delegate;

  @VisibleForTesting
  final int maxSize;

  private XEvictingQueue(int maxSize) {
    checkArgument(maxSize >= 0, "maxSize (%s) must >= 0", maxSize);
    this.delegate = new ArrayDeque<E>(maxSize);
    this.maxSize = maxSize;
  }

  /**
   * Creates and returns a new evicting queue that will hold up to {@code maxSize} elements.
   *
   * <p>When {@code maxSize} is zero, elements will be evicted immediately after being added to the
   * queue.
   */
  public static <E> XEvictingQueue<E> create(int maxSize) {
    return new XEvictingQueue<E>(maxSize);
  }

  /**
   * Returns the number of additional elements that this queue can accept without evicting;
   * zero if the queue is currently full.
   *
   * @since 16.0
   */
  public int remainingCapacity() {
    return maxSize - size();
  }

  @Override protected Queue<E> delegate() {
    return delegate;
  }

  /**
   * Adds the given element to this queue. If the queue is currently full, the element at the head
   * of the queue is evicted to make room.
   *
   * @return {@code true} always
   */
  @Override public boolean offer(E e) {
    return add(e);
  }

  /**
   * Adds the given element to this queue. If the queue is currently full, the element at the head
   * of the queue is evicted to make room.
   *
   * @return {@code true} always
   */
  @Override public boolean add(E e) {
    checkNotNull(e);  // check before removing
    if (maxSize == 0) {
      return true;
    }
    if (size() == maxSize) {
      delegate.remove();
    }
    delegate.add(e);
    return true;
  }

  /**
   * Adds the given element to this queue. If the queue is currently full, the element at the head
   * of the queue is evicted to make room and returns the evicted element.
   *
   * @return the evicted element, <code>NULL</code> if none.
   */
  public E addAndGetEvicted(E e) {
    checkNotNull(e);  // check before removing
    if (maxSize == 0) {
      return null;
    }
    E evicted = null;
    if (size() == maxSize) {
      evicted = delegate.remove();
    }
    delegate.add(e);
    return evicted;
  }

  @Override public boolean addAll(Collection<? extends E> collection) {
    return standardAddAll(collection);
  }

  @Override
  public boolean contains(Object object) {
    return delegate().contains(checkNotNull(object));
  }

  @Override
  public boolean remove(Object object) {
    return delegate().remove(checkNotNull(object));
  }

  // TODO(user): Do we want to checkNotNull each element in containsAll, removeAll, and retainAll?

  private static final long serialVersionUID = 0L;
}
