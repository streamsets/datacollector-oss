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
package com.streamsets.pipeline.stage.processor.statsaggregation;

import java.util.Iterator;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * A bounded deque that also de-dupes values between consecutive offers.
 *
 * This is required to make sure we store only the last n alert texts and do not store 2 consecutive alert texts that
 * happen to be the same.
 *
 * @param <E>
 */
class BoundedDeque<E> implements Iterable<E> {

  private final LinkedBlockingDeque<E> linkedBlockingDeque;

  public BoundedDeque(int capacity) {
    linkedBlockingDeque = new LinkedBlockingDeque<>(capacity);
  }

  public boolean offerLast(E e) {
    if (null != e && (null == linkedBlockingDeque.peekLast() || !e.equals(linkedBlockingDeque.peekLast()))) {
      if (linkedBlockingDeque.remainingCapacity() == 0) {
        linkedBlockingDeque.removeFirst();
      }
      linkedBlockingDeque.offerLast(e);
      return true;
    }
    return false;
  }

  public Iterator<E> iterator() {
    return linkedBlockingDeque.iterator();
  }
}
