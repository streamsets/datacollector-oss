/**
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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileBackedHashQueue<E> implements HashQueue<E> {

  public static final int MB_100 = 100 * 1024 * 1024;
  private HTreeMap underlying;
  private final DB db;
  private final ScheduledExecutorService executorService =
      Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("Queue Clean up").build());

  private InMemoryHashQueue<RsIdSsn> keys = new InMemoryHashQueue<>();

  public FileBackedHashQueue(File file) {
    db = DBMaker.fileDB(file)
        .allocateStartSize(MB_100)
        .allocateIncrement(MB_100)
        .closeOnJvmShutdown()
        .fileDeleteAfterOpen()
        .fileDeleteAfterClose()
        .fileMmapEnable()
        .fileLockDisable()
        .concurrencyDisable()
        .make();

    underlying = db.hashMap("t").create();
    executorService.scheduleWithFixedDelay(() -> db.getStore().compact(), 10, 10, TimeUnit.MINUTES);

  }

  @Override
  @SuppressWarnings("unchecked")
  public E tail() {
    RsIdSsn key = keys.tail();
    return (E) underlying.get(key);
  }

  @Override
  public int size() {
    return keys.size();
  }

  @Override
  public boolean isEmpty() {
    return keys.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return keys.contains(o);
  }

  @NotNull
  @Override
  @SuppressWarnings("unchecked")
  public Iterator<E> iterator() {
    return new FileBackedHashQueueIterator();
  }

  @NotNull
  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException();
  }

  @NotNull
  @Override
  public <T> T[] toArray(@NotNull T[] a) {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean add(E e) {
    RsIdSsn key = new RsIdSsn(((RecordSequence)e).rsId, ((RecordSequence)e).ssn.toString());
    keys.add(key);
    underlying.put(key, e);
    return true;
  }

  @Override
  public boolean remove(Object e) {
    return keys.remove(e);
  }

  @Override
  public boolean containsAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addAll(@NotNull Collection<? extends E> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    keys.clear();
    underlying.clear();
  }

  @Override
  public boolean offer(E e) {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public E remove() {
    RsIdSsn key = keys.remove();
    return (E) underlying.get(key);
  }

  @Override
  public E poll() {
    return keys.isEmpty() ? null : remove();
  }

  @Override
  @SuppressWarnings("unchecked")
  public E element() {
    throw new UnsupportedOperationException();
  }

  @Override
  public E peek() {
    return underlying.isEmpty() ? null : element();
  }

  public void close() {
    executorService.shutdown();
    this.db.close();
  }

  private class FileBackedHashQueueIterator implements Iterator<E> {

    @SuppressWarnings("unchecked")
    private final Iterator<RsIdSsn> underlyingIter = keys.iterator();

    @Override
    public boolean hasNext() {
      return underlyingIter.hasNext();
    }

    @Override
    @SuppressWarnings("unchecked")
    public E next() {
      RsIdSsn key = underlyingIter.next();
      return (E) underlying.get(key);
    }

    /**
     * This iterator's remove is special. It removes the first element always (which is our CDC use-case)
     */
    @Override
    public void remove() {
      underlyingIter.remove();
    }
  }
}
