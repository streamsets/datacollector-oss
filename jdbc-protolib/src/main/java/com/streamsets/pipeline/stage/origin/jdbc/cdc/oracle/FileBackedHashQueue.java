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
package com.streamsets.pipeline.stage.origin.jdbc.cdc.oracle;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;

/*
We remove all commit-related actions and references as we are using mmap files with no expected failover behavior.
It would be necessary to reconsider this decision if in the future this class relies on other supporting DBMap
entities.
 */
public class FileBackedHashQueue<E> implements HashQueue<E> {

  /* Some benchmarks show that there is a huge performance penalty using the old MB_100 (100 * 1024 * 1024) value.
  For small transactions, the old size is too big, and transactions are about 50 times slower with it.
  For large transactions, there are no essential noticeable differences starting from our proposed size. With
  transactions up to 1.000.000 operations, this value seems to be close the its optimum.
   */
  public static final int MB_1 = 1024 * 1024;
  private HTreeMap underlying;
  private final DB db;
  private E tail;
  private File folder;

  private LinkedHashSet<RsIdSsn> keys = new LinkedHashSet<>();

  public FileBackedHashQueue(File file) throws IOException {
    this.folder = file;
    Files.createDirectories(file.toPath());
    Path f = Files.createTempDirectory(file.toPath(), "db-");
    db = DBMaker.fileDB(new File(f.toFile(), "dbFile"))
        .allocateStartSize(MB_1)
        .allocateIncrement(MB_1)
        .closeOnJvmShutdown()
        .fileDeleteAfterOpen()
        .fileDeleteAfterClose()
        .fileMmapEnable()
        .fileLockDisable()
        .cleanerHackEnable()
        .fileMmapPreclearDisable()
        .make();

    underlying = db.hashMap("t").create();
  }

  @Override
  @SuppressWarnings("unchecked")
  public E tail() {
    return tail;
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
    RecordSequence incoming = (RecordSequence) o;
    RsIdSsn key = new RsIdSsn(incoming.rsId, incoming.ssn.toString());
    return keys.contains(key);
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
    RecordSequence incoming = (RecordSequence)e;
    RsIdSsn key = new RsIdSsn(incoming.rsId, incoming.ssn.toString());
    keys.add(key);
    tail = e;
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
    Iterator<RsIdSsn> it = keys.iterator();
    RsIdSsn key = it.next();
    it.remove();
    if (keys.isEmpty()) {
      tail = null;
    }
    return (E) underlying.get(key);
  }

  @Override
  public E poll() {
    return keys.isEmpty() ? null : remove();
  }

  @Override
  @SuppressWarnings("unchecked")
  public E element() {
    Iterator<RsIdSsn> it = keys.iterator();
    RsIdSsn key = it.next();
    if (key == null) {
      throw new NoSuchElementException();
    }
    return (E) underlying.get(key);
  }

  @Override
  public E peek() {
    if (!underlying.isEmpty()) {
      try {
        return element();
      } catch (NoSuchElementException eNoSuchElementException) {
        return null;
      }
    }
    return null;
  }

  public void close() {
    this.underlying.close();
    this.db.close();
    try {
      FileUtils.deleteDirectory(folder);
      underlying = null;
      keys = null;
    } catch (Throwable eThrowable) {
    }
  }

  @Override
  public void completeInserts() {
    return;
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
