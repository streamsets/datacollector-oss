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
package com.streamsets.datacollector.io;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.io.input.ProxyInputStream;
import org.apache.commons.io.output.ProxyOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class DataStore {
  private final static Logger LOG = LoggerFactory.getLogger(DataStore.class);
  private final static Map<Path, CounterLock> FILE_LOCKS = new HashMap<>();

  private final Path file;
  private final Path fileTmp;
  private final Path fileNew;
  private final Path fileOld;
  private Closeable stream;
  private boolean forWrite;
  private boolean isClosed;
  private boolean isRecovered;

  /**
   * Lock with counter so that we know how many threads are using the lock.
   */
  private static class CounterLock {

    ReentrantLock lock;
    int counter;

    public CounterLock() {
      lock = new ReentrantLock();
      counter = 1;
    }

    public void inc() {
      counter++;
    }

    public void dec() {
      counter--;
    }

    public void lock() {
      lock.lock();
    }

    public boolean isHeldByCurrentThread() {
      return lock.isHeldByCurrentThread();
    }

    public void unlock() {
      lock.unlock();
    }
  }

  public DataStore(File file) {
    Utils.checkNotNull(file, "file");
    File absFile = file.getAbsoluteFile();
    this.file = absFile.toPath();
    fileTmp = new File(absFile.getAbsolutePath() + "-tmp").toPath();
    fileNew = new File(absFile.getAbsolutePath() + "-new").toPath();
    fileOld = new File(absFile.getAbsolutePath() + "-old").toPath();
    LOG.trace("Create DataStore for '{}'", file);
  }

  public File getFile() {
    return file.toFile();
  }

  public boolean isRecovered() {
    return isRecovered;
  }

  public void close() throws IOException {
    LOG.trace("Close DataStore for '{}'", file);
    synchronized (DataStore.class) {
      if (stream != null) {
        try {
          stream.close();
        } catch (IOException ex) {
          LOG.error("DataStore '{}' error while closing stream, {}", file, ex.toString(), ex);
        }
        FILE_LOCKS.remove(file);
        throw new IOException(Utils.format("DataStore '{}' closed while open for '{}'", file,
                                           (forWrite) ? "WRITE" : "READ"));
      }
    }
  }

  @Override
  protected void finalize() throws Throwable {
    close();
  }

  @VisibleForTesting
  void acquireLock() {
    LOG.trace("Acquiring lock for '{}'", file);
    CounterLock lock;
    synchronized (DataStore.class) {
      lock = FILE_LOCKS.get(file);
      if (lock == null) {
        lock = new CounterLock();
        FILE_LOCKS.put(file, lock);
      } else {
        lock.inc();
        Utils.checkState(!lock.isHeldByCurrentThread(), Utils.format("The current thread already has a lock on '{}'",
                                                                     file));
      }
    }
    lock.lock();
    LOG.trace("Acquired lock '{}' for '{}'", lock, file);
  }

  /**
   * This method must be used after completing the write to output stream which was obtained by calling the
   * {@link #getOutputStream()} method.
   *
   * If the write operation was successful, then this method must be called after calling the
   * {@link #commit(java.io.OutputStream)} method. Otherwise it must be called after calling the {@link #close()} on the
   * output stream.
   *
   * Example usage:
   *
   * DataStore dataStore = new DataStore(...);
   * try (OutputStream os = dataStore.getOutputStream()) {
   *   os.write(..);
   *   dataStore.commit(os);
   * } catch (IOException e) {
   *   ...
   * } finally {
   *   dataStore.release();
   * }
   *
   */
  public void release() {
    CounterLock lock;
    synchronized (DataStore.class) {
      lock = FILE_LOCKS.get(file);

      if(lock == null) {
        LOG.error("Trying to release unlocked file {}", file);
        return;
      }

      lock.dec();

      if(lock.counter == 0) {
        FILE_LOCKS.remove(file);
      }
    }

    LOG.trace("Releasing the lock {} for '{}'", lock, file);
    lock.unlock();
    LOG.trace("Released the lock {} for '{}'", lock, file);
  }

  /**
   * Returns an input stream for the requested file.
   *
   * After completing the read the stream must be closed. It is not necessary to call the {@link #release()}
   * method after reading from the input stream.
   *
   * Example usage:
   *
   * DataStore dataStore = new DataStore(...);
   * try (InputStream is = dataStore.getInputStream()) {
   *   // read from is
   * } catch (IOException e) {
   *   ...
   * }
   *
   * @return
   * @throws IOException
   */
  public InputStream getInputStream() throws IOException {
    acquireLock();
    try {
      isClosed = false;
      forWrite = false;
      LOG.trace("Starts read '{}'", file);
      verifyAndRecover();
      InputStream is = new ProxyInputStream(new FileInputStream(file.toFile())) {
        @Override
        public void close() throws IOException {
          if (isClosed) {
            return;
          }
          try {
            super.close();
          } finally {
            release();
            isClosed = true;
            stream = null;
          }
          LOG.trace("Finishes read '{}'", file);
        }
      };
      stream = is;
      return is;
    } catch (Exception ex) {
      release();
      throw ex;
    }
  }

  private final static FileAttribute DEFAULT_PERMISSIONS = PosixFilePermissions.asFileAttribute(
    ImmutableSet.of(PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ)
  );

  /**
   * Returns an output stream for the requested file.
   *
   * After completing the write the contents must be committed using the {@link #commit(java.io.OutputStream)}
   * method and the stream must be released using the {@link #release()} method.
   *
   * Example usage:
   *
   * DataStore dataStore = new DataStore(...);
   * try (OutputStream os = dataStore.getOutputStream()) {
   *   os.write(..);
   *   dataStore.commit(os);
   * } catch (IOException e) {
   *   ...
   * } finally {
   *   dataStore.release();
   * }
   *
   * @return
   * @throws IOException
   */
  public OutputStream getOutputStream() throws IOException {
    acquireLock();
    try {
      isClosed = false;
      forWrite = true;
      LOG.trace("Starts write '{}'", file);
      verifyAndRecover();
      FileAttribute permissions = DEFAULT_PERMISSIONS;
      if (Files.exists(file)) {
        permissions = PosixFilePermissions.asFileAttribute(Files.getPosixFilePermissions(file));
        Files.move(file, fileOld);
        LOG.trace("Starting write, move '{}' to '{}'", file, fileOld);
      }
      Files.createFile(fileTmp, permissions);
      OutputStream os = new ProxyOutputStream(new FileOutputStream(fileTmp.toFile())) {
        @Override
        public void close() throws IOException {
          if (isClosed) {
            return;
          }
          try {
            super.close();
          } finally {
            isClosed = true;
            stream = null;
          }
          LOG.trace("Finishes write '{}'", file);
        }
      };
      stream = os;
      return os;
    } catch (Exception ex) {
      release();
      throw ex;
    }
  }

  /**
   * This method must be used to commit contents written to the output stream which is obtained by calling
   * the {@link #getOutputStream()} method. This method closes the argument output stream.
   *
   * Example usage:
   *
   * DataStore dataStore = new DataStore(...);
   * try (OutputStream os = dataStore.getOutputStream()) {
   *   os.write(..);
   *   dataStore.commit(os);
   * } catch (IOException e) {
   *   ...
   * } finally {
   *   dataStore.release();
   * }
   *
   * @throws IOException
   */
  public void commit(OutputStream out) throws IOException {
    // close the stream in order to flush the contents into the disk
    Utils.checkNotNull(out, "Argument output stream cannot be null");
    Utils.checkState(stream == out, "The argument output stream must be the same as the output stream obtained " +
      "from this data store instance");
    out.close();
    Files.move(fileTmp, fileNew);
    LOG.trace("Committing write, move '{}' to '{}'", fileTmp, fileNew);
    Files.move(fileNew, file);
    LOG.trace("Committing write, move '{}' to '{}'", fileNew, file);
    if (Files.exists(fileOld)) {
      Files.delete(fileOld);
      LOG.trace("Committing write, deleting '{}'", fileOld);
    }
    isRecovered = false;
    LOG.trace("Committed");
  }

  private void verifyAndRecover() throws IOException {
    if (Files.exists(fileOld) || Files.exists(fileTmp) || Files.exists(fileNew)) {
      if (Files.exists(fileNew)) {
        LOG.warn("File '{}', write completed but not committed, committing", file);
        if (Files.exists(fileTmp)) {
          throw new IOException(Utils.format("File '{}' exists, '{}' should not exist", fileNew, fileTmp));
        }
        if (Files.exists(file)) {
          throw new IOException(Utils.format("File '{}' exists, '{}' should not exist", fileNew, file));
        }
        Files.move(fileNew, file);
        if (Files.exists(fileOld)) {
          Files.delete(fileOld);
          LOG.warn("File '{}', deleted during verification", fileOld);
        }
        isRecovered = true;
        LOG.warn("File '{}', committed during verification", file);
      } else if (Files.exists(fileTmp)) {
        LOG.warn("File '{}', write incomplete while writing, rolling back", file);
        if (!Files.exists(fileOld)) {
          throw new IOException(Utils.format("File '{}' exists, '{}' should exists", fileTmp, fileOld));
        }
        if (Files.exists(file)) {
          throw new IOException(Utils.format("File '{}' exists, '{}' should not exist", fileTmp, file));
        }
        Files.delete(fileTmp);
        Files.move(fileOld, file);
        LOG.warn("File '{}', rolled back during verification", file);
        isRecovered = true;
      } else if (Files.exists(fileOld)) {
        if (Files.exists(file)) {
          LOG.warn("Both file {} and old file '{}' exists, deleting old file during verification", file, fileOld);
          Files.delete(fileOld);
        } else {
          Files.move(fileOld, file);
          LOG.warn("File '{}', rolled back during verification", file);
          isRecovered = true;
        }
      }
    } else {
      LOG.trace("File '{}' no recovery needed", file);
    }
  }

  /**
   * Check if the DataStore exists and contains data.
   * This method will check for the presence of the set of files that can be used to read data from the store.
   */
  public boolean exists() throws IOException {
    acquireLock();
    try {
      verifyAndRecover();
      return Files.exists(file) && Files.size(file) > 0;
    } finally {
      release();
    }
  }

  public void delete() throws IOException {
    if (Files.exists(fileTmp)) {
      Files.delete(fileTmp);
    }
    if (Files.exists(fileOld)) {
      Files.delete(fileOld);
    }
    if (Files.exists(fileNew)) {
      Files.delete(fileNew);
    }
    if (Files.exists(file)) {
      Files.delete(file);
    }
  }
}
