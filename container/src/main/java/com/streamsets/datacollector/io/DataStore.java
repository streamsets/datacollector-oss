/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
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
package com.streamsets.datacollector.io;

import com.google.common.annotations.VisibleForTesting;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class DataStore {
  private final static Logger LOG = LoggerFactory.getLogger(DataStore.class);
  private final static Map<Path, ReentrantLock> FILE_LOCKS = new HashMap<>();

  private final Path file;
  private final Path fileTmp;
  private final Path fileNew;
  private final Path fileOld;
  private Closeable stream;
  private boolean forWrite;
  private boolean isClosed;

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
    ReentrantLock lock  = null;
    synchronized (DataStore.class) {
      lock = FILE_LOCKS.get(file);
      if (lock == null) {
        lock = new ReentrantLock();
        FILE_LOCKS.put(file, lock);
      } else {
        Utils.checkState(!lock.isHeldByCurrentThread(), Utils.format("The current thread already has a lock on '{}'",
                                                                     file));
      }
    }
    lock.lock();
    LOG.trace("Acquired lock for '{}'", file);
  }

  @VisibleForTesting
  void releaseLock() {
    ReentrantLock lock;
    synchronized (DataStore.class) {
      lock = FILE_LOCKS.remove(file);
    }
    if (lock != null) {
      LOG.trace("Releasing the lock for '{}'", file);
      lock.unlock();
      LOG.trace("Released the lock for '{}'", file);
    }
  }


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
            releaseLock();
            isClosed = true;
            stream = null;
          }
          LOG.trace("Finishes read '{}'", file);
        }
      };
      stream = is;
      return is;
    } catch (Exception ex) {
      releaseLock();
      throw ex;
    }
  }

  public OutputStream getOutputStream() throws IOException {
    acquireLock();
    try {
      isClosed = false;
      forWrite = true;
      LOG.trace("Starts write '{}'", file);
      verifyAndRecover();
      if (Files.exists(file)) {
        Files.move(file, fileOld);
        LOG.trace("Starting write, move '{}' to '{}'", file, fileOld);
      }
      OutputStream os = new ProxyOutputStream(new FileOutputStream(fileTmp.toFile())) {
        @Override
        public void close() throws IOException {
          if (isClosed) {
            return;
          }
          try {
            super.close();
            Files.move(fileTmp, fileNew);
            LOG.trace("Finishing write, move '{}' to '{}'", fileTmp, fileNew);
            Files.move(fileNew, file);
            LOG.trace("Finishing write, move '{}' to '{}'", fileNew, file);
            if (Files.exists(fileOld)) {
              Files.delete(fileOld);
              LOG.trace("Finishing write, deleting '{}'", fileOld);
            }
          } finally {
            releaseLock();
            isClosed = true;
            stream = null;
          }
          LOG.trace("Finishes write '{}'", file);
        }
      };
      stream = os;
      return os;
    } catch (Exception ex) {
      releaseLock();
      throw ex;
    }
  }


  private void verifyAndRecover() throws IOException {
    if (Files.exists(fileOld) || Files.exists(fileTmp) || Files.exists(fileNew)) {
      if (Files.exists(fileNew)) {
        LOG.debug("File '{}', write completed but not committed, committing", file);
        if (Files.exists(fileTmp)) {
          throw new IOException(Utils.format("File '{}' exists, '{}' should not exist", fileNew, fileTmp));
        }
        if (Files.exists(file)) {
          throw new IOException(Utils.format("File '{}' exists, '{}' should not exist", fileNew, file));
        }
        Files.move(fileNew, file);
        if (Files.exists(fileOld)) {
          Files.delete(fileOld);
          LOG.trace("File '{}', deleted during verification", fileOld);
        }
        LOG.trace("File '{}', committed during verification", file);
      } else if (Files.exists(fileTmp)) {
        LOG.debug("File '{}', write incomplete while writing, rolling back", file);
        if (!Files.exists(fileOld)) {
          throw new IOException(Utils.format("File '{}' exists, '{}' should exists", fileTmp, fileOld));
        }
        if (Files.exists(file)) {
          throw new IOException(Utils.format("File '{}' exists, '{}' should not exist", fileTmp, file));
        }
        Files.delete(fileTmp);
        Files.move(fileOld, file);
        LOG.trace("File '{}', rolled back during verification", file);
      } else if (Files.exists(fileOld)) {
        LOG.debug("File '{}', write incomplete while starting write, rolling back", file);
        if (Files.exists(file)) {
          throw new IOException(Utils.format("File '{}' exists, '{}' should not exist", fileOld, file));
        }
        Files.move(fileOld, file);
        LOG.trace("File '{}', rolled back during verification", file);
      }
    } else {
      LOG.trace("File '{}' no recovery needed", file);
    }
  }

}
