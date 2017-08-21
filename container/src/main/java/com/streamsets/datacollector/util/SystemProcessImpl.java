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
package com.streamsets.datacollector.util;

import com.google.common.base.Joiner;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.util.SystemProcess;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SystemProcessImpl implements SystemProcess {
  private static final Logger LOG = LoggerFactory.getLogger(SystemProcessImpl.class);
  private static final AtomicLong fileCounter = new AtomicLong(0);
  private static final int OUT_FILE_LIMIT = 50;
  static final String OUT_EXT = ".out";
  static final String ERR_EXT = ".err";
  private static final Method DESTROY_FORCIBLY;
  static {
    Method destroyForcibly;
    try {
      destroyForcibly = Process.class.getDeclaredMethod("destroyForcibly");
    } catch (NoSuchMethodException e) {
      destroyForcibly = null;
    }
    DESTROY_FORCIBLY = destroyForcibly;
  }
  protected ImmutableList<String> args;
  private final File tempDir;
  private final File input = new File("/dev/null");
  private final File output;
  private final File error;
  private SimpleFileTailer outputTailer;
  private SimpleFileTailer errorTailer;
  private Process delegate;

  SystemProcessImpl(String name, File tempDir, File logDir) {
    String id = nextId();
    this.tempDir = tempDir;
    output = new File(logDir, Utils.format("{}-{}{}", name, id, OUT_EXT));
    error = new File(logDir, Utils.format("{}-{}{}", name, id, ERR_EXT));
  }

  public SystemProcessImpl(String name, File tempDir, List<String> args) {
    clean(tempDir, OUT_FILE_LIMIT);
    String id = nextId();
    output = new File(tempDir, Utils.format("{}-{}{}", name, id, OUT_EXT));
    error = new File(tempDir, Utils.format("{}-{}{}", name, id, ERR_EXT));
    this.tempDir = tempDir;
    this.args = ImmutableList.copyOf(args);
  }

  /**
   * @return a unique number which shorts in descending order
   */
  private static String nextId() {
    NumberFormat numberFormat = NumberFormat.getInstance();
    numberFormat.setMinimumIntegerDigits(10);
    numberFormat.setGroupingUsed(false);
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");
    return Utils.format("{}-{}", dateFormat.format(new Date()), numberFormat.format(fileCounter.incrementAndGet()));
  }

  static void clean(File tempDir, int limit) {
    String[] files = tempDir.list((dir, name) -> name.endsWith(OUT_EXT) || name.endsWith(ERR_EXT));
    if (files != null && files.length > limit) {
      List<String> fileList = new ArrayList<>(files.length);
      fileList.addAll(Arrays.asList(files));
      Collections.sort(fileList);
      while (fileList.size() > limit) {
        File file = new File(tempDir, fileList.remove(0));
        if (!FileUtils.deleteQuietly(file)) {
          LOG.warn("Could not delete: {}", file);
        }
      }
    }
  }

  @Override
  public void start() throws IOException {
    start(new HashMap<String, String>());
  }

  @Override
  public void start(Map<String, String> env) throws IOException {
    Utils.checkState(output.createNewFile(), Utils.formatL("Could not create output file: {}", output));
    Utils.checkState(error.createNewFile(), Utils.formatL("Could not create error file: {}", error));
    Utils.checkState(delegate == null, "start can only be called once");
    LOG.info("Standard output for process written to file: " + output);
    LOG.info("Standard error for process written to file: " + error);
    ProcessBuilder processBuilder = new ProcessBuilder()
      .redirectInput(input)
      .redirectOutput(output)
      .redirectError(error)
      .directory(tempDir).command(args);
    processBuilder.environment().putAll(env);
    LOG.info("Starting: " + args);
    delegate = processBuilder.start();
    ThreadUtil.sleep(100); // let it start
    outputTailer = new SimpleFileTailer(output);
    errorTailer = new SimpleFileTailer(error);
  }

  @Override
  public String getCommand() {
    return Joiner.on(" ").join(args);
  }

  @Override
  public boolean isAlive() {
    return delegate != null && isAlive(delegate);
  }

  @Override
  public void cleanup() {
    if (outputTailer != null) {
      outputTailer.close();
    }
    if (errorTailer != null) {
      errorTailer.close();
    }
    kill(5000);
  }
  @Override
  public Collection<String> getAllOutput() {
    if (outputTailer != null) {
      return outputTailer.getAllData();
    }
    return new ArrayList<>();
  }

  @Override
  public Collection<String> getAllError() {
    if (errorTailer != null) {
      return errorTailer.getAllData();
    }
    return new ArrayList<>();
  }

  @Override
  public Collection<String> getOutput() {
    if (outputTailer != null) {
      return outputTailer.getData();
    }
    return new ArrayList<>();
  }

  @Override
  public Collection<String> getError() {
    if (errorTailer != null) {
      return errorTailer.getData();
    }
    return new ArrayList<>();
  }

  @Override
  public void kill(long timeoutBeforeForceKill) {
    if (outputTailer != null) {
      outputTailer.close();
    }
    if (errorTailer != null) {
      errorTailer.close();
    }
    if (delegate != null && isAlive(delegate)) {
      delegate.destroy();
      long start = System.currentTimeMillis();
      while (isAlive(delegate) && (System.currentTimeMillis() - start) > timeoutBeforeForceKill) {
        if (!ThreadUtil.sleep(100)) {
          break;
        }
      }
      if (isAlive(delegate)) {
        if (DESTROY_FORCIBLY != null) {
          try {
            DESTROY_FORCIBLY.invoke(delegate);
          } catch (Exception e) {
            LOG.error("Error trying to call destroyForcibly on {}: {}", delegate, e, e);
          }
        }
      }
    }
  }

  @Override
  public String toString() {
    return Utils.format("SystemProcess: {} ", Joiner.on(" ").join(args));
  }

  @Override
  public int exitValue() {
    return delegate.exitValue();
  }

  @Override
  public boolean waitFor(long timeout, TimeUnit unit) {
    return waitFor(delegate, timeout, unit);
  }

  /**
   * Java 1.7 does not have Process.isAlive
   */
  private static boolean isAlive(Process process) {
    try {
      process.exitValue();
      return false;
    } catch(IllegalThreadStateException e) {
      return true;
    }
  }

  /**
   * Java 1.7 does not have Process.waitFor(timeout)
   */
  private static boolean waitFor(Process process, long timeout, TimeUnit unit) {
    long startTime = System.nanoTime();
    long rem = unit.toNanos(timeout);
    do {
      try {
        process.exitValue();
        return true;
      } catch(IllegalThreadStateException ex) {
        if (rem > 0)
          ThreadUtil.sleep(
            Math.min(TimeUnit.NANOSECONDS.toMillis(rem) + 1, 100));
      }
      rem = unit.toNanos(timeout) - (System.nanoTime() - startTime);
    } while (rem > 0);
    return false;
  }

  private static class SimpleFileTailer {
    private final File file;
    private final EvictingQueue<String> history;
    private final RandomAccessFile randomAccessFile;
    private final byte[] inbuf;

    public SimpleFileTailer(File file) {
      this.file = file;
      this.history = EvictingQueue.create(2500);
      this.inbuf = new byte[8192 * 8];
      try {
        this.randomAccessFile = new RandomAccessFile(file, "r");
      } catch (FileNotFoundException e) {
        throw new RuntimeException(Utils.format("Unexpected error reading output file '{}': {}", file, e), e);
      }
    }

    public void close() {
      IOUtils.closeQuietly(randomAccessFile);
    }

    public List<String> getData() {
      List<String> result = new ArrayList<>();
      try {
        readLines(randomAccessFile, result);
      } catch (IOException e) {
        throw new RuntimeException(Utils.format("Error reading from '{}': {}", file, e), e);
      }
      history.addAll(result);
      return result;
    }

    public Collection<String> getAllData() {
      EvictingQueue<String> result = EvictingQueue.create(2500);
      BufferedReader reader = null;
      try {
        reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
          result.add(line);
        }
      } catch (IOException e) {
        String msg = Utils.format("Error reading from command output file '{}': {}", file, e);
        throw new RuntimeException(msg, e);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (IOException ex) {
            // ignored
          }
        }
      }
      return result;
    }

    /**
     * Read new lines.
     *
     * @param reader The file to read
     * @return The new position after the lines have been read
     * @throws java.io.IOException if an I/O error occurs.
     */
    private long readLines(final RandomAccessFile reader, List<String> result) throws IOException {
      ByteArrayOutputStream lineBuf = new ByteArrayOutputStream(64);
      long pos = reader.getFilePointer();
      long rePos = pos; // position to re-read
      int num;
      boolean seenCR = false;
      while (((num = reader.read(inbuf)) != -1)) {
        for (int i = 0; i < num; i++) {
          final byte ch = inbuf[i];
          switch (ch) {
            case '\n':
              seenCR = false; // swallow CR before LF
              result.add(new String(lineBuf.toByteArray(), StandardCharsets.UTF_8));
              lineBuf.reset();
              rePos = pos + i + 1;
              break;
            case '\r':
              if (seenCR) {
                lineBuf.write('\r');
              }
              seenCR = true;
              break;
            default:
              if (seenCR) {
                seenCR = false; // swallow final CR
                result.add(new String(lineBuf.toByteArray(), StandardCharsets.UTF_8));
                lineBuf.reset();
                rePos = pos + i + 1;
              }
              lineBuf.write(ch);
          }
        }
        pos = reader.getFilePointer();
      }
      IOUtils.closeQuietly(lineBuf); // not strictly necessary
      reader.seek(rePos); // Ensure we can re-read if necessary
      return rePos;
    }
  }
}
