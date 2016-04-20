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
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.FakeRecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.destination.hdfs.Errors;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsFileType;
import com.streamsets.pipeline.stage.destination.hdfs.IdleClosedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class RecordWriterManager {
  private final static Logger LOG = LoggerFactory.getLogger(RecordWriterManager.class);

  private URI hdfsUri;;
  private Configuration hdfsConf;
  private String uniquePrefix;
  private String dirPathTemplate;
  private PathResolver pathResolver;
  private TimeZone timeZone;
  private long cutOffMillis;
  private long cutOffSize;
  private long cutOffRecords;
  private HdfsFileType fileType;
  private CompressionCodec compressionCodec;
  private SequenceFile.CompressionType compressionType;
  private String keyEL;
  private DataGeneratorFactory generatorFactory;
  private Target.Context context;
  private final Path tempFilePath;
  private final LoadingCache<String, Path> dirPathCache;
  private long idleTimeoutSeconds = -1L;

  public RecordWriterManager(URI hdfsUri, Configuration hdfsConf, String uniquePrefix, String dirPathTemplate,
      TimeZone timeZone, long cutOffSecs, long cutOffSizeBytes, long cutOffRecords, HdfsFileType fileType,
      CompressionCodec compressionCodec, SequenceFile.CompressionType compressionType, String keyEL,
      DataGeneratorFactory generatorFactory, Target.Context context, String config) {
    this.hdfsUri = hdfsUri;
    this.hdfsConf = hdfsConf;
    this.uniquePrefix = uniquePrefix;
    this.dirPathTemplate = dirPathTemplate;
    this.timeZone = timeZone;
    this.cutOffMillis = preventOverflow(cutOffSecs * 1000);
    this.cutOffSize = cutOffSizeBytes;
    this.cutOffRecords = cutOffRecords;
    this.fileType = fileType;
    this.compressionCodec = compressionCodec;
    this.compressionType = compressionType;
    this.keyEL = keyEL;
    this.generatorFactory = generatorFactory;
    this.context = context;
    pathResolver = new PathResolver(context, config, dirPathTemplate, timeZone);

    // we use/reuse Path as they are expensive to create (it increases the performance by at least 3%)
    tempFilePath = new Path(getTempFileName());
    dirPathCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.MINUTES).build(
        new CacheLoader<String, org.apache.hadoop.fs.Path>() {
          @Override
          public Path load(String key) throws Exception {
            return new Path(key, tempFilePath);
          }
        });
  }

  public boolean validateDirTemplate(
      String group,
      String config,
      String qualifiedConfigName,
      List<Stage.ConfigIssue> issues
  ) {
    return pathResolver.validate(group, config, qualifiedConfigName, issues);
  }

  public void setIdleTimeoutSeconds(long idleTimeoutSeconds) {
    this.idleTimeoutSeconds = idleTimeoutSeconds;
  }

  public long getCutOffMillis() {
    return cutOffMillis;
  }

  public long getCutOffSizeBytes() {
    return cutOffSize;
  }

  public long getCutOffRecords() {
    return cutOffRecords;
  }

  String getDirPath(Date date, Record record) throws StageException {
    return pathResolver.resolvePath(date, record);
  }

  String getExtension() {
    return (compressionCodec == null) ? "" : compressionCodec.getDefaultExtension();
  }

  String getTempFileName() {
    return "_tmp_" + uniquePrefix + getExtension();
  }

  public String getDirPath(Date date) throws StageException {
    return pathResolver.resolvePath(date, null);
  }

  public Path getPath(Date recordDate, Record record) throws StageException {
    // runUuid is fixed for the current pipeline run. it avoids collisions with other SDCs running the same/similar
    // pipeline
    try {
      return dirPathCache.get(getDirPath(recordDate, record));
    } catch (ExecutionException ex) {
      if (ex.getCause() instanceof StageException) {
        throw (StageException) ex.getCause();
      } else{
        throw new StageException(Errors.HADOOPFS_24, ex.toString(), ex);
      }
    }
  }

  Path renameToFinalName(FileSystem fs, Path tempPath) throws IOException {
    Path parent = tempPath.getParent();
    Path finalPath = new Path(parent, uniquePrefix + "_" + UUID.randomUUID().toString() + getExtension());
    if (!fs.rename(tempPath, finalPath)) {
      throw new IOException(Utils.format("Could not rename '{}' to '{}'", tempPath, finalPath));
    }
    return finalPath;
  }

  long getTimeToLiveMillis(Date now, Date recordDate) {
    // we up the record date to the greatest one based on the template
    recordDate = pathResolver.getCeilingDate(recordDate);
    if (recordDate != null) {
      return preventOverflow(recordDate.getTime() + cutOffMillis) - now.getTime();
    } else {
      return Long.MAX_VALUE;
    }
  }

  RecordWriter createWriter(FileSystem fs, Path path, long timeToLiveMillis) throws StageException, IOException {
    switch (fileType) {
      case TEXT:
        OutputStream os = fs.create(path, false);
        if (compressionCodec != null) {
          try {
            os = compressionCodec.createOutputStream(os);
          } catch (UnsatisfiedLinkError unsatisfiedLinkError) {
            throw new StageException(Errors.HADOOPFS_46, compressionType.name(), unsatisfiedLinkError,
              unsatisfiedLinkError);
          }
        }
        RecordWriter recordWriter = new RecordWriter(path, timeToLiveMillis, os, generatorFactory);
        if (idleTimeoutSeconds != -1) {
          recordWriter.setIdleTimeout(idleTimeoutSeconds);
        }
        return recordWriter;
      case SEQUENCE_FILE:
        Utils.checkNotNull(compressionType, "compressionType");
        Utils.checkNotNull(keyEL, "keyEL");
        Utils.checkArgument(compressionCodec == null || compressionType != SequenceFile.CompressionType.NONE,
                            "if using a compressionCodec, compressionType cannot be NULL");
        try {
          SequenceFile.Writer writer = SequenceFile.createWriter(fs, hdfsConf, path, Text.class, Text.class,
                                                                 compressionType, compressionCodec);
          RecordWriter seqRecordWriter =
              new RecordWriter(path, timeToLiveMillis, writer, keyEL, generatorFactory, context);
          if (idleTimeoutSeconds != -1) {
            seqRecordWriter.setIdleTimeout(idleTimeoutSeconds);
          }
          return seqRecordWriter;
        } catch (UnsatisfiedLinkError unsatisfiedLinkError) {
          throw new StageException(Errors.HADOOPFS_46, compressionType.name(), unsatisfiedLinkError,
            unsatisfiedLinkError);
        }
      default:
        throw new UnsupportedOperationException(Utils.format("Unsupported file Type '{}'", fileType));
    }
  }

  public RecordWriter getWriter(Date now, Date recordDate, Record record) throws StageException, IOException {
    RecordWriter writer = null;
    long writerTimeToLive = getTimeToLiveMillis(now, recordDate);
    Path tempPath = getPath(recordDate, record);
    if (writerTimeToLive >= 0) {
      FileSystem fs = FileSystem.get(hdfsUri, hdfsConf);
      if (fs.exists(tempPath)) {
        Path path = renameToFinalName(fs, tempPath);
        LOG.warn("Path[{}] - Found previous file '{}', committing it", tempPath, path);
      }
      LOG.debug("Path[{}] - Create writer,  time to live '{}ms'", tempPath, writerTimeToLive);
      writer = createWriter(fs, tempPath, writerTimeToLive);
    } else {
      LOG.warn("Path[{}] - Cannot not create writer, requested date already cut off", tempPath);
    }
    return writer;
  }

  public Path commitWriter(RecordWriter writer) throws IOException {
    Path path = null;
    if (!writer.isClosed()) {
      // Unset the interrupt flag before close(). InterruptedIOException makes close() fail
      // resulting that the tmp file never gets renamed when stopping the pipeline.
      boolean interrupted = Thread.interrupted();
      try {
        writer.close();
      } catch (IdleClosedException e) {
        LOG.info("Failed to close writer writer for {} was closed as it was idle", writer.getPath());
      }
      // Reset the interrupt flag back.
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
      LOG.debug("Path[{}] - Committing Writer to '{}'", writer.getPath(), path);
      FileSystem fs = FileSystem.get(hdfsUri, hdfsConf);
      path = renameToFinalName(fs, writer.getPath());
    }
    return path;
  }

  public boolean isOverThresholds(RecordWriter writer) throws IOException {
    boolean overLength = (cutOffSize > 0) && writer.getLength() >= cutOffSize;
    boolean overRecords = (cutOffRecords > 0) && writer.getRecords() >= cutOffRecords;
    boolean over = overLength || overRecords;
    if (over) {
      LOG.debug("Path[{}] - Over threshold, length={} records={}", writer.getPath(), overLength, overRecords);
    }
    return over;
  }

  Date incrementDate(Date date, int timeIncrement) {
    Calendar calendar = Calendar.getInstance(timeZone);
    calendar.setTime(date);
    calendar.add(timeIncrement, 1);
    return calendar.getTime();
  }

  String createGlob(Calendar calendar) throws ELEvalException {
    ELVars vars = context.createELVars();
    ELEval elEval = context.createELEval("dirPathTemplate", FakeRecordEL.class);
    TimeEL.setCalendarInContext(vars, calendar);
    String dirGlob = elEval.eval(vars, dirPathTemplate, String.class);
    return dirGlob + "/" + getTempFileName();
  }

  List<String> getGlobs() throws ELEvalException {
    List<String> globs = new ArrayList<>();
    Calendar endCalendar = Calendar.getInstance(timeZone);
    if (pathResolver.getTimeIncrementUnit() > -1) {

      // we need to scan dirs from last batch minus the cutOff time
      Calendar calendar = Calendar.getInstance(timeZone);
      calendar.setTime(new Date(context.getLastBatchTime()));
      calendar.add(Calendar.MILLISECOND, (int) -cutOffMillis);
      // adding an extra hour to scan
      calendar.add(Calendar.HOUR, -1);

      // set the calendar to the floor date of the computed start time
      calendar.setTime(pathResolver.getFloorDate(calendar.getTime()));

      LOG.info("Looking for uncommitted files from '{}' onwards", calendar.getTime());

      // iterate from last batch time until now at the dir template minimum time precision increments
      // we create a glob for each template time tick
      for (; calendar.compareTo(endCalendar) < 0;
           calendar.add(pathResolver.getTimeIncrementUnit(), pathResolver.getTimeIncrementValue())) {
        globs.add(createGlob(calendar));
      }
    } else {
      // in this case we don't use the calendar at all as the dir template does not use time functions
      globs.add(createGlob(endCalendar));
    }
    return globs;
  }

  public void commitOldFiles(FileSystem fs) throws IOException, ELEvalException {
    // if getLastBatchTime() is zero it means we never run, nothing to commit
    if (context.getLastBatchTime() > 0) {
      for (String glob : getGlobs()) {
        LOG.debug("Looking for uncommitted files using glob '{}'", glob);
        FileStatus[] globStatus = fs.globStatus(new Path(glob));
        if (globStatus != null) {
          for (FileStatus status : globStatus) {
            LOG.debug("Found uncommitted file '{}'", status.getPath());
            renameToFinalName(fs, status.getPath());
          }
        }
      }
    }
  }

  private long preventOverflow(long valueToVerify) {
    return (valueToVerify > 0) ? valueToVerify : Long.MAX_VALUE;
  }


}
