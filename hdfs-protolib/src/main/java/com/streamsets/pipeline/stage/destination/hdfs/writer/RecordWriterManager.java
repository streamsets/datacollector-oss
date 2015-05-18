/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.FakeRecordEL;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.destination.hdfs.Errors;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsFileType;
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

  public static void validateDirPathTemplate1(Target.Context context, String pathTemplate) {
    getCeilingDateBasedOnTemplate(pathTemplate, TimeZone.getDefault(), new Date());
  }

  public static void validateDirPathTemplate2(Target.Context context, String pathTemplate) throws ELEvalException {
    ELEval dirPathTemplateEval = context.createELEval("dirPathTemplate");
    ELVars vars = context.createELVars();
    RecordEL.setRecordInContext(vars, context.createRecord("validateDirPathTemplate"));
    Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
    calendar.setTime(new Date());
    TimeEL.setCalendarInContext(vars, calendar);
    dirPathTemplateEval.eval(vars, pathTemplate, String.class);
  }

  private URI hdfsUri;;
  private Configuration hdfsConf;
  private String uniquePrefix;
  private String dirPathTemplate;
  private ELEval dirPathTemplateElEval;
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

  public RecordWriterManager(URI hdfsUri, Configuration hdfsConf, String uniquePrefix, String dirPathTemplate,
      TimeZone timeZone, long cutOffSecs, long cutOffSizeBytes, long cutOffRecords, HdfsFileType fileType,
      CompressionCodec compressionCodec, SequenceFile.CompressionType compressionType, String keyEL,
      DataGeneratorFactory generatorFactory, Target.Context context) {
    this.hdfsUri = hdfsUri;
    this.hdfsConf = hdfsConf;
    this.uniquePrefix = uniquePrefix;
    this.dirPathTemplate = dirPathTemplate;
    this.timeZone = timeZone;
    this.cutOffMillis = cutOffSecs * 1000;
    this.cutOffSize = cutOffSizeBytes;
    this.cutOffRecords = cutOffRecords;
    this.fileType = fileType;
    this.compressionCodec = compressionCodec;
    this.compressionType = compressionType;
    this.keyEL = keyEL;
    this.generatorFactory = generatorFactory;
    this.context = context;
    dirPathTemplateElEval = context.createELEval("dirPathTemplate");
    getCeilingDateBasedOnTemplate(dirPathTemplate, timeZone, new Date());

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

  public long getCutOffMillis() {
    return cutOffMillis;
  }

  public long getCutOffSizeBytes() {
    return cutOffSize;
  }

  public long getCutOffRecords() {
    return cutOffRecords;
  }

  private static final String CONST_YYYY = "YYYY";
  private static final String CONST_YY = "YY";
  private static final String CONST_MM = "MM";
  private static final String CONST_DD = "DD";
  private static final String CONST_hh = "hh";
  private static final String CONST_mm = "mm";
  private static final String CONST_ss = "ss";

  String getDirPath(Date date, Record record) throws StageException {
    try {
      ELVars vars = context.createELVars();
      RecordEL.setRecordInContext(vars, record);
      Calendar calendar = Calendar.getInstance(timeZone);
      calendar.setTime(date);
      TimeEL.setCalendarInContext(vars, calendar);
      return dirPathTemplateElEval.eval(vars, dirPathTemplate, String.class);
    } catch (ELEvalException ex) {
      throw new StageException(Errors.HADOOPFS_02, dirPathTemplate, ex.getMessage(), ex);
    }
  }

  String getExtension() {
    return (compressionCodec == null) ? "" : compressionCodec.getDefaultExtension();
  }

  String getTempFileName() {
    return "_tmp_" + uniquePrefix + getExtension();
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
        throw new StageException(Errors.HADOOPFS_24, ex.getMessage(), ex);
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

  static Date getCeilingDateBasedOnTemplate(String dirPathTemplate, TimeZone timeZone, Date date) {
    Calendar calendar = null;
    boolean done = false;
    if (!dirPathTemplate.contains("${" + CONST_YY + "()}") && !dirPathTemplate.contains(CONST_YYYY)) {
      done = true;
    } else {
      calendar = Calendar.getInstance(timeZone);
      calendar.setTime(date);
    }
    if (!dirPathTemplate.contains("${" + CONST_MM + "()}")) {
      if (!done) {
        calendar.set(Calendar.MONTH, calendar.getActualMaximum(Calendar.MONTH));
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        calendar.set(Calendar.HOUR_OF_DAY, calendar.getActualMaximum(Calendar.HOUR_OF_DAY));
        calendar.set(Calendar.MINUTE, calendar.getActualMaximum(Calendar.MINUTE));
        calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));
        calendar.set(Calendar.MILLISECOND, calendar.getActualMaximum(Calendar.MILLISECOND));
        done = true;
      }
    }
    else {
      if (done) {
        throw new IllegalArgumentException(
            "dir path template has the '${MM()}' token but does not have the '${YY()}' or '${YYYY()}' tokens");
      }
    }
    if (!dirPathTemplate.contains("${" + CONST_DD + "()}")) {
      if (!done) {
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        calendar.set(Calendar.HOUR_OF_DAY, calendar.getActualMaximum(Calendar.HOUR_OF_DAY));
        calendar.set(Calendar.MINUTE, calendar.getActualMaximum(Calendar.MINUTE));
        calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));
        calendar.set(Calendar.MILLISECOND, calendar.getActualMaximum(Calendar.MILLISECOND));
        done = true;
      }
    } else {
      if (done) {
        throw new IllegalArgumentException(
            "dir path template has the '${DD()}' token but does not have the '${MM()}' token");
      }
    }
    if (!dirPathTemplate.contains("${" + CONST_hh + "()}")) {
      if (!done) {
        calendar.set(Calendar.HOUR_OF_DAY, calendar.getActualMaximum(Calendar.HOUR_OF_DAY));
        calendar.set(Calendar.MINUTE, calendar.getActualMaximum(Calendar.MINUTE));
        calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));
        calendar.set(Calendar.MILLISECOND, calendar.getActualMaximum(Calendar.MILLISECOND));
        done = true;
      }
    } else {
      if (done) {
        throw new IllegalArgumentException(
            "dir path template has the '${hh()}' token but does not have the '${DD()}' token");
      }
    }
    if (!dirPathTemplate.contains("${" + CONST_mm + "()}")) {
      if (!done) {
        calendar.set(Calendar.MINUTE, calendar.getActualMaximum(Calendar.MINUTE));
        calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));
        calendar.set(Calendar.MILLISECOND, calendar.getActualMaximum(Calendar.MILLISECOND));
        done = true;
      }
    } else {
      if (done) {
        throw new IllegalArgumentException(
            "dir path template has the '${mm()}' token but does not have the '${hh()}' token");
      }
    }
    if (!dirPathTemplate.contains("${" + CONST_ss + "()}")) {
      if (!done) {
        calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));
      }
    } else {
      if (done) {
        throw  new IllegalArgumentException(
            "dir path template has the '${ss()}' token but does not have the '${mm()}' token");
      }
    }
    if (calendar != null) {
      calendar.set(Calendar.MILLISECOND, calendar.getActualMaximum(Calendar.MILLISECOND));
      date = calendar.getTime();
    } else {
      return null;
    }
    return date;
  }

  long getTimeToLiveMillis(Date now, Date recordDate) {
    // we up the record date to the greatest one based on the template
    recordDate = getCeilingDateBasedOnTemplate(dirPathTemplate, timeZone, recordDate);
    return (recordDate != null) ? recordDate.getTime() + cutOffMillis - now.getTime() : Long.MAX_VALUE;
  }

  RecordWriter createWriter(FileSystem fs, Path path, long timeToLiveMillis) throws StageException, IOException {
    switch (fileType) {
      case TEXT:
        OutputStream os = fs.create(path, false);
        if (compressionCodec != null) {
          os = compressionCodec.createOutputStream(os);
        }
        return new RecordWriter(path, timeToLiveMillis, os, generatorFactory);
      case SEQUENCE_FILE:
        Utils.checkNotNull(compressionType, "compressionType");
        Utils.checkNotNull(keyEL, "keyEL");
        Utils.checkArgument(compressionCodec == null || compressionType != SequenceFile.CompressionType.NONE,
                            "if using a compressionCodec, compressionType cannot be NULL");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, hdfsConf, path, Text.class, Text.class,
                                                               compressionType, compressionCodec);
        return new RecordWriter(path, timeToLiveMillis, writer, keyEL, generatorFactory, context);
      default:
        throw new UnsupportedOperationException(Utils.format("Unsupported file Type '{}'", fileType));
    }
  }

  public RecordWriter getWriter(Date now, Date recordDate, Record record) throws StageException, IOException {
    RecordWriter writer = null;
    long writerTimeToLive = getTimeToLiveMillis(now, recordDate);
    Path tempPath = getPath(recordDate, record);
    if (writerTimeToLive > 0) {
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
      writer.close();
      FileSystem fs = FileSystem.get(hdfsUri, hdfsConf);
      path = renameToFinalName(fs, writer.getPath());
      LOG.debug("Path[{}] - Committing Writer to '{}'", writer.getPath(), path);
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

  // returns the time procession of the directory template
  int getTimeIncrement(String template) {
    if (template.contains("${" + CONST_ss + "()}")) {
      return Calendar.SECOND;
    }
    if (template.contains("${" + CONST_mm + "()}")) {
      return Calendar.MINUTE;
    }
    if (template.contains("${" + CONST_hh + "()}")) {
      return Calendar.HOUR;
    }
    if (template.contains("${" + CONST_DD + "()}")) {
      return Calendar.DATE;
    }
    if (template.contains("${" + CONST_MM + "()}")) {
      return Calendar.MONTH;
    }
    if (template.contains("${" + CONST_YY + "()}")) {
      return Calendar.YEAR;
    }
    if (template.contains("${" + CONST_YYYY + "()}")) {
      return Calendar.YEAR;
    }
    return -1;
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
    int timeIncrement = getTimeIncrement(dirPathTemplate);
    Calendar endCalendar = Calendar.getInstance(timeZone);
    if (timeIncrement > -1) {

      // we need to scan dirs from last batch minus the cutOff time
      Calendar calendar = Calendar.getInstance(timeZone);
      calendar.setTime(new Date(context.getLastBatchTime()));
      calendar.add(Calendar.MILLISECOND, (int) -cutOffMillis);
      // adding an extra hour to scan
      calendar.add(Calendar.HOUR, -1);

      LOG.info("Looking for uncommitted files from '{}' onwards", calendar.getTime());

      // iterate from last batch time until now at the dir template minimum time precision increments
      // we create a glob for each template time tick
      for (; calendar.compareTo(endCalendar) < 0; calendar.add(timeIncrement, 1)) {
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


}
