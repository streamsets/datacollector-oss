/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.hdfs.writer;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
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
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

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

  public Path getPath(Date recordDate, Record record) throws StageException {
    return new Path(getDirPath(recordDate, record), "_" + uniquePrefix + "_tmp" + getExtension());
  }

  Path renameTempToNextPart(FileSystem fs, Path tempPath) throws IOException {
    Path parent = tempPath.getParent();
    FileStatus[] status = fs.globStatus(new Path(parent, uniquePrefix + "-[0-9][0-9][0-9][0-9][0-9][0-9]" +
                                                         getExtension()));
    int count = 0;
    if (status.length > 0) {
      List<FileStatus> list = new ArrayList<>(status.length);
      Collections.addAll(list, status);

      Collections.sort(list);
      String name = list.get(list.size() - 1).getPath().getName();
      String countStr = name.substring(uniquePrefix.length() + 1, uniquePrefix.length() + 1 + 6);
      count = Integer.parseInt(countStr) + 1;
    }
    Path finalPath = new Path(parent, String.format("%s-%06d%s", uniquePrefix, count, getExtension()));
    if (!fs.rename(tempPath, finalPath)) {
      throw new IOException(Utils.format("Could not rename '{}' to '{}'", tempPath, finalPath));
    }
    return finalPath;
  }

  private final static Set<String> TIME_CONSTANTS = new HashSet<>();
  static {
    TIME_CONSTANTS.add(CONST_YYYY);
    TIME_CONSTANTS.add(CONST_YY);
    TIME_CONSTANTS.add(CONST_MM);
    TIME_CONSTANTS.add(CONST_DD);
    TIME_CONSTANTS.add(CONST_hh);
    TIME_CONSTANTS.add(CONST_mm);
    TIME_CONSTANTS.add(CONST_ss);
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
        Path path = renameTempToNextPart(fs, tempPath);
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
      path = renameTempToNextPart(fs, writer.getPath());
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

}
