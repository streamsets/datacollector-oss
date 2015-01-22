/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.hdfs.writer;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.el.ELRecordSupport;
import com.streamsets.pipeline.hdfs.HdfsFileType;
import com.streamsets.pipeline.hdfs.HdfsLibError;
import com.streamsets.pipeline.lib.recordserialization.RecordToString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;

import javax.servlet.jsp.el.ELException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class RecordWriterManager {
  private URI hdfsUri;;
  private Configuration hdfsConf;
  private String uniquePrefix;
  private String dirPathTemplate;
  private ELEvaluator pathElEval;
  private TimeZone timeZone;
  private long cutOffMillis;
  private long cutOffSize;
  private long cutOffRecords;
  private HdfsFileType fileType;
  private CompressionCodec compressionCodec;
  private SequenceFile.CompressionType compressionType;
  private String keyEL;
  private RecordToString recordToString;

  public RecordWriterManager(URI hdfsUri, Configuration hdfsConf, String uniquePrefix, String dirPathTemplate,
      TimeZone timeZone, long cutOffSecs, long cutOffSize, long cutOffRecords, HdfsFileType fileType,
      CompressionCodec compressionCodec, SequenceFile.CompressionType compressionType, String keyEL,
      RecordToString recordToString) {
    this.hdfsUri = hdfsUri;
    this.hdfsConf = hdfsConf;
    this.uniquePrefix = uniquePrefix;
    this.dirPathTemplate = dirPathTemplate;
    this.timeZone = timeZone;
    this.cutOffMillis = cutOffSecs * 1000;
    this.cutOffSize = cutOffSize;
    this.cutOffRecords = cutOffRecords;
    this.fileType = fileType;
    this.compressionCodec = compressionCodec;
    this.compressionType = compressionType;
    this.keyEL = keyEL;
    this.recordToString = recordToString;
    pathElEval = new ELEvaluator();
    ELRecordSupport.registerRecordFunctions(pathElEval);
  }

  private static final String CONST_YYYY = "YYYY";
  private static final String CONST_YY = "YY";
  private static final String CONST_MM = "MM";
  private static final String CONST_DD = "DD";
  private static final String CONST_hh = "hh";
  private static final String CONST_mm = "mm";
  private static final String CONST_ss = "ss";

  Map<String, Object> getELVarsForTime(Date date) {
    Calendar calendar = Calendar.getInstance(timeZone);
    calendar.setTime(date);
    Map<String, Object> map = new HashMap<>();
    String year = String.format("%04d", calendar.get(Calendar.YEAR));
    map.put(CONST_YYYY, year);
    map.put(CONST_YY, year.substring(year.length() - 2));
    map.put(CONST_MM, String.format("%02d", calendar.get(Calendar.MONTH) + 1));
    map.put(CONST_DD, String.format("%02d", calendar.get(Calendar.DAY_OF_MONTH)));
    map.put(CONST_hh, String.format("%02d", calendar.get(Calendar.HOUR_OF_DAY)));
    map.put(CONST_mm, String.format("%02d", calendar.get(Calendar.MINUTE)));
    map.put(CONST_ss, String.format("%02d", calendar.get(Calendar.SECOND)));
    return map;
  }

  String getDirPath(Date date, Record record) throws StageException {
    try {
      ELEvaluator.Variables vars = new ELEvaluator.Variables(getELVarsForTime(date), null);
      ELRecordSupport.setRecordInContext(vars, record);
      return (String) pathElEval.eval(vars, dirPathTemplate);
    } catch (ELException ex) {
      throw new StageException(HdfsLibError.HDFS_0003, dirPathTemplate, ex.getMessage(), ex);
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
      String name = status[status.length - 1].getPath().getName();
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

  Date getCeilingDateBasedOnTemplate(Date date) {
    Calendar calendar = Calendar.getInstance(timeZone);
    calendar.setTime(date);
    if (!dirPathTemplate.contains(CONST_YY) && !dirPathTemplate.contains(CONST_YYYY)) {
      throw  new IllegalArgumentException("dir path template must have a '${YY}' or '${YYYY}' token");
    }
    boolean done = false;
    if (!dirPathTemplate.contains(CONST_MM)) {
      calendar.set(Calendar.MONTH, calendar.getActualMaximum(Calendar.MONTH));
      calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
      calendar.set(Calendar.HOUR_OF_DAY, calendar.getActualMaximum(Calendar.HOUR_OF_DAY));
      calendar.set(Calendar.MINUTE, calendar.getActualMaximum(Calendar.MINUTE));
      calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));
      calendar.set(Calendar.MILLISECOND, calendar.getActualMaximum(Calendar.MILLISECOND));
      done = true;
    }
    if (!dirPathTemplate.contains(CONST_DD)) {
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
        throw  new IllegalArgumentException("dir path template has the '${DD}' token but does not have the '${MM}' token");
      }
    }
    if (!dirPathTemplate.contains(CONST_hh)) {
      if (!done) {
        calendar.set(Calendar.HOUR_OF_DAY, calendar.getActualMaximum(Calendar.HOUR_OF_DAY));
        calendar.set(Calendar.MINUTE, calendar.getActualMaximum(Calendar.MINUTE));
        calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));
        calendar.set(Calendar.MILLISECOND, calendar.getActualMaximum(Calendar.MILLISECOND));
        done = true;
      }
    } else {
      if (done) {
        throw  new IllegalArgumentException("dir path template has the '${hh}' token but does not have the '${DD}' token");
      }
    }
    if (!dirPathTemplate.contains(CONST_mm)) {
      if (!done) {
        calendar.set(Calendar.MINUTE, calendar.getActualMaximum(Calendar.MINUTE));
        calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));
        calendar.set(Calendar.MILLISECOND, calendar.getActualMaximum(Calendar.MILLISECOND));
        done = true;
      }
    } else {
      if (done) {
        throw  new IllegalArgumentException("dir path template has the '${mm}' token but does not have the '${hh}' token");
      }
    }
    if (!dirPathTemplate.contains(CONST_ss)) {
      if (!done) {
        calendar.set(Calendar.SECOND, calendar.getActualMaximum(Calendar.SECOND));
      }
    } else {
      if (done) {
        throw  new IllegalArgumentException("dir path template has the '${ss}' token but does not have the '${ss}' token");
      }
    }
    calendar.set(Calendar.MILLISECOND, calendar.getActualMaximum(Calendar.MILLISECOND));
    date = calendar.getTime();
    return date;
  }

  long getTimeToLiveMillis(Date now, Date recordDate) {
    // we up the record date to the greatest one based on the template
    recordDate = getCeilingDateBasedOnTemplate(recordDate);
    return recordDate.getTime() + cutOffMillis - now.getTime();
  }

  RecordWriter createWriter(FileSystem fs, Path path, long timeToLiveMillis) throws IOException {
    switch (fileType) {
      case TEXT:
        OutputStream os = fs.create(path, false);
        if (compressionCodec != null) {
          os = compressionCodec.createOutputStream(os);
        }
        return new RecordWriter(path, timeToLiveMillis, os, recordToString);
      case SEQUENCE_FILE:
        Utils.checkNotNull(compressionType, "compressionType");
        Utils.checkNotNull(keyEL, "keyEL");
        Utils.checkArgument(compressionCodec == null || compressionType != SequenceFile.CompressionType.NONE,
                            "if using a compressionCodec, compressionType cannot be NULL");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, hdfsConf, path, Text.class, Text.class,
                                                               compressionType, compressionCodec);
        return new RecordWriter(path, timeToLiveMillis, writer, keyEL, recordToString);
      default:
        throw new UnsupportedOperationException(Utils.format("Unsupported file Type '{}'", fileType));
    }
  }

  public RecordWriter getWriter(Date now, Date recordDate, Record record) throws StageException, IOException {
    RecordWriter writer = null;
    long writerTimeToLive = getTimeToLiveMillis(now, recordDate);
    if (writerTimeToLive > 0) {
      Path tempPath = getPath(recordDate, record);
      FileSystem fs = FileSystem.get(hdfsUri, hdfsConf);
      if (fs.exists(tempPath)) {
        //TODO LOG WARN
        renameTempToNextPart(fs, tempPath);
      }
      writer = createWriter(fs, tempPath, writerTimeToLive);
    }
    return writer;
  }

  public Path commitWriter(RecordWriter writer) throws IOException {
    Path path = null;
    if (!writer.isClosed()) {
      writer.close();
      FileSystem fs = FileSystem.get(hdfsUri, hdfsConf);
      path = renameTempToNextPart(fs, writer.getPath());
    }
    return path;
  }

  public boolean isOverThresholds(RecordWriter writer) throws IOException {
    boolean overLength = (cutOffSize > 0) && writer.getLength() >= cutOffSize;
    boolean overRecords = (cutOffRecords > 0) && writer.getRecords() >= cutOffRecords;
    boolean over = overLength || overRecords;
    if (over) {
      //TODO LOG DEBUG
    }
    return over;
  }

}
