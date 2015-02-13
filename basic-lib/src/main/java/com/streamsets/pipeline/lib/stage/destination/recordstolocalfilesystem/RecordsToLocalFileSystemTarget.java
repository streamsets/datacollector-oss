/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.lib.stage.destination.recordstolocalfilesystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.io.CountingOutputStream;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.impl.ContextExt;
import com.streamsets.pipeline.api.impl.JsonRecordWriter;
import com.streamsets.pipeline.el.ELEvaluator;
import com.streamsets.pipeline.lib.io.WildcardFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.jsp.el.ELException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

@GenerateResourceBundle
@StageDef(
    version = "1.0.0",
    label = "Records to File",
    description = "Writes Data Collector records to the local File System",
    icon="localfilesystem.png",
    requiredFields = false
)
@ErrorStage
@ConfigGroups(RecordsToLocalFileSystemTarget.Groups.class)
public class RecordsToLocalFileSystemTarget extends BaseTarget {
  private final static Logger LOG = LoggerFactory.getLogger(RecordsToLocalFileSystemTarget.class);

  public enum Groups implements Label {
    FILES;

    @Override
    public String getLabel() {
      return "Files";
    }
  }

  public enum Error implements ErrorCode {
    RECORD_LOCAL_FS_001("Directory '{}' does not exist"),
    RECORD_LOCAL_FS_002("Path '{}' is not a directory"),
    RECORD_LOCAL_FS_003("Rotation interval '{}' must be greater than zero, it is '{}'"),
    RECORD_LOCAL_FS_004("Rotation interval '{}' is not a valid expression"),
    RECORD_LOCAL_FS_005("Could not write record to file '{}', error: {}"),
    RECORD_LOCAL_FS_006("Could not rotate file '{}', error: {}"),
    RECORD_LOCAL_FS_007("Could not rotate file '{}', error: {}"),
    RECORD_LOCAL_FS_008("Max file size '{}' must be zero or greater"),
    ;

    private final String msg;
    Error(String msg) {
      this.msg = msg;
    }

    @Override
    public String getCode() {
      return name();
    }

    @Override
    public String getMessage() {
      return msg;
    }

  }

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Directory",
      description = "Directory to write records",
      displayPosition = 10,
      group = "FILES"
  )
  public String directory;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.EL_NUMBER,
      defaultValue = "${1 * HOURS}",
      label = "File Wait Time (secs)",
      description = "Max time to wait for error records before creating a new error file. \n" +
                    "Enter the time in seconds or use the default expression to enter the time limit in minutes. " +
                    "You can also use HOURS in the expression to enter the limit in hours.",
      displayPosition = 20,
      group = "FILES"
  )
  public String rotationIntervalSecs;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.INTEGER,
      defaultValue = "512",
      label = "Max File Size (MB)",
      description = "Max file size to trigger the creation of a new file. Use 0 to opt out.",
      displayPosition = 30,
      group = "FILES"
  )
  public int maxFileSizeMbs;

  private ObjectWriter jsonWriter;
  private File dir;
  private long rotationMillis;
  private int maxFileSizeBytes;
  private long lastRotation;
  private DirectoryStream.Filter<Path> fileFilter;
  private File activeFile;
  private CountingOutputStream countingOutputStream;
  private JsonRecordWriter writer;

  @Override
  protected List<ConfigIssue> validateConfigs() {
    List<ConfigIssue> issues =  super.validateConfigs();

    dir = new File(directory);
    if (!dir.exists()) {
      issues.add(getContext().createConfigIssue(Error.RECORD_LOCAL_FS_001, directory));
    }
    if (!dir.isDirectory()) {
      issues.add(getContext().createConfigIssue(Error.RECORD_LOCAL_FS_002, directory));
    }
    try {
      rotationMillis = ELEvaluator.evaluateHoursMinutesToSecondsExpr(rotationIntervalSecs) * 1000;
      if (rotationMillis <= 0) {
        issues.add(getContext().createConfigIssue(Error.RECORD_LOCAL_FS_003, rotationIntervalSecs, rotationMillis / 1000));
      }
    } catch (ELException ex) {
      issues.add(getContext().createConfigIssue(Error.RECORD_LOCAL_FS_004, rotationIntervalSecs));
    }
    if (maxFileSizeMbs < 0) {
      issues.add(getContext().createConfigIssue(Error.RECORD_LOCAL_FS_008, maxFileSizeMbs));
    }
    maxFileSizeBytes = maxFileSizeMbs * 1024 * 1024;
    return issues;
  }

  @Override
  protected void init() throws StageException {
    super.init();
    jsonWriter = new ObjectMapper().writer();
    activeFile = new File(dir, "_tmp_records.json").getAbsoluteFile();
    fileFilter = WildcardFilter.createRegex("records-[0-9][0-9][0-9][0-9][0-9][0-9].json");
    // if we had non graceful shutdown we may have a _tmp file around. new file is not created.
    rotate(false);
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> it = batch.getRecords();
    try {
      while (it.hasNext()) {
        if (writer == null || hasToRotate()) {
          //rotating file because of rotation interval or size limit. creates new file as we need to write records
          //or we don't have a writer and need to create one
          rotate(true);
        }
        writer.write(it.next());
      }
      if (writer != null) {
        writer.flush();
      }
      if (hasToRotate()) {
        // rotating file because of rotation interval in case of empty batches. new file is not created.
        rotate(false);
      }
    } catch (IOException ex) {
      throw new StageException(Error.RECORD_LOCAL_FS_005, activeFile, ex.getMessage(), ex);
    }
  }

  private boolean hasToRotate() {
    return System.currentTimeMillis() - lastRotation > rotationMillis ||
           (countingOutputStream != null && countingOutputStream.getCount() > maxFileSizeBytes);
  }

  private File findFinalName() throws StageException, IOException {
    String latest = null;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir.toPath(), fileFilter)) {
      for (Path file : stream) {
        String name = file.getFileName().toString();
        if (latest == null) {
          latest = name;
        }
        if (name.compareTo(latest) > 0) {
          latest = name;
        }
      }
    }
    if (latest == null) {
      latest = "records-000000.json";
    } else {
      String countStr = latest.substring("records-".length(), "records-".length() + 6);
      try {
        int count = Integer.parseInt(countStr) + 1;
        latest = String.format("records-%06d.json", count);
      } catch (NumberFormatException ex) {
        throw new StageException(Error.RECORD_LOCAL_FS_007, latest, ex.getMessage(), ex);
      }
    }
    return new File(dir, latest).getAbsoluteFile();
  }

  private void rotate(boolean createNewFile) throws StageException {
    try {
      if (writer != null) {
        writer.close();
        writer = null;
      }
      if (activeFile.exists()) {
        File finalName = findFinalName();
        LOG.debug("Rotating '{}' to '{}'", activeFile, finalName);
        Files.move(activeFile.toPath(), finalName.toPath());
      }
      if (createNewFile) {
        LOG.debug("Creating new '{}'", activeFile);
        OutputStream outputStream = new FileOutputStream(activeFile);
        if (maxFileSizeBytes > 0) {
          countingOutputStream = new CountingOutputStream(outputStream);
          outputStream = countingOutputStream;
        }
        writer = ((ContextExt)getContext()).createJsonRecordWriter(new OutputStreamWriter(outputStream));
      }
      lastRotation = System.currentTimeMillis();
    } catch (IOException ex) {
      if (writer != null) {
        writer.close();
      }
      throw new StageException(Error.RECORD_LOCAL_FS_006, activeFile, ex.getMessage(), ex);
    }
  }

  @Override
  public void destroy() {
    try {
      //closing file and rotating.
      rotate(false);
    } catch (StageException ex) {
      LOG.warn("Could not do rotation on destroy: {}", ex.getMessage(), ex);
    }
    super.destroy();
  }

}
