/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.destination.recordstolocalfilesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.io.CountingOutputStream;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.generator.CharDataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.io.WildcardFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class RecordsToLocalFileSystemTarget extends BaseTarget {
  private final static Logger LOG = LoggerFactory.getLogger(RecordsToLocalFileSystemTarget.class);

  private final String directory;
  private final String rotationIntervalSecs;
  private final int maxFileSizeMbs;

  public RecordsToLocalFileSystemTarget(String directory, String rotationIntervalSecs, int maxFileSizeMbs) {
    this.directory = directory;
    this.rotationIntervalSecs = rotationIntervalSecs;
    this.maxFileSizeMbs = maxFileSizeMbs;
  }

  private File dir;
  private long rotationMillis;
  private int maxFileSizeBytes;
  private long lastRotation;
  private DirectoryStream.Filter<Path> fileFilter;
  private File activeFile;
  private CountingOutputStream countingOutputStream;
  private CharDataGeneratorFactory generatorFactory;
  private DataGenerator generator;
  private ELEval rotationMillisEvaluator;

  @Override
  public List<ELEval> getELEvals(ELContext elContext) {
    return ImmutableList.of(createRotationMillisEval(elContext));
  }

  private ELEval createRotationMillisEval(ELContext elContext) {
    return elContext.createELEval("rotationIntervalSecs", TimeEL.class);
  }

  @Override
  protected List<ConfigIssue> validateConfigs() throws StageException {
    List<ConfigIssue> issues =  super.validateConfigs();

    dir = new File(directory);
    if (!dir.exists()) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "directory", Errors.RECORDFS_01, directory));
    } else{
      if (!dir.isDirectory()) {
        issues.add(getContext().createConfigIssue(Groups.FILES.name(), "directory", Errors.RECORDFS_02, directory));
      }
    }
    try {
      rotationMillisEvaluator = createRotationMillisEval(getContext());
      getContext().parseEL(rotationIntervalSecs);
      rotationMillis = rotationMillisEvaluator.eval(getContext().createELVars(), rotationIntervalSecs, Long.class);
      if (rotationMillis <= 0) {
        issues.add(getContext().createConfigIssue(Groups.FILES.name(), "rotationIntervalSecs", Errors.RECORDFS_03,
                                                  rotationIntervalSecs, rotationMillis / 1000));
      }
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "rotationIntervalSecs", Errors.RECORDFS_04,
                                                rotationIntervalSecs));
    }
    if (maxFileSizeMbs < 0) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "maxFileSizeMbs", Errors.RECORDFS_00,
                                                maxFileSizeMbs));
    }
    maxFileSizeBytes = maxFileSizeMbs * 1024 * 1024;
    return issues;
  }

  @Override
  protected void init() throws StageException {
    super.init();
    activeFile = new File(dir, "_tmp_records.json").getAbsoluteFile();
    fileFilter = WildcardFilter.createRegex("records-[0-9][0-9][0-9][0-9][0-9][0-9].json");
    // if we had non graceful shutdown we may have a _tmp file around. new file is not created.
    rotate(false);
    generatorFactory = new CharDataGeneratorFactory.Builder(getContext(),
                                                            CharDataGeneratorFactory.Format.SDC_RECORD).build();
  }

  @Override
  public void write(Batch batch) throws StageException {
    Iterator<Record> it = batch.getRecords();
    try {
      while (it.hasNext()) {
        if (generator == null || hasToRotate()) {
          //rotating file because of rotation interval or size limit. creates new file as we need to write records
          //or we don't have a writer and need to create one
          rotate(true);
        }
        generator.write(it.next());
      }
      if (generator != null) {
        generator.flush();
      }
      if (hasToRotate()) {
        // rotating file because of rotation interval in case of empty batches. new file is not created.
        rotate(false);
      }
    } catch (IOException ex) {
      throw new StageException(Errors.RECORDFS_05, activeFile, ex.getMessage(), ex);
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
        throw new StageException(Errors.RECORDFS_07, latest, ex.getMessage(), ex);
      }
    }
    return new File(dir, latest).getAbsoluteFile();
  }

  private void rotate(boolean createNewFile) throws StageException {
    OutputStream outputStream = null;
    try {
      if (generator != null) {
        generator.close();
        generator = null;
      }
      if (activeFile.exists()) {
        File finalName = findFinalName();
        LOG.debug("Rotating '{}' to '{}'", activeFile, finalName);
        Files.move(activeFile.toPath(), finalName.toPath());
      }
      if (createNewFile) {
        LOG.debug("Creating new '{}'", activeFile);
        outputStream = new FileOutputStream(activeFile);
        if (maxFileSizeBytes > 0) {
          countingOutputStream = new CountingOutputStream(outputStream);
          outputStream = countingOutputStream;
        }
        generator = generatorFactory.getGenerator(new OutputStreamWriter(outputStream));
      }
      lastRotation = System.currentTimeMillis();
    } catch (IOException ex) {
      if (generator != null) {
        try {
          generator.close();
        } catch (IOException ex1) {
          //NOP
        }
        generator = null;
      } else {
        if (outputStream != null) {
          try {
            outputStream.close();
          } catch (IOException ex1) {
            //NOP
          }
        }
      }
      throw new StageException(Errors.RECORDFS_06, activeFile, ex.getMessage(), ex);
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
