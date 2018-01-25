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
package com.streamsets.pipeline.stage.destination.recordstolocalfilesystem;

import com.google.common.io.CountingOutputStream;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class RecordsToLocalFileSystemTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(RecordsToLocalFileSystemTarget.class);
  private static final String CHARSET_UTF8 = "UTF-8";

  private final String directory;
  private final String uniquePrefix;
  private final String rotationIntervalSecsExpr;
  private final int maxFileSizeMbs;

  private File dir;
  private long rotationMillis;
  private long maxFileSizeBytes;
  private long lastRotation;
  private File activeFile;
  private CountingOutputStream countingOutputStream;
  private DataGeneratorFactory generatorFactory;
  private DataGenerator generator;

  public RecordsToLocalFileSystemTarget(String directory, String uniquePrefix, String rotationIntervalSecs,
                                        int maxFileSizeMbs) {
    this.directory = directory;
    this.uniquePrefix = (uniquePrefix == null) ? "" : uniquePrefix;
    this.rotationIntervalSecsExpr = rotationIntervalSecs;
    this.maxFileSizeMbs = maxFileSizeMbs;
  }

  private ELEval createRotationIntervalSecsEval(ELContext elContext) {
    return elContext.createELEval("rotationIntervalSecs");
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues =  super.init();

    dir = new File(directory);
    if (!dir.exists()) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "directory", Errors.RECORDFS_01, directory));
    } else{
      if (!dir.isDirectory()) {
        issues.add(getContext().createConfigIssue(Groups.FILES.name(), "directory", Errors.RECORDFS_02, directory));
      }
    }
    try {
      ELEval rotationIntervalSecsEvaluator = createRotationIntervalSecsEval(getContext());
      getContext().parseEL(rotationIntervalSecsExpr);
      long rotationIntervalSecs = rotationIntervalSecsEvaluator.eval(getContext().createELVars(), rotationIntervalSecsExpr, Long.class);
      rotationMillis = rotationIntervalSecs * 1000;
      if (rotationMillis <= 0) {
        issues.add(getContext().createConfigIssue(Groups.FILES.name(), "rotationIntervalSecs", Errors.RECORDFS_03,
            rotationIntervalSecsExpr, rotationIntervalSecs));
      }
    } catch (ELEvalException ex) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "rotationIntervalSecs", Errors.RECORDFS_04,
          rotationIntervalSecsExpr));
    }
    if (maxFileSizeMbs < 0) {
      issues.add(getContext().createConfigIssue(Groups.FILES.name(), "maxFileSizeMbs", Errors.RECORDFS_00,
                                                maxFileSizeMbs));
    }
    maxFileSizeBytes = maxFileSizeMbs * 1024L * 1024L;

    activeFile = new File(dir, "_tmp_" + uniquePrefix + ".sdc").getAbsoluteFile();

    generatorFactory = new DataGeneratorFactoryBuilder(getContext(), DataGeneratorFormat.SDC_RECORD)
        .setCharset(Charset.forName(CHARSET_UTF8)).build();

    if (issues.isEmpty()) {
      try {
        // if we had non graceful shutdown we may have a _tmp file around. new file is not created.
        rotate(false);
      } catch (IOException ex) {
        LOG.warn("Could not do rotation on init(): {}", ex.toString(), ex);
        issues.add(getContext().createConfigIssue(null, null, Errors.RECORDFS_06, activeFile, ex.toString()));
      }
    }
    return issues;
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
      throw new StageException(Errors.RECORDFS_05, activeFile, ex.toString(), ex);
    }
  }

  private boolean hasToRotate() {
    return System.currentTimeMillis() - lastRotation > rotationMillis ||
           (countingOutputStream != null && countingOutputStream.getCount() > maxFileSizeBytes);
  }

  private File findFinalName() throws IOException {
    return new File(dir, uniquePrefix + "_" + UUID.randomUUID().toString() + ".sdc").getAbsoluteFile();
  }

  private void rotate(boolean createNewFile) throws IOException {
    OutputStream outputStream = null;
    try {
      IOUtils.closeQuietly(generator);
      generator = null;
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
        generator = generatorFactory.getGenerator(outputStream);
      }
      lastRotation = System.currentTimeMillis();
    } catch (IOException ex) {
      IOUtils.closeQuietly(generator);
      generator = null;
      IOUtils.closeQuietly(countingOutputStream);
      countingOutputStream = null;
      IOUtils.closeQuietly(outputStream);
      throw ex;
    }
  }

  @Override
  public void destroy() {
    try {
      //closing file and rotating.
      rotate(false);
    } catch (IOException ex) {
      LOG.warn("Could not do rotation on destroy(): {}", ex.toString(), ex);
    }
    IOUtils.closeQuietly(generator);
    IOUtils.closeQuietly(countingOutputStream);
    super.destroy();
  }

}
