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
package com.streamsets.pipeline.stage.destination.fifo;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Iterator;
import java.util.List;

public class FifoTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(FifoTarget.class);
  private String namedPipe;
  private DataFormat dataFormat;
  private FileOutputStream fos;
  private DataGeneratorFactory generatorFactory;
  private DataGeneratorFormatConfig dataGeneratorFormatConfig;

  public FifoTarget(String namedPipe, DataFormat dataFormat, DataGeneratorFormatConfig dataGeneratorFormatConfig) {
    this.namedPipe = namedPipe;
    this.dataFormat = dataFormat;
    this.dataGeneratorFormatConfig = dataGeneratorFormatConfig;
  }

  @Override
  protected List<ConfigIssue> init() {
    final List<ConfigIssue> issues = super.init();

    if (StringUtils.isEmpty(namedPipe)) {
      issues.add(getContext().createConfigIssue(Groups.NAMED_PIPE.name(), "namedPipe", Errors.NAMED_PIPE_01));
    }

    try {
      if (!Files.readAttributes(Paths.get(namedPipe), BasicFileAttributes.class).isOther()) {
        LOG.error(Errors.NAMED_PIPE_06.getMessage(), namedPipe);
        issues.add(getContext().createConfigIssue(
            Groups.NAMED_PIPE.name(),
            "namedPipe",
            Errors.NAMED_PIPE_06,
            namedPipe
        ));
      }
    } catch (IOException ex) {
      LOG.error(Errors.NAMED_PIPE_05.getMessage(), ex);
      issues.add(getContext().createConfigIssue(Groups.NAMED_PIPE.name(), "namedPipe", Errors.NAMED_PIPE_05, ex));
    }

    if(!issues.isEmpty()) {
      return issues;
    }

    if(!getContext().isPreview()) {
      try {
        fos = new FileOutputStream(namedPipe);
      } catch (IOException ex) {
        issues.add(getContext().createConfigIssue(Groups.NAMED_PIPE.name(), "namedPipe", Errors.NAMED_PIPE_02, ex));
        return issues;
      }
    }

    if (issues.isEmpty()) {
      dataGeneratorFormatConfig.init(getContext(), dataFormat, Groups.NAMED_PIPE.name(), "Data Format", issues);
      generatorFactory = dataGeneratorFormatConfig.getDataGeneratorFactory();
    }

    return issues;
  }

  @Override
  public void write(final Batch batch) throws StageException {

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (DataGenerator dataGenerator = generatorFactory.getGenerator(baos)) {
        Iterator<Record> records = batch.getRecords();
        while (records.hasNext()) {
          Record record = records.next();
          dataGenerator.write(record);
        }
        dataGenerator.flush();
      } catch (DataGeneratorException e) {
        LOG.error(Errors.NAMED_PIPE_03.getMessage(), e.getMessage(), e);
        throw new IOException(e);
      }

      if (!getContext().isPreview()) {
        fos.write(baos.toByteArray());
      }
    } catch (IOException ex) {
      LOG.error(Errors.NAMED_PIPE_04.getMessage(), ex.getMessage(), ex);
      throw new StageException(Errors.NAMED_PIPE_04, ex.getMessage(), ex);
    }
  }

  @Override
  public void destroy() {
    try {
      if (fos != null) {
        fos.close();
        fos = null;
      }
    } catch (IOException ex) {
      LOG.error("destroy(): IOException closing connection: '{}' ", ex.toString(), ex);
    }
  }
}
