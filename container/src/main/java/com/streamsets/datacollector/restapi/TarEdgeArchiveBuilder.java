/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import static com.streamsets.datacollector.restapi.PipelineInfoConstants.DATA_PIPELINES_FOLDER;
import static com.streamsets.datacollector.restapi.PipelineInfoConstants.PIPELINE_INFO_FILE;
import static com.streamsets.datacollector.restapi.PipelineInfoConstants.PIPELINE_JSON_FILE;

public class TarEdgeArchiveBuilder extends EdgeArchiveBuilder {
  @Override
  public void finish() throws IOException {
    try (
        TarArchiveOutputStream tarArchiveOutput = new TarArchiveOutputStream(new GzipCompressorOutputStream(outputStream));
        TarArchiveInputStream tarArchiveInput = new TarArchiveInputStream(new GzipCompressorInputStream(new FileInputStream(edgeArchive)))
    ) {
      tarArchiveOutput.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      TarArchiveEntry entry = tarArchiveInput.getNextTarEntry();

      while (entry != null) {
        tarArchiveOutput.putArchiveEntry(entry);
        IOUtils.copy(tarArchiveInput, tarArchiveOutput);
        tarArchiveOutput.closeArchiveEntry();
        entry = tarArchiveInput.getNextTarEntry();
      }

      for (PipelineConfigurationJson pipelineConfiguration : pipelineConfigurationList) {
        addArchiveEntry(tarArchiveOutput,
            pipelineConfiguration,
            pipelineConfiguration.getPipelineId(),
            PIPELINE_JSON_FILE
        );
        addArchiveEntry(tarArchiveOutput,
            pipelineConfiguration.getInfo(),
            pipelineConfiguration.getPipelineId(),
            PIPELINE_INFO_FILE
        );
      }

      tarArchiveOutput.finish();
    }
  }

  protected void addArchiveEntry(
      ArchiveOutputStream archiveOutput,
      Object fileContent,
      String pipelineId,
      String fileName
  ) throws IOException {
    File pipelineFile = File.createTempFile(pipelineId, fileName);
    FileOutputStream pipelineOutputStream = new FileOutputStream(pipelineFile);
    ObjectMapperFactory.get().writeValue(pipelineOutputStream, fileContent);
    pipelineOutputStream.flush();
    pipelineOutputStream.close();
    TarArchiveEntry archiveEntry = new TarArchiveEntry(
        pipelineFile,
        DATA_PIPELINES_FOLDER + pipelineId + "/" + fileName
    );
    archiveEntry.setSize(pipelineFile.length());
    archiveOutput.putArchiveEntry(archiveEntry);
    IOUtils.copy(new FileInputStream(pipelineFile), archiveOutput);
    archiveOutput.closeArchiveEntry();
  }
}
