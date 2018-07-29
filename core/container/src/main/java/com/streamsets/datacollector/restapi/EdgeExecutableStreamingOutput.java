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

package com.streamsets.datacollector.restapi;

import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class EdgeExecutableStreamingOutput implements StreamingOutput {
  private final static String EDGE_DIST_DIR = "/edge-binaries/";
  private final static String PIPELINE_JSON_FILE = "pipeline.json";
  private final static String PIPELINE_INFO_FILE = "info.json";
  private final static String DATA_PIPELINES_FOLDER = "streamsets-datacollector-edge/data/pipelines/";
  private final static String TAR_FILE_PREFIX = "streamsets-datacollector-edge-";

  private final File edgeTarFile;
  private final List<PipelineConfigurationJson> pipelineConfigurationList;

  EdgeExecutableStreamingOutput(
      RuntimeInfo runtimeInfo,
      String edgeOs,
      String edgeArch,
      List<PipelineConfigurationJson> pipelineConfigurationList
  ) {
    String endsWith = "-" + edgeOs + "-" + edgeArch + ".tgz";
    File dir = new File(runtimeInfo.getRuntimeDir() + EDGE_DIST_DIR);
    File[] files = dir.listFiles((dir1, name) -> name.startsWith(TAR_FILE_PREFIX) && name.endsWith(endsWith));

    if (files == null || files.length != 1) {
      throw new RuntimeException("Executable file not found");
    }

    this.edgeTarFile = files[0];
    this.pipelineConfigurationList = pipelineConfigurationList;
  }

  @Override
  public void write(OutputStream output) throws IOException, WebApplicationException {
    try (
        TarArchiveOutputStream tarArchiveOutput = new TarArchiveOutputStream(new GzipCompressorOutputStream(output));
        TarArchiveInputStream tarArchiveInput = new TarArchiveInputStream(
            new GzipCompressorInputStream(new FileInputStream(edgeTarFile))
        )
    ) {
      tarArchiveOutput.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);
      TarArchiveEntry entry = tarArchiveInput.getNextTarEntry();

      while (entry != null) {
        tarArchiveOutput.putArchiveEntry(entry);
        IOUtils.copy(tarArchiveInput, tarArchiveOutput);
        tarArchiveOutput.closeArchiveEntry();
        entry = tarArchiveInput.getNextTarEntry();
      }

      for (PipelineConfigurationJson pipelineConfiguration: pipelineConfigurationList) {
        addArchiveEntry(
            tarArchiveOutput,
            pipelineConfiguration,
            pipelineConfiguration.getPipelineId(),
            PIPELINE_JSON_FILE
        );
        addArchiveEntry(
            tarArchiveOutput,
            pipelineConfiguration.getInfo(),
            pipelineConfiguration.getPipelineId(),
            PIPELINE_INFO_FILE
        );
      }

      tarArchiveOutput.finish();
    }
  }

  public String getFileName() {
    return this.edgeTarFile.getName();
  }

  private void addArchiveEntry(
      TarArchiveOutputStream tarArchiveOutput,
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
    tarArchiveOutput.putArchiveEntry(archiveEntry);
    IOUtils.copy(new FileInputStream(pipelineFile), tarArchiveOutput);
    tarArchiveOutput.closeArchiveEntry();
  }
}
