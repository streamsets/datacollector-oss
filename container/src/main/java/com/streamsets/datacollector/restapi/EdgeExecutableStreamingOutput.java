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

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.PipelineConfigurationJson;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class EdgeExecutableStreamingOutput implements StreamingOutput {
  private final static String EDGE_DIST_DIR = "/edge-binaries/";
  private final static String TAR_FILE_PREFIX = "streamsets-datacollector-edge-";

  private final File edgeTarFile;
  private final List<PipelineConfigurationJson> pipelineConfigurationList;
  private final EdgeArchiveType edgeArchiveType;

  EdgeExecutableStreamingOutput(
      RuntimeInfo runtimeInfo,
      String edgeOs,
      String edgeArch,
      List<PipelineConfigurationJson> pipelineConfigurationList
  ) {
    if(edgeOs.equalsIgnoreCase("windows")) {
      edgeArchiveType = EdgeArchiveType.ZIP;
    } else {
      edgeArchiveType = EdgeArchiveType.TAR;
    }
    String endsWith = "-" + edgeOs + "-" + edgeArch + edgeArchiveType.getFileSuffix();
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
    EdgeArchiveBuilder edgeArchiveBuilder;

    switch (edgeArchiveType) {
      case TAR:
        edgeArchiveBuilder = new TarEdgeArchiveBuilder();
        break;
      case ZIP:
        edgeArchiveBuilder = new ZipEdgeArchiveBuilder();
        break;
      default:
        throw new IllegalArgumentException("Unsupported edge archive type: " + edgeArchiveType.name());
    }

    edgeArchiveBuilder
        .archive(edgeTarFile)
        .to(output)
        .with(pipelineConfigurationList)
        .finish();
  }

  public String getFileName() {
    return this.edgeTarFile.getName();
  }
}
