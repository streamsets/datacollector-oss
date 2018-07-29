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
package com.streamsets.datacollector.util;

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.impl.PipelineUtils;
import com.streamsets.pipeline.api.impl.Utils;

import java.io.File;

public class PipelineDirectoryUtil {

  public static final String PIPELINE_BASE_DIR = "runInfo";
  public static final String PIPELINE_INFO_BASE_DIR = "pipelines";

  private static final String SNAPSHOTS_BASE_DIR = "snapshots";

  private PipelineDirectoryUtil() {}

  public static File getPipelineDir(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
    return new File(new File(new File(runtimeInfo.getDataDir(), PIPELINE_BASE_DIR),
      PipelineUtils.escapedPipelineName(pipelineName)), rev);
  }

  public static boolean deletePipelineDir(RuntimeInfo runtimeInfo, String pipelineName) {
    File pipelineDir = new File(new File(runtimeInfo.getDataDir(), PIPELINE_BASE_DIR),
      PipelineUtils.escapedPipelineName(pipelineName));
    boolean deleted = true;
    if(pipelineDir.exists()) {
      deleted = deleteAll(pipelineDir);
    }
    return deleted;
  }

  public static File getPipelineSnapshotDir(RuntimeInfo runtimeInfo, String pipelineName, String rev,
                                            String snapshotName) {
    File pipelineDir = getPipelineDir(runtimeInfo, pipelineName, rev);
    File snapshotsBaseDir = new File(pipelineDir, SNAPSHOTS_BASE_DIR);
    File snapshotDir = new File(snapshotsBaseDir, snapshotName);
    return snapshotDir;
  }

  public static File getPipelineSnapshotBaseDir(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
    File pipelineDir = getPipelineDir(runtimeInfo, pipelineName, rev);
    File snapshotsBaseDir = new File(pipelineDir, SNAPSHOTS_BASE_DIR);

    return snapshotsBaseDir;
  }


  public static void createPipelineSnapshotDir(RuntimeInfo runtimeInfo, String pipelineName,
                                               String rev, String snapshotName) {
    File pipelineDir = getPipelineDir(runtimeInfo, pipelineName, rev);
    File snapshotsBaseDir = new File(pipelineDir, SNAPSHOTS_BASE_DIR);

    if(!snapshotsBaseDir.exists()) {
      if(!snapshotsBaseDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", snapshotsBaseDir.getAbsolutePath()));
      }
    }

    File snapshotDir = new File(snapshotsBaseDir, snapshotName);
    if(!snapshotDir.exists()) {
      if(!snapshotDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", snapshotDir.getAbsolutePath()));
      }
    }
  }

  public static boolean deleteAll(File path) {
    boolean ok = true;
    File[] children = path.listFiles();
    if (children != null) {
      for (File child : children) {
        ok = deleteAll(child);
        if (!ok) {
          break;
        }
      }
    }
    return ok && path.delete();
  }

}
