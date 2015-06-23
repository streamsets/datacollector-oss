/**
 * (c) 2014 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.util;

import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.main.RuntimeInfo;

import java.io.File;

public class PipelineDirectoryUtil {

  public static final String PIPELINE_BASE_DIR = "runInfo";
  private static final String SNAPSHOTS_BASE_DIR = "snapshots";

  public static File getPipelineDir(RuntimeInfo runtimeInfo, String pipelineName, String rev) {
    File pipelineDir = new File(new File(new File(runtimeInfo.getDataDir(), PIPELINE_BASE_DIR),
      getEscapedPipelineName(pipelineName)), rev);
    if(!pipelineDir.exists()) {
      if(!pipelineDir.mkdirs()) {
        throw new RuntimeException(Utils.format("Could not create directory '{}'", pipelineDir.getAbsolutePath()));
      }
    }
    return pipelineDir;
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

  public static String getEscapedPipelineName(String pipelineName) {
    return pipelineName.replaceAll(" ", ".");
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
