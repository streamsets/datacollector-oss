/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.dc.execution.snapshot.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.dc.execution.SnapshotInfo;
import com.streamsets.dc.execution.Snapshot;
import com.streamsets.dc.execution.SnapshotStore;
import com.streamsets.dc.execution.snapshot.common.SnapshotData;
import com.streamsets.dc.execution.snapshot.common.SnapshotImpl;
import com.streamsets.dc.execution.snapshot.common.SnapshotInfoImpl;
import com.streamsets.dc.restapi.bean.SnapshotDataJson;
import com.streamsets.dc.restapi.bean.SnapshotInfoJson;
import com.streamsets.pipeline.io.DataStore;
import com.streamsets.pipeline.json.ObjectMapperFactory;
import com.streamsets.pipeline.main.RuntimeInfo;
import com.streamsets.pipeline.runner.PipelineRuntimeException;
import com.streamsets.pipeline.runner.StageOutput;
import com.streamsets.pipeline.store.PipelineStoreException;
import com.streamsets.pipeline.util.ContainerError;
import com.streamsets.pipeline.util.PipelineDirectoryUtil;
import com.streamsets.pipeline.util.PipelineException;
import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileSnapshotStore implements SnapshotStore {
  private static final String SNAPSHOT_FILE_NAME = "snapshot.json";
  private static final String INFO_FILE_NAME = "info.json";

  private final RuntimeInfo runtimeInfo;
  private final ObjectMapper json;

  @Inject
  public FileSnapshotStore(RuntimeInfo runtimeInfo) {
    this.runtimeInfo = runtimeInfo;
    json = ObjectMapperFactory.get();
  }

  @Override
  public SnapshotInfo create(String user, String name, String rev, String id) throws PipelineException {
    PipelineDirectoryUtil.createPipelineSnapshotDir(runtimeInfo, name, rev, id);
    SnapshotInfo snapshotInfo = new SnapshotInfoImpl(user, id, name, rev, System.currentTimeMillis(), true);
    persistSnapshotInfo(snapshotInfo);
    return snapshotInfo;
  }

  @Override
  public SnapshotInfo save(String name, String rev, String id, List<List<StageOutput>> snapshotBatches)
    throws PipelineException {
    SnapshotInfo existingInfo = getInfo(name, rev, id);
    if(existingInfo == null) {
      throw new PipelineException(ContainerError.CONTAINER_0605);
    }
    persistSnapshot(name, rev, id, snapshotBatches);
    SnapshotInfo updatedSnapshotInfo = new SnapshotInfoImpl(existingInfo.getUser(), id, name, rev,
      System.currentTimeMillis(), false);
    persistSnapshotInfo(updatedSnapshotInfo);
    return updatedSnapshotInfo;
  }

  @Override
  public Snapshot get(String name, String rev, String id) throws PipelineException {
    SnapshotInfo info = getInfo(name, rev, id);
    File data = getData(name, rev, id);
    return new SnapshotImpl(info, data);
  }

  @Override
  public List<SnapshotInfo> getSummaryForPipeline(String name, String rev) throws PipelineException {
    List<SnapshotInfo> list = new ArrayList<>();
    File snapshotDir = PipelineDirectoryUtil.getPipelineSnapshotBaseDir(runtimeInfo, name, rev);
    if(snapshotDir.exists()) {
      for (String snapshotName : snapshotDir.list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          //If one browses to the pipelines directory, mac creates a ".DS_store directory and this causes a problem
          //So filter it out
          return !name.startsWith(".");
        }
      })) {
        SnapshotInfo snapshotInfo = getInfo(name, rev, snapshotName);
        if(snapshotInfo != null) {
          list.add(snapshotInfo);
        }
      }
    }
    return Collections.unmodifiableList(list);
  }

  @Override
  public void deleteSnapshot(String name, String rev, String id) throws PipelineException {
    File snapshotBaseDir = PipelineDirectoryUtil.getPipelineSnapshotDir(runtimeInfo, name, rev, id);
    if(snapshotBaseDir.exists()) {
      if (!PipelineDirectoryUtil.deleteAll(snapshotBaseDir)) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0601);
      }
    }
  }

  @Override
  public  SnapshotInfo getInfo(String name, String rev, String id) throws PipelineException {
    try {
      File infoFile = getPipelineSnapshotInfoFile(name, rev, id);
      if(infoFile.exists()) {
        try (InputStream in = new FileInputStream(infoFile)) {
          SnapshotInfoJson snapshotInfoJsonBean =json.readValue(in, SnapshotInfoJson.class);
          return snapshotInfoJsonBean.getSnapshotInfo();
        } catch (IOException e) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0101, e.getMessage(), e);
        }
      } else {
        return null;
      }
    } catch (Exception e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0604, id, name, rev, e.getMessage(), e);
    }
  }

  File getData(String name, String rev, String id) throws PipelineRuntimeException {
    File dataFile = getPipelineSnapshotFile(name, rev, id);
    if(dataFile.exists()) {
      return dataFile;
    } else {
      return null;
    }
  }

  private File getPipelineSnapshotFile(String pipelineName, String rev, String snapshotName) {
    return new File(PipelineDirectoryUtil.getPipelineSnapshotDir(runtimeInfo, pipelineName, rev, snapshotName),
      SNAPSHOT_FILE_NAME);
  }

  private File getPipelineSnapshotInfoFile(String name, String rev, String id) {
    return new File(PipelineDirectoryUtil.getPipelineSnapshotDir(runtimeInfo, name, rev, id),
      INFO_FILE_NAME);
  }


  private void persistSnapshotInfo(SnapshotInfo snapshotInfo) throws PipelineRuntimeException {
    try (OutputStream out = new DataStore(getPipelineSnapshotInfoFile(snapshotInfo.getName(), snapshotInfo.getRev(),
      snapshotInfo.getId())).getOutputStream()) {
      json.writeValue(out, new SnapshotInfoJson(snapshotInfo));
    } catch (IOException e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0602, snapshotInfo.getId(), snapshotInfo.getName(),
        snapshotInfo.getRev(), e.getMessage(), e);
    }
  }

  private void persistSnapshot(String name, String rev, String id, List<List<StageOutput>> snapshotBatches)
    throws PipelineRuntimeException {
    try (OutputStream out = new DataStore(getPipelineSnapshotFile(name, rev, id)).getOutputStream()) {
      json.writeValue(out, new SnapshotDataJson(new SnapshotData(snapshotBatches)));
    } catch (IOException e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0603, id, name, rev, e.getMessage(), e);
    }
  }

}
