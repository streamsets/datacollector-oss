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
package com.streamsets.datacollector.execution.snapshot.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.execution.Snapshot;
import com.streamsets.datacollector.execution.SnapshotInfo;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.execution.snapshot.common.SnapshotData;
import com.streamsets.datacollector.execution.snapshot.common.SnapshotImpl;
import com.streamsets.datacollector.execution.snapshot.common.SnapshotInfoImpl;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.restapi.bean.SnapshotDataJson;
import com.streamsets.datacollector.restapi.bean.SnapshotInfoJson;
import com.streamsets.datacollector.runner.PipelineRuntimeException;
import com.streamsets.datacollector.runner.StageOutput;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.util.PipelineException;

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
  private final LockCache<String> lockCache;
  private final RuntimeInfo runtimeInfo;
  private final ObjectMapper json;

  @Inject
  public FileSnapshotStore(RuntimeInfo runtimeInfo, LockCache<String> lockCache) {
    this.runtimeInfo = runtimeInfo;
    this.lockCache = lockCache;
    json = ObjectMapperFactory.get();
  }

  @Override
  public SnapshotInfo create(String user, String name, String rev, String id, String label, boolean failureSnapshot) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      SnapshotInfo existingInfo = getInfo(name, rev, id);
      if (existingInfo != null) {
        throw new PipelineException(ContainerError.CONTAINER_0606, id, name);
      }
      PipelineDirectoryUtil.createPipelineSnapshotDir(runtimeInfo, name, rev, id);
      SnapshotInfo snapshotInfo = new SnapshotInfoImpl(user, id, label, name, rev, System.currentTimeMillis(), true, 0, failureSnapshot);
      persistSnapshotInfo(snapshotInfo);
      return snapshotInfo;
    }
  }

  @Override
  public SnapshotInfo save(
      String name,
      String rev,
      String id,
      long batchNumber,
      List<List<StageOutput>> snapshotBatches
  ) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      SnapshotInfo existingInfo = getInfo(name, rev, id);
      if (existingInfo == null) {
        throw new PipelineException(ContainerError.CONTAINER_0605);
      }
      persistSnapshot(name, rev, id, snapshotBatches);
      SnapshotInfo updatedSnapshotInfo =
        new SnapshotInfoImpl(
            existingInfo.getUser(),
            id,
            existingInfo.getLabel(),
            name,
            rev,
            System.currentTimeMillis(),
            false,
            batchNumber,
            existingInfo.isFailureSnapshot()
        );
      persistSnapshotInfo(updatedSnapshotInfo);
      return updatedSnapshotInfo;
    }
  }

  @Override
  public SnapshotInfo updateLabel(String name, String rev, String id, String snapshotLabel) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      SnapshotInfo existingInfo = getInfo(name, rev, id);
      if (existingInfo == null) {
        throw new PipelineException(ContainerError.CONTAINER_0605);
      }
      SnapshotInfo updatedSnapshotInfo =
          new SnapshotInfoImpl(
              existingInfo.getUser(),
              existingInfo.getId(),
              snapshotLabel,
              existingInfo.getName(),
              existingInfo.getRev(),
              existingInfo.getTimeStamp(),
              existingInfo.isInProgress(),
              existingInfo.getBatchNumber(),
              existingInfo.isFailureSnapshot()
          );
      persistSnapshotInfo(updatedSnapshotInfo);
      return updatedSnapshotInfo;
    }
  }

  @Override
  public Snapshot get(String name, String rev, String id) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      SnapshotInfo info = getInfo(name, rev, id);
      File data = getData(name, rev, id);
      return new SnapshotImpl(info, data);
    }
  }

  @Override
  public List<SnapshotInfo> getSummaryForPipeline(String name, String rev) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      List<SnapshotInfo> list = new ArrayList<>();
      File snapshotDir = PipelineDirectoryUtil.getPipelineSnapshotBaseDir(runtimeInfo, name, rev);
      if (snapshotDir.exists()) {
        for (String snapshotName : snapshotDir.list(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            // If one browses to the pipelines directory, mac creates a ".DS_store directory and this causes a problem
            // So filter it out
            return !name.startsWith(".");
          }
        })) {
          SnapshotInfo snapshotInfo = getInfo(name, rev, snapshotName);
          if (snapshotInfo != null) {
            list.add(snapshotInfo);
          }
        }
      }
      return Collections.unmodifiableList(list);
    }
  }

  @Override
  public void deleteSnapshot(String name, String rev, String id) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      File snapshotBaseDir = PipelineDirectoryUtil.getPipelineSnapshotDir(runtimeInfo, name, rev, id);
      if (snapshotBaseDir.exists()) {
        if (!PipelineDirectoryUtil.deleteAll(snapshotBaseDir)) {
          throw new PipelineRuntimeException(ContainerError.CONTAINER_0601);
        }
      }
    }
  }

  @Override
  public  SnapshotInfo getInfo(String name, String rev, String id) throws PipelineException {
    synchronized (lockCache.getLock(name)) {
      try {
        File infoFile = getPipelineSnapshotInfoFile(name, rev, id);
        if (infoFile.exists()) {
          try (InputStream in = new FileInputStream(infoFile)) {
            SnapshotInfoJson snapshotInfoJsonBean = json.readValue(in, SnapshotInfoJson.class);
            return snapshotInfoJsonBean.getSnapshotInfo();
          } catch (IOException e) {
            throw new PipelineStoreException(ContainerError.CONTAINER_0101, e.toString(), e);
          }
        } else {
          return null;
        }
      } catch (Exception e) {
        throw new PipelineRuntimeException(ContainerError.CONTAINER_0604, id, name, rev, e.toString(), e);
      }
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
    DataStore dataStore = new DataStore(getPipelineSnapshotInfoFile(snapshotInfo.getName(), snapshotInfo.getRev(),
      snapshotInfo.getId()));
    try (OutputStream out = dataStore.getOutputStream()) {
      json.writeValue(out, new SnapshotInfoJson(snapshotInfo));
      dataStore.commit(out);
    } catch (IOException e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0602, snapshotInfo.getId(), snapshotInfo.getName(),
        snapshotInfo.getRev(), e.toString(), e);
    } finally {
      dataStore.release();
    }
  }

  private void persistSnapshot(String name, String rev, String id, List<List<StageOutput>> snapshotBatches)
    throws PipelineRuntimeException {
    DataStore dataStore = new DataStore(getPipelineSnapshotFile(name, rev, id));
    try (OutputStream out = dataStore.getOutputStream()) {
      json.writeValue(out, new SnapshotDataJson(new SnapshotData(snapshotBatches)));
      dataStore.commit(out);
    } catch (IOException e) {
      throw new PipelineRuntimeException(ContainerError.CONTAINER_0603, id, name, rev, e.toString(), e);
    } finally {
      dataStore.release();
    }
  }

}
