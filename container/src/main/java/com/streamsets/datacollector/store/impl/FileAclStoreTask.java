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
package com.streamsets.datacollector.store.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamsets.datacollector.io.DataStore;
import com.streamsets.datacollector.json.ObjectMapperFactory;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.main.UserGroupManager;
import com.streamsets.datacollector.store.PipelineStoreException;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.ContainerError;
import com.streamsets.datacollector.util.LockCache;
import com.streamsets.datacollector.util.PipelineDirectoryUtil;
import com.streamsets.datacollector.util.PipelineException;
import com.streamsets.lib.security.acl.AclDtoJsonMapper;
import com.streamsets.lib.security.acl.dto.Acl;
import com.streamsets.lib.security.acl.dto.Permission;
import com.streamsets.lib.security.acl.dto.ResourceType;
import com.streamsets.lib.security.acl.dto.SubjectType;
import com.streamsets.lib.security.acl.json.AclJson;
import com.streamsets.pipeline.api.impl.PipelineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileAclStoreTask extends AbstractAclStoreTask {
  private static final Logger LOG = LoggerFactory.getLogger(FileAclStoreTask.class);
  private final PipelineStoreTask pipelineStore;
  private final LockCache<String> lockCache;
  public static final String ACL_FILE = "acl.json";
  private Path storeDir;
  private final ObjectMapper json;

  @Inject
  public FileAclStoreTask(
      RuntimeInfo runtimeInfo,
      PipelineStoreTask pipelineStoreTask,
      LockCache<String> lockCache,
      UserGroupManager userGroupManager
  ) {
    super(pipelineStoreTask, lockCache, userGroupManager);
    json = ObjectMapperFactory.get();
    this.pipelineStore = pipelineStoreTask;
    this.lockCache = lockCache;
    storeDir = Paths.get(runtimeInfo.getDataDir(), PipelineDirectoryUtil.PIPELINE_INFO_BASE_DIR);
  }

  private Path getPipelineDir(String name) {
    return storeDir.resolve(PipelineUtils.escapedPipelineName(name));
  }

  private Path getPipelineAclFile(String name) {
    return getPipelineDir(name).resolve(ACL_FILE);
  }

  @Override
  public void initTask() {
  }

  @Override
  public Acl createAcl(
      String pipelineName,
      ResourceType resourceType,
      long resourceCreateTime,
      String resourceOwner
  ) throws PipelineStoreException {
    synchronized (lockCache.getLock(pipelineName)) {
      Acl acl = new Acl();
      acl.setResourceId(pipelineName);
      acl.setResourceOwner(resourceOwner);
      acl.setResourceType(resourceType);
      acl.setResourceCreatedTime(resourceCreateTime);
      acl.setLastModifiedBy(resourceOwner);
      acl.setLastModifiedOn(resourceCreateTime);

      Permission ownerPermission = new Permission();
      ownerPermission.setSubjectId(resourceOwner);
      ownerPermission.setSubjectType(SubjectType.USER);
      ownerPermission.setLastModifiedOn(resourceCreateTime);
      ownerPermission.setLastModifiedBy(resourceOwner);
      ownerPermission.getActions().addAll(resourceType.getActions());

      acl.getPermissions().add(ownerPermission);
      saveAcl(pipelineName, acl);
      return acl;
    }
  }

  @Override
  public Acl saveAcl(String pipelineName, Acl acl) throws PipelineStoreException {
    synchronized (lockCache.getLock(pipelineName)) {
      DataStore dataStore = new DataStore(getPipelineAclFile(pipelineName).toFile());
      try (OutputStream os = dataStore.getOutputStream()) {
        ObjectMapperFactory.get().writeValue(os, AclDtoJsonMapper.INSTANCE.toAclJson(acl));
        dataStore.commit(os);
      } catch (IOException ex) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0406, pipelineName, ex.toString(), ex);
      } finally {
        dataStore.release();
      }
      return acl;
    }
  }

  @Override
  public Acl getAcl(String pipelineName) throws PipelineException {
    return getAcl(pipelineName, false);
  }

  private Acl getAcl(String pipelineName, boolean checkExistence) throws PipelineException {
    synchronized (lockCache.getLock(pipelineName)) {
      if (checkExistence && !pipelineStore.hasPipeline(pipelineName)) {
        throw new PipelineStoreException(ContainerError.CONTAINER_0200, pipelineName);
      }
      Path aclFilePath = getPipelineAclFile(pipelineName);
      if (Files.exists(aclFilePath)) {
        try (InputStream aclFile = Files.newInputStream(getPipelineAclFile(pipelineName))) {
          AclJson aclJsonBean = json.readValue(aclFile, AclJson.class);
          return AclDtoJsonMapper.INSTANCE.asAclDto(aclJsonBean);
        } catch (Exception ex) {
          throw new PipelineStoreException(ContainerError.CONTAINER_0206, pipelineName, ex);
        }
      }
      return null;
    }
  }

  @Override
  public void deleteAcl(String name) {
    // no op, deleting pipeline takes care of deleting pipeline acl file too
  }

}
