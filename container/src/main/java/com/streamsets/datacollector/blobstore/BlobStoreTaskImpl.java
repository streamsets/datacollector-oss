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
package com.streamsets.datacollector.blobstore;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.datacollector.blobstore.meta.BlobStoreMetadata;
import com.streamsets.datacollector.blobstore.meta.NamespaceMetadata;
import com.streamsets.datacollector.blobstore.meta.ObjectMetadata;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.task.AbstractTask;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class BlobStoreTaskImpl extends AbstractTask implements BlobStoreTask {

  private static final Logger LOG = LoggerFactory.getLogger(BlobStoreTaskImpl.class);
  // Subdirectory that will be created under data directory for storing the objects
  private static final String BASE_DIR = "blobstore";
  // File name of stored version of our metadata database
  private static final String METADATA_FILE = "metadata.json";
  // Temporary name for a three-phase commit
  private static final String NEW_METADATA_FILE = "updated.metadata.json";

  // JSON ObjectMapper for storing and reading metadata database to/from disk
  private static final ObjectMapper jsonMapper = new ObjectMapper();

  static {
    jsonMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
  }

  /**
   * Implementation of VersionedContent when we need to return both version and it's content at the same time.
   */
  private static class VersionedContentImpl implements VersionedContent {
    long version;
    String content;

    VersionedContentImpl(long version, String content) {
      this.version = version;
      this.content = content;
    }

    @Override
    public long version() {
      return version;
    }

    @Override
    public String content() {
      return content;
    }
  }

  private final RuntimeInfo runtimeInfo;

  private Path baseDir;
  @VisibleForTesting
  Path metadataFile;
  @VisibleForTesting
  Path newMetadataFile;
  private BlobStoreMetadata metadata;

  @Inject
  public BlobStoreTaskImpl(
    RuntimeInfo runtimeInfo
  ) {
    super("Blob Store Task");
    this.runtimeInfo = runtimeInfo;
  }

  @Override
  public void initTask() {
    this.baseDir = Paths.get(runtimeInfo.getDataDir(), BASE_DIR);
    this.metadataFile = this.baseDir.resolve(METADATA_FILE);
    this.newMetadataFile = this.baseDir.resolve(NEW_METADATA_FILE);

    if (!Files.exists(baseDir)) {
      initializeFreshInstall();
    } else {
      initializeFromDisk();
    }
  }

  private void initializeFreshInstall() {
    try {
      Files.createDirectories(baseDir);
    } catch (IOException e) {
      throw new RuntimeException(Utils.format("Could not create directory '{}'", baseDir), e);
    }

    // Create new fresh metadata and persist them on disk
    this.metadata = new BlobStoreMetadata();
    try {
      saveMetadata();
    } catch (StageException e) {
      throw new RuntimeException(Utils.format("Can't initialize blob store: {}", e.toString()), e);
    }
  }

  private void initializeFromDisk() {
    try {
      if (Files.exists(metadataFile)) {
        // Most happy path - the metadata file exists
        this.metadata = loadMetadata();

        if (Files.exists(newMetadataFile)) {
          LOG.error("Old temporary file already exists, using older state and dropping new state");
          Files.delete(newMetadataFile);
        }
      } else if (Files.exists(newMetadataFile)) {
        LOG.error("Missing primary metadata file, recovering new state");
        Files.move(newMetadataFile, metadataFile);

        this.metadata = loadMetadata();
      } else {
        throw new StageException(BlobStoreError.BLOB_STORE_0013);
      }

      LOG.debug(
        "Loaded blob store metadata of version {} with {} namespaces",
        metadata.getVersion(),
        metadata.getNamespaces().size()
      );
    } catch (StageException | IOException e) {
      throw new RuntimeException(Utils.format("Can't initialize blob store: {}", e.toString()), e);
    }
  }

  @Override
  public synchronized void store(String namespace, String id, long version, String content) throws StageException {
    LOG.debug("Store on namespace={}, id={}, version={}", namespace, id, version);
    Preconditions.checkArgument(BlobStore.VALID_NAMESPACE_PATTERN.matcher(namespace).matches());
    Preconditions.checkArgument(BlobStore.VALID_ID_PATTERN.matcher(id).matches());

    ObjectMetadata objectMetadata = metadata.getOrCreateNamespace(namespace).getOrCreateObject(id);

    if (objectMetadata.containsVersion(version)) {
      throw new StageException(BlobStoreError.BLOB_STORE_0003, namespace, id, version);
    }

    // Store the content inside independent file with randomized location
    String contentFile = namespace + UUID.randomUUID().toString() + ".content";
    try {
      Files.write(baseDir.resolve(contentFile), content.getBytes());
    } catch (IOException e) {
      throw new StageException(BlobStoreError.BLOB_STORE_0004, e.toString(), e);
    }

    // Update and save internal structure
    objectMetadata.createContent(version, contentFile);
    saveMetadata();
  }

  @Override
  public synchronized long latestVersion(String namespace, String id) throws StageException {
    return getObjectDieIfNotExists(namespace, id).latestVersion();
  }

  @Override
  public synchronized boolean exists(String namespace, String id) {
    ObjectMetadata objectMetadata = getObject(namespace, id);
    return objectMetadata != null;
  }

  @Override
  public synchronized boolean exists(String namespace, String id, long version) {
    ObjectMetadata objectMetadata = getObject(namespace, id);
    if (objectMetadata == null) {
      return false;
    }

    return objectMetadata.containsVersion(version);
  }

  @Override
  public synchronized Set<Long> allVersions(String namespace, String id) {
    ObjectMetadata objectMetadata = getObject(namespace, id);
    return objectMetadata == null ? Collections.emptySet() : objectMetadata.allVersions();
  }

  @Override
  public synchronized String retrieve(String namespace, String id, long version) throws StageException {
    LOG.debug("Retrieve on namespace={}, id={}, version={}", namespace, id, version);
    ObjectMetadata objectMetadata = getObjectDieIfNotExists(namespace, id);

    if (!objectMetadata.containsVersion(version)) {
      throw new StageException(BlobStoreError.BLOB_STORE_0007, namespace, id, version);
    }

    return loadContentFromDrive(objectMetadata.uuidForVersion(version));
  }

  @Override
  public synchronized VersionedContent retrieveLatest(String namespace, String id) throws StageException {
    ObjectMetadata objectMetadata = getObject(namespace, id);
    if (objectMetadata == null) {
      return null;
    }

    long version = objectMetadata.latestVersion();
    return new VersionedContentImpl(
      version,
      loadContentFromDrive(objectMetadata.uuidForVersion(version))
    );
  }

  private String loadContentFromDrive(String objectUuid) throws StageException {
    try {
      return new String(Files.readAllBytes(baseDir.resolve(objectUuid)));
    } catch(IOException e) {
      throw new StageException(BlobStoreError.BLOB_STORE_0008, e.toString(), e);
    }
  }

  @Override
  public synchronized void delete(String namespace, String id, long version) throws StageException {
    LOG.debug("Delete on namespace={}, id={}, version={}", namespace, id, version);
    ObjectMetadata objectMetadata = getObjectDieIfNotExists(namespace, id);

    if(!objectMetadata.containsVersion(version)) {
      throw new StageException(BlobStoreError.BLOB_STORE_0007, namespace, id, version);
    }

    String uuid = objectMetadata.uuidForVersion(version);
    objectMetadata.removeVersion(version);
    try {
      Files.delete(baseDir.resolve(uuid));
    } catch (IOException e) {
      throw new StageException(BlobStoreError.BLOB_STORE_0008, e.toString(), e);
    }

    NamespaceMetadata namespaceMetadata = metadata.getNamespace(namespace);

    // If this is the last version of given object, let's also remove the whole object
    if(objectMetadata.isEmpty()) {
      namespaceMetadata.removeObject(id);
    }

    // And propagate the same delete up
    if(namespaceMetadata.isEmpty()) {
      metadata.removeNamespace(namespace);
    }

    saveMetadata();
  }

  @Override
  public void deleteAllVersions(String namespace, String id) throws StageException {
    for(long version: allVersions(namespace, id)) {
      delete(namespace, id, version);
    }
  }

  /**
   * Commit metadata content to a file to disk.
   *
   * This method does three-phased commit:
   * 1) New content is written into a new temporary file.
   * 2) Old metadata is dropped
   * 3) Rename from new to old is done
   */
  synchronized private void saveMetadata() throws StageException {
    // 0) Validate pre-conditions
    if(Files.exists(newMetadataFile))  {
      throw new StageException(BlobStoreError.BLOB_STORE_0010);
    }

    // 1) New content is written into a new temporary file.
    try (
      OutputStream os = Files.newOutputStream(newMetadataFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
    ) {
      jsonMapper.writeValue(os, metadata);
    } catch (IOException e) {
      throw new StageException(BlobStoreError.BLOB_STORE_0001, e.toString(), e);
    }

    // 2) Old metadata is dropped
    try {
      if(Files.exists(metadataFile)) {
        Files.delete(metadataFile);
      }
    } catch (IOException e) {
      throw new StageException(BlobStoreError.BLOB_STORE_0011, e.toString(), e);
    }

    // 3) Rename from new to old is done
    try {
      Files.move(newMetadataFile, metadataFile);
    } catch (IOException e) {
      throw new StageException(BlobStoreError.BLOB_STORE_0012, e.toString(), e);
    }
  }

  private BlobStoreMetadata loadMetadata() throws StageException {
    try(
      InputStream is = Files.newInputStream(metadataFile);
    ) {
      return jsonMapper.readValue(is, BlobStoreMetadata.class);
    } catch (IOException e) {
      throw new StageException(BlobStoreError.BLOB_STORE_0001, e.toString(), e);
    }
  }

  private ObjectMetadata getObject(String namespace, String id) {
    NamespaceMetadata namespaceMetadata = metadata.getNamespace(namespace);
    if(namespaceMetadata == null) {
      return null;
    }

    return namespaceMetadata.getObject(id);
  }

  private ObjectMetadata getObjectDieIfNotExists(String namespace, String id) throws StageException {
    NamespaceMetadata namespaceMetadata = metadata.getNamespace(namespace);
    if(namespaceMetadata == null) {
      throw new StageException(BlobStoreError.BLOB_STORE_0005, namespace);
    }

    ObjectMetadata objectMetadata = namespaceMetadata.getObject(id);
    if(objectMetadata == null) {
      throw new StageException(BlobStoreError.BLOB_STORE_0006, namespace, id);
    }

    return objectMetadata;
  }

  @Override
  public Set<String> listNamespaces() {
    return Collections.unmodifiableSet(metadata.getNamespaces().keySet());
  }

  @Override
  public Set<String> listObjects(String namespace) {
    NamespaceMetadata namespaceMetadata = metadata.getNamespace(namespace);
    if(namespaceMetadata == null) {
      return Collections.emptySet();
    }

    return Collections.unmodifiableSet(namespaceMetadata.getObjects().keySet());
  }

  @Override
  public String retrieveContentFileName(String namespace, String id, long version) throws StageException {
    LOG.debug("Retrieve content file name on namespace={}, id={}, version={}", namespace, id, version);
    ObjectMetadata objectMetadata = getObjectDieIfNotExists(namespace, id);

    if (!objectMetadata.containsVersion(version)) {
      throw new StageException(BlobStoreError.BLOB_STORE_0007, namespace, id, version);
    }
    return objectMetadata.uuidForVersion(version);
  }
}
