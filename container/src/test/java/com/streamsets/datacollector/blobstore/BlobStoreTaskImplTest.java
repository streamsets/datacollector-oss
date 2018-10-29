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

import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.pipeline.api.BlobStore;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class BlobStoreTaskImplTest {

  private static RuntimeInfo createRuntimeInfo() throws Exception {
   String dataDirectory = Files.createTempDirectory("blob-store-test").toString();

    RuntimeInfo runtimeInfo = mock(RuntimeInfo.class);
    when(runtimeInfo.getDataDir()).thenReturn(dataDirectory);

    return runtimeInfo;
  }

  @Test
  public void testInitializeAndReinitialize() throws Exception {
    RuntimeInfo runtimeInfo = createRuntimeInfo();

    // Should create a metadata file on disk
    BlobStoreTask store = new BlobStoreTaskImpl(runtimeInfo);
    store.init();

    assertTrue(Files.exists(Paths.get(runtimeInfo.getDataDir(), "blobstore", "metadata.json")));

    // Creating second instance in the same spot should not have any issues
    BlobStoreTask secondStore = new BlobStoreTaskImpl(runtimeInfo);
    secondStore.init();
  }

  @Test
  public void testStoreAndRetrieve() throws Exception {
    RuntimeInfo runtimeInfo = createRuntimeInfo();

    BlobStoreTask store = new BlobStoreTaskImpl(runtimeInfo);
    store.init();

    store.store("policy", "1234", 1, "Drop everything!");

    BlobStoreTask retrieveStore = new BlobStoreTaskImpl(runtimeInfo);
    retrieveStore.init();

    String retrieved = retrieveStore.retrieve("policy", "1234", 1);

    assertEquals("Drop everything!", retrieved);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidNamespace() throws Exception {
    BlobStoreTask store = new BlobStoreTaskImpl(createRuntimeInfo());
    store.init();

    store.store("!@#$%$^%^", "1234", 1, "Drop everything!");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidId() throws Exception {
    BlobStoreTask store = new BlobStoreTaskImpl(createRuntimeInfo());
    store.init();

    store.store("policy", "!@#$%$^%^", 1, "Drop everything!");
  }

  @Test
  public void testExists() throws Exception {
    BlobStoreTask store = new BlobStoreTaskImpl(createRuntimeInfo());
    store.init();

    assertFalse(store.exists("policy", "1234"));

    store.store("policy", "1234", 4, "");
    store.store("policy", "abcd", 4, "");

    assertTrue(store.exists("policy", "1234"));
    assertTrue(store.exists("policy", "abcd"));
    assertFalse(store.exists("policy", "Sorry"));
  }

  @Test
  public void testVersions() throws Exception {
    BlobStoreTask store = new BlobStoreTaskImpl(createRuntimeInfo());
    store.init();

    store.store("policy", "1234", 10, "");
    store.store("policy", "1234", 15, "");
    store.store("policy", "1234", 4, "");

    assertEquals(15, store.latestVersion("policy", "1234"));
    assertEquals(ImmutableSet.of(10L, 15L, 4L), store.allVersions("policy", "1234"));
  }

  @Test
  public void testRetrieveLatest() throws Exception {
    BlobStoreTask store = new BlobStoreTaskImpl(createRuntimeInfo());
    store.init();

    store.store("policy", "1234", 10, "10");
    store.store("policy", "1234", 15, "15");
    store.store("policy", "1234", 4, "4");

    BlobStore.VersionedContent content = store.retrieveLatest("policy", "1234");
    assertEquals(15, content.version());
    assertEquals("15", content.content());
  }

  @Test
  public void testDelete() throws Exception {
    BlobStoreTask store = new BlobStoreTaskImpl(createRuntimeInfo());
    store.init();

    store.store("policy", "1234", 10, "");
    store.store("policy", "1234", 15, "");
    store.store("policy", "1234", 4, "");

    store.delete("policy", "1234", 10);
    assertEquals(15, store.latestVersion("policy", "1234"));
    assertEquals(ImmutableSet.of(15L, 4L), store.allVersions("policy", "1234"));

    store.delete("policy", "1234", 15);
    assertEquals(4, store.latestVersion("policy", "1234"));
    assertEquals(ImmutableSet.of(4L), store.allVersions("policy", "1234"));

    store.delete("policy", "1234", 4);
    assertEquals(Collections.emptySet(), store.allVersions("policy", "1234"));

    assertFalse(store.exists("policy", "1234"));
  }

  @Test
  public void testDeleteAllVersions() throws Exception {
    BlobStoreTask store = new BlobStoreTaskImpl(createRuntimeInfo());
    store.init();

    store.store("policy", "1234", 10, "");
    store.store("policy", "1234", 15, "");
    store.store("policy", "1234", 4, "");

    store.deleteAllVersions("policy", "1234");

    assertEquals(Collections.emptySet(), store.allVersions("policy", "1234"));
    assertFalse(store.exists("policy", "1234"));
  }

  @Test(expected = RuntimeException.class)
  public void testMetadataFileMissing() throws Exception {
    RuntimeInfo runtimeInfo = createRuntimeInfo();

    // Create a dummy content
    BlobStoreTaskImpl store = new BlobStoreTaskImpl(runtimeInfo);
    store.init();
    store.store("policy", "1234", 10, "");

    // Drop main metadata file (e.g. the work directory is non-empty and is missing metadata file(s)
    Files.delete(store.metadataFile);
    store = new BlobStoreTaskImpl(runtimeInfo);
    store.initTask();
  }

  @Test
  public void testRecoverMetadataFile() throws Exception {
    RuntimeInfo runtimeInfo = createRuntimeInfo();

    // Create a dummy content
    BlobStoreTaskImpl store = new BlobStoreTaskImpl(runtimeInfo);
    store.init();
    store.store("policy", "1234", 10, "");

    // Model issue with missing primary file and existing secondary
    Files.move(store.metadataFile, store.newMetadataFile);

    store = new BlobStoreTaskImpl(runtimeInfo);
    store.initTask();

    // The store should be able to recover and see the data
    assertTrue(Files.exists(store.metadataFile));
    assertFalse(Files.exists(store.newMetadataFile));
    assertTrue(store.exists("policy", "1234", 10));
  }

  @Test
  public void tetstRetrieveContentFileName() throws Exception {

    BlobStoreTask store = new BlobStoreTaskImpl(createRuntimeInfo());
    store.init();
    store.store("policy", "1234", 10, "10");
    String filename = store.retrieveContentFileName("policy", "1234", 10);
    assertTrue(filename.startsWith("policy"));
    assertTrue(filename.endsWith(".content"));
  }
}
