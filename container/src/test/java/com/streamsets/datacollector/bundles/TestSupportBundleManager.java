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
package com.streamsets.datacollector.bundles;

import com.google.common.collect.ImmutableList;
import com.streamsets.datacollector.bundles.content.SimpleGenerator;
import com.streamsets.datacollector.execution.PipelineStateStore;
import com.streamsets.datacollector.execution.SnapshotStore;
import com.streamsets.datacollector.main.BuildInfo;
import com.streamsets.datacollector.main.RuntimeInfo;
import com.streamsets.datacollector.store.PipelineStoreTask;
import com.streamsets.datacollector.util.Configuration;
import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.apache.commons.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSupportBundleManager {
  private static final Logger LOG = LoggerFactory.getLogger(TestSupportBundleManager.class);

  private static SupportBundleManager manager;

  @BeforeClass
  public static void createManager() {
    Configuration configuration = mock(Configuration.class);
    RuntimeInfo runtimeInfo = mock(RuntimeInfo.class);
    when(runtimeInfo.getId()).thenReturn("super-secret-id");
    when(runtimeInfo.isAclEnabled()).thenReturn(false);
    BuildInfo buildInfo = mock(BuildInfo.class);
    when(buildInfo.getVersion()).thenReturn("666");
    PipelineStoreTask pipelineStoreTask = mock(PipelineStoreTask.class);
    PipelineStateStore stateStore = mock(PipelineStateStore.class);
    SnapshotStore snapshotStore = mock(SnapshotStore.class);

    manager = new SupportBundleManager(
      new SafeScheduledExecutorService(1, "supportBundleExecutor"),
      configuration,
      pipelineStoreTask,
      stateStore,
      snapshotStore,
      runtimeInfo,
      buildInfo
    );

    // Initialize manager to load all content generators
    manager.init();
  }

  @Test
  public void testGetContentDefinitions() {
    List<BundleContentGeneratorDefinition> defs = manager.getContentDefinitions();
    assertNotNull(defs);
    assertTrue(defs.size() > 0);

    boolean found = false;
    for(BundleContentGeneratorDefinition def : defs) {
      LOG.debug("Definition: " + def);
      if(def.getKlass() == SimpleGenerator.class) {
        found = true;
        break;
      }
    }

    assertTrue(found);
  }

  @Test
  public void testSimpleBundleCreation() throws Exception {
    ZipFile bundle = zipFile(ImmutableList.of(SimpleGenerator.class.getSimpleName()));
    ZipEntry entry;

    // Check we have expected files
    entry = bundle.getEntry("metadata.properties");
    assertNotNull(entry);

    entry = bundle.getEntry("generators.properties");
    assertNotNull(entry);

    entry = bundle.getEntry("com.streamsets.datacollector.bundles.content.SimpleGenerator/file.txt");
    assertNotNull(entry);
  }

  private ZipFile zipFile(List<String> bundles) throws Exception {
    InputStream bundleStream = manager.generateNewBundle(bundles).getInputStream();
    File outputFile = File.createTempFile("test-support-bundle", ".zip");
    outputFile.deleteOnExit();

    try(FileOutputStream outputStream = new FileOutputStream(outputFile)) {
      IOUtils.copy(bundleStream, outputStream);
    }

    ZipFile zipFile = new ZipFile(outputFile);
    Enumeration<? extends ZipEntry> entries = zipFile.entries();
    LOG.debug("Archive content:");
    while(entries.hasMoreElements()) {
      ZipEntry entry = entries.nextElement();
      LOG.debug("Entry {}", entry.getName());
    }

    return zipFile;
  }
}
