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
package com.streamsets.pipeline.lib.io;

import com.streamsets.pipeline.lib.executor.SafeScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestDirectoryPathCreationWatcher {

  private class DirectoryCreationWorker implements Runnable {
    List<Path> remainingPathsToCreate;
    boolean shouldShuffle;

    DirectoryCreationWorker(Collection<Path> paths, boolean shouldShuffle) {
      this.remainingPathsToCreate = Collections.synchronizedList(new ArrayList<Path>(paths));
      this.shouldShuffle = shouldShuffle;
    }

    List<Path> getRemainingPathsToCreate() {
      return this.remainingPathsToCreate;
    }

    @Override
    public void run() {
      if (!remainingPathsToCreate.isEmpty()) {
        if (shouldShuffle) {
          Collections.shuffle(remainingPathsToCreate);
        }
        Path path = remainingPathsToCreate.remove(0);
        try {
          Files.createDirectories(path);
        } catch (IOException e) {
          Assert.fail(e.getMessage());
        }
      }
    }
  }

  private void checkCorrectness(Collection<Path> paths, Set<Path> foundPaths, ScheduledExecutorService executor) {
    Assert.assertEquals("All Paths are not found", paths.size(), foundPaths.size());
    Assert.assertTrue("Found Unwanted Paths", foundPaths.containsAll(paths));
    Assert.assertTrue("Executor should be null after shut down", executor == null);
  }

  @Test
  public void testSynchronousDirectoryFinder() throws Exception {
    String testDataDir = "target" + File.separatorChar + UUID.randomUUID().toString();
    Files.createDirectories(Paths.get(testDataDir));
    final Collection<Path> paths = Arrays.asList(
        Paths.get(testDataDir + File.separatorChar + "d1"),
        Paths.get(testDataDir + File.separatorChar + "d2"),
        Paths.get(testDataDir + File.separatorChar + "d3"),
        Paths.get(testDataDir + File.separatorChar + "d4"),
        Paths.get(testDataDir + File.separatorChar + "d5"),
        Paths.get(testDataDir + File.separatorChar + "d6"),
        Paths.get(testDataDir + File.separatorChar + "d7"),
        Paths.get(testDataDir + File.separatorChar + "d8")
    );
    DirectoryPathCreationWatcher watcher = new DirectoryPathCreationWatcher(paths, 0);
    DirectoryCreationWorker worker = new DirectoryCreationWorker(paths, true);
    for (int i =0; i < paths.size(); i++) {
      worker.run();
    }

    Set<Path> foundPaths = watcher.find();
    checkCorrectness(paths, foundPaths, (ScheduledExecutorService)Whitebox.getInternalState(watcher, "executor"));
  }

  @Test
  public void testAsyncDirFinderAllDirsAtOnce() throws Exception {
    String testDataDir = "target" + File.separatorChar + UUID.randomUUID().toString();
    Files.createDirectories(Paths.get(testDataDir));
    final Collection<Path> paths = Arrays.asList(
        Paths.get(testDataDir + File.separatorChar + "d1"),
        Paths.get(testDataDir + File.separatorChar + "d2"),
        Paths.get(testDataDir + File.separatorChar + "d3"),
        Paths.get(testDataDir + File.separatorChar + "d4"),
        Paths.get(testDataDir + File.separatorChar + "d5"),
        Paths.get(testDataDir + File.separatorChar + "d6"),
        Paths.get(testDataDir + File.separatorChar + "d7"),
        Paths.get(testDataDir + File.separatorChar + "d8")
    );
    DirectoryPathCreationWatcher watcher = new DirectoryPathCreationWatcher(paths, 1);
    DirectoryCreationWorker worker = new DirectoryCreationWorker(paths, true);
    for (int i =0; i < paths.size(); i++) {
      worker.run();
    }

    Thread.sleep(paths.size() * 1000);

    Set<Path> foundPaths = watcher.find();
    checkCorrectness(paths, foundPaths, (ScheduledExecutorService)Whitebox.getInternalState(watcher, "executor"));
  }

  @Test
  public void testAsyncDirFinderDirsCreatedPeriodically() throws Exception {
    String testDataDir = "target" + File.separatorChar + UUID.randomUUID().toString();
    Files.createDirectories(Paths.get(testDataDir));
    final Collection<Path> paths = Arrays.asList(
        Paths.get(testDataDir + File.separatorChar + "d1"),
        Paths.get(testDataDir + File.separatorChar + "d2"),
        Paths.get(testDataDir + File.separatorChar + "d3"),
        Paths.get(testDataDir + File.separatorChar + "d4"),
        Paths.get(testDataDir + File.separatorChar + "d5"),
        Paths.get(testDataDir + File.separatorChar + "d6"),
        Paths.get(testDataDir + File.separatorChar + "d7"),
        Paths.get(testDataDir + File.separatorChar + "d8"),

        Paths.get(testDataDir + File.separatorChar + "d1" + File.separatorChar + "d11"),
        Paths.get(testDataDir + File.separatorChar + "d1" + File.separatorChar + "d12"),

        Paths.get(testDataDir + File.separatorChar + "d2" + File.separatorChar + "d21" + File.separatorChar + "d211"),
        Paths.get(testDataDir + File.separatorChar + "d2" + File.separatorChar + "d22" + File.separatorChar + "d221"),
        Paths.get(testDataDir + File.separatorChar + "d2" + File.separatorChar + "d23" + File.separatorChar + "d231"),

        Paths.get(testDataDir + File.separatorChar + "d3" + File.separatorChar + "d31"),

        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"
            + File.separatorChar + "d4111"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"
            + File.separatorChar + "d4111" + File.separatorChar + "d41111")

    );
    DirectoryPathCreationWatcher watcher = new DirectoryPathCreationWatcher(paths, 1);

    ScheduledExecutorService directoryCreationExecutor = new SafeScheduledExecutorService(1, "Directory Creator");
    DirectoryCreationWorker worker = new DirectoryCreationWorker(paths, false);
    directoryCreationExecutor.scheduleAtFixedRate(worker, 0, 100, TimeUnit.MILLISECONDS);

    Set<Path> foundPaths = new HashSet<Path>();
    while (!worker.getRemainingPathsToCreate().isEmpty()) {
      Set<Path> foundPathsTillNow = watcher.find();
      foundPaths.addAll(foundPathsTillNow);
      Thread.sleep(100);
    }

    directoryCreationExecutor.shutdownNow();
    directoryCreationExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);

    Thread.sleep(paths.size() * 100 + 1000);

    //Just to find the last set of detected directories.
    foundPaths.addAll(watcher.find());

    checkCorrectness(paths, foundPaths, (ScheduledExecutorService)Whitebox.getInternalState(watcher, "executor"));
  }

  @Test
  public void testAsyncDirFinderShuffledDirsCreatedPeriodically() throws Exception {
    String testDataDir = "target" + File.separatorChar + UUID.randomUUID().toString();
    Files.createDirectories(Paths.get(testDataDir));
    final Collection<Path> paths = Arrays.asList(
        Paths.get(testDataDir + File.separatorChar + "d1"),
        Paths.get(testDataDir + File.separatorChar + "d2"),
        Paths.get(testDataDir + File.separatorChar + "d3"),
        Paths.get(testDataDir + File.separatorChar + "d4"),
        Paths.get(testDataDir + File.separatorChar + "d5"),
        Paths.get(testDataDir + File.separatorChar + "d6"),
        Paths.get(testDataDir + File.separatorChar + "d7"),
        Paths.get(testDataDir + File.separatorChar + "d8"),

        Paths.get(testDataDir + File.separatorChar + "d1" + File.separatorChar + "d11"),
        Paths.get(testDataDir + File.separatorChar + "d1" + File.separatorChar + "d12"),

        Paths.get(testDataDir + File.separatorChar + "d2" + File.separatorChar + "d21" + File.separatorChar + "d211"),
        Paths.get(testDataDir + File.separatorChar + "d2" + File.separatorChar + "d22" + File.separatorChar + "d221"),
        Paths.get(testDataDir + File.separatorChar + "d2" + File.separatorChar + "d23" + File.separatorChar + "d231"),

        Paths.get(testDataDir + File.separatorChar + "d3" + File.separatorChar + "d31"),

        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"
            + File.separatorChar + "d4111"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"
            + File.separatorChar + "d4112"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"
            + File.separatorChar + "d4111" + File.separatorChar + "d41111"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"
            + File.separatorChar + "d4111" + File.separatorChar + "d41112"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"
            + File.separatorChar + "d4111" + File.separatorChar + "d41111" + File.separatorChar + "d411111"),
        Paths.get(testDataDir + File.separatorChar + "d4" + File.separatorChar + "d41" + File.separatorChar + "d411"
            + File.separatorChar + "d4111" + File.separatorChar + "d41112" + File.separatorChar + "d411111")

    );
    DirectoryPathCreationWatcher watcher = new DirectoryPathCreationWatcher(paths, 1);

    ScheduledExecutorService directoryCreationExecutor = new SafeScheduledExecutorService(1, "Directory Creator");
    DirectoryCreationWorker worker = new DirectoryCreationWorker(paths, true);
    directoryCreationExecutor.scheduleAtFixedRate(worker, 0, 100, TimeUnit.MILLISECONDS);

    Set<Path> foundPaths = new HashSet<Path>();
    while (!worker.getRemainingPathsToCreate().isEmpty()) {
      Set<Path> foundPathsTillNow = watcher.find();
      foundPaths.addAll(foundPathsTillNow);
      Thread.sleep(100);
    }

    directoryCreationExecutor.shutdownNow();
    directoryCreationExecutor.awaitTermination(1000, TimeUnit.MILLISECONDS);

    Thread.sleep(paths.size() * 100 + 1000);

    //Just to find the last set of detected directories.
    foundPaths.addAll(watcher.find());

    checkCorrectness(paths, foundPaths, (ScheduledExecutorService)Whitebox.getInternalState(watcher, "executor"));
  }
}
