/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.security;

import com.streamsets.pipeline.api.Configuration;
import com.streamsets.pipeline.api.service.dataformats.WholeFileChecksumAlgorithm;
import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestTempKeytabManager {

  private static final String tempDirProperty = "my.tempKeytab.dir";
  // this should NOT be returned
  private static final String tempDirDefaultValue = System.getProperty("java.io.tmpdir") + "/keytabManager";
  private static final String subdirName = "tempKeytabs";

  private final Set<PosixFilePermission> existingDirPerms;

  public TestTempKeytabManager(Set<PosixFilePermission> existingDirPerms) {
    this.existingDirPerms = existingDirPerms;
  }

  @Parameterized.Parameters(name = "Existing Dir Permissions : {0}")
  public static Collection<Object[]> data() throws Exception {
    List<Object[]> finalData = new ArrayList<>();
    // this tests an "upgrade" (i.e. the existing temp keytab dir is user-only)
    finalData.add(new Object[] {TempKeytabManager.USER_ONLY_PERM});
    // this tests a new installation (i.e. there is no existing temp keytab dir)
    finalData.add(new Object[] {null});
    return finalData;
  }

  @Test
  public void testTempKeytabManager() throws IOException {
    final Path tempDir = Files.createTempDirectory("tempKeytabDir");
    if (existingDirPerms != null) {
      // apply existing directory permissions to this new temp dir for the test
      Files.setPosixFilePermissions(tempDir, existingDirPerms);
    }
    tempDir.toFile().deleteOnExit();

    final Configuration config = Mockito.mock(Configuration.class);
    Mockito.when(config.get(Matchers.eq(tempDirProperty), Matchers.anyString())).thenReturn(tempDir.toString());
    final TempKeytabManager keytabManager = new TempKeytabManager(
        config,
        tempDirProperty,
        tempDirDefaultValue,
        subdirName
    );
    keytabManager.ensureKeytabTempDir();

    // not valid Kerberos keytab bytes, but for this test it doesn't matter
    final byte[] dummyKeytabBinaryData = new byte[] {1, 1, 2, 3, 5, 8, 13};
    final String keytabEncoded = Base64.getEncoder().encodeToString(dummyKeytabBinaryData);

    // write the data to the temp keytab; this should succeed
    final String keytabName = keytabManager.createTempKeytabFile(keytabEncoded);

    // get the file and ensure the contents are correct
    final Path tempKeytabPath = keytabManager.getTempKeytabPath(keytabName);
    assertTrue(Files.exists(tempKeytabPath));
    final byte[] keytabContents = IOUtils.toByteArray(new FileReader(tempKeytabPath.toFile()));
    assertThat(keytabContents, equalTo(dummyKeytabBinaryData));

    // make sure directory permissions are correct
    // the subdir should be globally writeable at this point
    final Path kafkaKeytabsDir = tempDir.resolve(subdirName);
    assertTrue(Files.exists(kafkaKeytabsDir));
    assertTrue(Files.isDirectory(kafkaKeytabsDir));
    final Set<PosixFilePermission> topLevelPerms = Files.getPosixFilePermissions(kafkaKeytabsDir);
    assertThat(topLevelPerms, CoreMatchers.equalTo(TempKeytabManager.GLOBAL_ALL_PERM));

    // current user level subdir under that should be restricted to user only
    final Path userLevelDir = kafkaKeytabsDir.resolve(System.getProperty("user.name"));
    assertTrue(Files.exists(userLevelDir));
    assertTrue(Files.isDirectory(userLevelDir));
    final Set<PosixFilePermission> userLevelPerms = Files.getPosixFilePermissions(userLevelDir);
    assertThat(userLevelPerms, CoreMatchers.equalTo(TempKeytabManager.USER_ONLY_PERM));

    // delete the temp keytab
    keytabManager.deleteTempKeytabFileIfExists(keytabName);

    // the keytab file should have been deleted
    assertFalse(Files.exists(tempKeytabPath));

  }
}
