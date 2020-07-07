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
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestTempKeytabManager {

  private static final String tempDirProperty = "my.tempKeytab.dir";
  private static final String tempDirDefaultValue = System.getProperty("java.io.tmpdir") + "/keytabManager";
  private static final String subdirName = "tempKeytabs";

  @Test
  public void testTempKeytabManager() throws IOException {
    final Path tempDir = Files.createTempDirectory("tempKeytabDir");
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

    // delete the temp keytab
    keytabManager.deleteTempKeytabFileIfExists(keytabName);

    // the keytab file should have been deleted
    assertFalse(Files.exists(tempKeytabPath));

  }
}
