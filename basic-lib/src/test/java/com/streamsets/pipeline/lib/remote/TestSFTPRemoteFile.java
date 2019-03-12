/*
 * Copyright 2019 StreamSets Inc.
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
package com.streamsets.pipeline.lib.remote;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class TestSFTPRemoteFile {

  @Test
  public void testCreateAndCommitOutputStream() throws Exception {
    String name = "file.txt";
    String filePath = "/some/path/";
    ChrootSFTPClient sftpClient = Mockito.mock(ChrootSFTPClient.class);

    SFTPRemoteFile file = new SFTPRemoteFile(filePath + name, 0L, sftpClient);

    try {
      file.commitOutputStream();
      Assert.fail("Expected IOException because called commitOutputStream before createOutputStream");
    } catch (IOException ioe) {
      Assert.assertEquals("Cannot commit " + filePath + name + " - it must be written first", ioe.getMessage());
    }

    file.createOutputStream();
    Mockito.verify(sftpClient).openForWriting(filePath + "_tmp_" + name);

    file.commitOutputStream();
    Mockito.verify(sftpClient).rename(filePath + "_tmp_" + name, filePath + name);
  }
}
