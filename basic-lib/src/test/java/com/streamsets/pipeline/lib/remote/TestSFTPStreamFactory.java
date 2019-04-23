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

import com.google.common.io.Files;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.EnumSet;

public class TestSFTPStreamFactory extends SSHDUnitTest {

  private RemoteFile remoteFile;

  @Before
  public void setUp() throws Exception {
    File file = testFolder.newFile("file.txt");
    String text = "hello";
    Files.write(text.getBytes(Charset.forName("UTF-8")), file);
    Assert.assertEquals(text, Files.readFirstLine(file, Charset.forName("UTF-8")));

    path = testFolder.getRoot().getAbsolutePath();
    setupSSHD(path);
    SSHClient sshClient = createSSHClient();

    SFTPClient sftpClient = sshClient.newSFTPClient();
    remoteFile = sftpClient.open(file.getName(), EnumSet.of(OpenMode.READ));
    remoteFile = Mockito.spy(remoteFile);
  }

  @Test
  public void testCreateReadAheadInputStream() throws Exception {
    InputStream is = SFTPStreamFactory.createReadAheadInputStream(remoteFile);
    is.read();
    Mockito.verify(remoteFile, Mockito.times(0)).close();
    is.close();
    Mockito.verify(remoteFile, Mockito.times(1)).close();
    is.close();
    Mockito.verify(remoteFile, Mockito.times(1)).close();
  }

  @Test
  public void testCreateInputStream() throws Exception {
    InputStream is = SFTPStreamFactory.createInputStream(remoteFile);
    is.read();
    Mockito.verify(remoteFile, Mockito.times(0)).close();
    is.close();
    Mockito.verify(remoteFile, Mockito.times(1)).close();
    is.close();
    Mockito.verify(remoteFile, Mockito.times(1)).close();
  }

  @Test
  public void testCreateOutputStream() throws Exception {
    OutputStream os = SFTPStreamFactory.createOutputStream(remoteFile);
    os.write(1);
    Mockito.verify(remoteFile, Mockito.times(0)).close();
    os.close();
    Mockito.verify(remoteFile, Mockito.times(1)).close();
    os.close();
    Mockito.verify(remoteFile, Mockito.times(1)).close();
  }
}
