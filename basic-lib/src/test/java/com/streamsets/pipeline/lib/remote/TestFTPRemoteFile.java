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

import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class TestFTPRemoteFile {

  @Test
  public void testCreateAndCommitOutputStream() throws Exception {
    String name = "file.txt";
    String filePath = "/some/path/";
    FileObject fileObject = Mockito.mock(FileObject.class);
    FileName fileName = Mockito.mock(FileName.class);
    Mockito.when(fileObject.getName()).thenReturn(fileName);
    Mockito.when(fileName.getBaseName()).thenReturn(name);
    FileObject parentFileObject = Mockito.mock(FileObject.class);
    FileObject tempFileObject = Mockito.mock(FileObject.class);
    Mockito.when(fileObject.getParent()).thenReturn(parentFileObject);
    Mockito.when(parentFileObject.resolveFile(Mockito.any())).thenReturn(tempFileObject);
    FileContent tempFileContent = Mockito.mock(FileContent.class);
    Mockito.when(tempFileObject.getContent()).thenReturn(tempFileContent);

    FTPRemoteFile file = new FTPRemoteFile(filePath + name, 0L, fileObject);

    try {
      file.commitOutputStream();
      Assert.fail("Expected IOException because called commitOutputStream before createOutputStream");
    } catch (IOException ioe) {
      Assert.assertEquals("Cannot commit " + filePath + name + " - it must be written first", ioe.getMessage());
    }

    file.createOutputStream();
    Mockito.verify(parentFileObject).resolveFile("_tmp_" + name);
    Mockito.verify(tempFileContent).getOutputStream();

    file.commitOutputStream();
    Mockito.verify(tempFileObject).moveTo(fileObject);
  }
}
