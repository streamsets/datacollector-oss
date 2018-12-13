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
package com.streamsets.pipeline.stage.origin.remote;

import net.schmizz.sshj.sftp.RemoteResourceInfo;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSelectInfo;
import org.apache.commons.vfs2.FileType;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestFileFilter {

  @Test
  public void testAccept() {
    FileFilter filter = new FileFilter("a*");
    // Dir but doesn't match the pattern
    Assert.assertTrue(filter.accept(createRemoteResourceInfo("b", false)));
    // File but doesn't match the pattern
    Assert.assertFalse(filter.accept(createRemoteResourceInfo("b", true)));
    // File and does match the pattern
    Assert.assertTrue(filter.accept(createRemoteResourceInfo("abc", true)));
  }

  @Test
  public void testIncludeFile() throws Exception {
    FileFilter filter = new FileFilter("a*");
    // Matches the pattern but Dir
    Assert.assertFalse(filter.includeFile(createFileSelectInfo("abc", false)));
    // File but doesn't match the pattern
    Assert.assertFalse(filter.includeFile(createFileSelectInfo("b", true)));
    // File and does match the pattern
    Assert.assertTrue(filter.includeFile(createFileSelectInfo("abc", true)));
  }

  @Test
  public void testTraverseDescendents() {
    FileFilter filter = new FileFilter("*");
    Assert.assertTrue(filter.traverseDescendents(null));
  }

  @Test
  public void testGlobToRegex() {
    FileFilter filter = new FileFilter("abc*def*");
    Assert.assertEquals("abc.+def.+", filter.getRegex().pattern());

    filter = new FileFilter("abc?def?");
    Assert.assertEquals("abc.{1}+def.{1}+", filter.getRegex().pattern());

    filter = new FileFilter("abc*def?");
    Assert.assertEquals("abc.+def.{1}+", filter.getRegex().pattern());
  }

  @Test
  public void testGlobToRegexSpecialCharacters() {
    FileFilter filter = new FileFilter("abc.def.");
    Assert.assertEquals("abc\\.def\\.", filter.getRegex().pattern());

    String[] illegalPatterns = new String[] {
        ".abc",
        "abc/def",
        "abc~def"};
    for (String illegalPattern : illegalPatterns) {
      try {
        new FileFilter(illegalPattern);
        Assert.fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException e) {
        Assert.assertEquals("Invalid character in file glob", e.getMessage());
      }
    }
  }

  private RemoteResourceInfo createRemoteResourceInfo(String fileNmae, boolean isFile) {
    RemoteResourceInfo resourceInfo = Mockito.mock(RemoteResourceInfo.class);
    Mockito.when(resourceInfo.getName()).thenReturn(fileNmae);
    Mockito.when(resourceInfo.isDirectory()).thenReturn(!isFile);
    Mockito.when(resourceInfo.isRegularFile()).thenReturn(isFile);
    return resourceInfo;
  }

  private FileSelectInfo createFileSelectInfo(String fileName, boolean isFile) throws Exception {
    FileSelectInfo fileSelectInfo = Mockito.mock(FileSelectInfo.class);
    FileObject fileObject = Mockito.mock(FileObject.class);
    Mockito.when(fileSelectInfo.getFile()).thenReturn(fileObject);
    if (isFile) {
      Mockito.when(fileObject.getType()).thenReturn(FileType.FILE);
    } else {
      Mockito.when(fileObject.getType()).thenReturn(FileType.FOLDER);
    }
    FileName fName = Mockito.mock(FileName.class);
    Mockito.when(fileObject.getName()).thenReturn(fName);
    Mockito.when(fName.getBaseName()).thenReturn(fileName);
    return fileSelectInfo;
  }
}
