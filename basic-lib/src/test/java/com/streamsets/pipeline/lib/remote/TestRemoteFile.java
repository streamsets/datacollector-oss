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

package com.streamsets.pipeline.lib.remote;

import org.junit.Assert;
import org.junit.Test;

public class TestRemoteFile {

  @Test
  public void testGetAbsolutePathName_onlyHost() throws Exception {
    String expected = "/file.txt";
    String address = "ftp://host:22/";
    String fileName = "/file.txt";

    Assert.assertEquals(expected, RemoteFile.getAbsolutePathFileName(address, fileName));
  }

  @Test
  public void testGetAbsolutePathName_includeOneDirectory() throws Exception {
    String expected = "/directory1/file.txt";
    String address = "ftp://host:22/directory1";
    String fileName = "/file.txt";

    Assert.assertEquals(expected, RemoteFile.getAbsolutePathFileName(address, fileName));
  }

  @Test
  public void testGetAbsolutePathName_includeMultiplesDirectory() throws Exception {
    String expected = "/directory1/directory2/file.txt";
    String address = "ftp://host:22/directory1/directory2";
    String fileName = "/file.txt";

    Assert.assertEquals(expected, RemoteFile.getAbsolutePathFileName(address, fileName));
  }

  @Test
  public void testGetAbsolutePathName_includeMultiplesDirectory_compositeFile() throws Exception {
    String expected = "/directory1/directory2/directory3/file.txt";
    String address = "ftp://host:22/directory1/directory2";
    String fileName = "/directory3/file.txt";

    Assert.assertEquals(expected, RemoteFile.getAbsolutePathFileName(address, fileName));
  }

  @Test
  public void testGetAbsolutePathName_directoryWithTrailingSlash() throws Exception {
    String expected = "/directory1/file.txt";
    String address = "ftp://host:22/directory1/";
    String fileName = "/file.txt";

    Assert.assertEquals(expected, RemoteFile.getAbsolutePathFileName(address, fileName));
  }

  @Test
  public void testGetAbsolutePathName_onlyHost_queryParameters() throws Exception {
    String expected = "/file.txt";
    String address = "ftp://host:22/?username=username";
    String fileName = "/file.txt";

    Assert.assertEquals(expected, RemoteFile.getAbsolutePathFileName(address, fileName));
  }

  @Test
  public void testGetAbsolutePathName_includeOneDirectory_queryParameters() throws Exception {
    String expected = "/directory1/file.txt";
    String address = "ftp://host:22/directory1?username=username";
    String fileName = "/file.txt";

    Assert.assertEquals(expected, RemoteFile.getAbsolutePathFileName(address, fileName));
  }

  @Test
  public void testGetAbsolutePathName_onlyHost_testGetAbsolutePathName_includeOneDirectory_queryParameters() throws Exception {
    String expected = "/file.txt";
    String address = "ftp://host:22/?username=username";
    String fileName = "/file.txt";

    Assert.assertEquals(expected, RemoteFile.getAbsolutePathFileName(address, fileName));
  }
}
