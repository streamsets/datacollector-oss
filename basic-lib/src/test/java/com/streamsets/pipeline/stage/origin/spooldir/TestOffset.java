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
package com.streamsets.pipeline.stage.origin.spooldir;

import com.streamsets.pipeline.sdk.DataCollectorServicesUtils;
import com.streamsets.pipeline.lib.dirspooler.Offset;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestOffset {
  private static final String NULL_FILE_OFFSET = "NULL_FILE_ID-48496481-5dc5-46ce-9c31-3ab3e034730c";

  @BeforeClass
  public static void setupTables() {
    DataCollectorServicesUtils.loadDefaultServices();
  }

  @Test
  public void getOffsetMethods() throws Exception {
    Offset offset = new Offset(Offset.VERSION_ONE, null);
    Assert.assertNull(offset.getRawFile());

    offset = new Offset(Offset.VERSION_ONE, "x::");
    Assert.assertEquals("x", offset.getFile());

    offset = new Offset(Offset.VERSION_ONE, null);
    Assert.assertEquals("0", offset.getOffset());

    offset = new Offset(Offset.VERSION_ONE, "x");
    Assert.assertEquals("0", offset.getOffset());

    offset = new Offset(Offset.VERSION_ONE, "x::1");
    Assert.assertEquals("x", offset.getFile());
    Assert.assertEquals("1", offset.getOffset());
  }

  @Test
  public void getSubDirectoryOffsetMethods() throws Exception {
    Offset offset = new Offset(Offset.VERSION_ONE, null);
    Assert.assertNull(offset.getRawFile());

    offset = new Offset(Offset.VERSION_ONE, "/dir1/x::");
    Assert.assertTrue(offset.getFile().endsWith("/dir1/x"));
    Assert.assertEquals("0", offset.getOffset());

    offset = new Offset(Offset.VERSION_ONE, null);
    Assert.assertEquals("0", offset.getOffset());

    offset = new Offset(Offset.VERSION_ONE, "/dir1/x::1");
    Assert.assertTrue(offset.getFile().endsWith("/dir1/x"));
    Assert.assertEquals("1", offset.getOffset());
  }

  @Test
  public void testOffsetMethods() throws Exception {
    Offset offset = new Offset(Offset.VERSION_ONE, NULL_FILE_OFFSET + "::0");
    Assert.assertEquals(NULL_FILE_OFFSET, offset.getFile());

    offset = new Offset(Offset.VERSION_ONE, "file1::0");
    Assert.assertEquals("file1", offset.getFile());
    Assert.assertEquals("0", offset.getOffset());

    offset = new Offset(Offset.VERSION_ONE, NULL_FILE_OFFSET+"::0");
    Assert.assertEquals("0", offset.getOffset());

    offset = new Offset(Offset.VERSION_ONE, NULL_FILE_OFFSET+"::0");
    Assert.assertNull(offset.getRawFile());
  }

  @Test
  public void testSubDirectoryOffsetMethods() throws Exception {
    Offset offset = new Offset(Offset.VERSION_ONE, NULL_FILE_OFFSET + "::0");
    Assert.assertEquals(NULL_FILE_OFFSET, offset.getFile());
    Assert.assertEquals("0", offset.getOffset());

    offset = new Offset(Offset.VERSION_ONE, "/dir1/file1::0");
    Assert.assertEquals("/dir1/file1", offset.getFile());
    Assert.assertEquals("0", offset.getOffset());

    offset = new Offset(Offset.VERSION_ONE, NULL_FILE_OFFSET);
    Assert.assertEquals(NULL_FILE_OFFSET, offset.getFile());
    Assert.assertEquals("0", offset.getOffset());

    offset = new Offset(Offset.VERSION_ONE, NULL_FILE_OFFSET);
    Assert.assertEquals(NULL_FILE_OFFSET, offset.getFile());
    Assert.assertEquals("0", offset.getOffset());
  }

  @Test
  public void testVersionUpgrade() throws Exception {
    final String fileName = "test.log";
    final String initialOffset = "0";

    final String offsetV1 = fileName + "::" + initialOffset;
    Offset offset = new Offset(Offset.VERSION_ONE, offsetV1);
    String offsetString = offset.getOffsetString();

    Offset offsetV2 = new Offset(Offset.VERSION_ONE, offset.getFile(), offsetString);

    Assert.assertEquals(fileName, offsetV2.getFile());
    Assert.assertEquals(initialOffset, offsetV2.getOffset());
  }

  @Test
  public void testWithNullOffset() throws Exception {
    final String fileName = "retail.tar.gz";
    final String offsetString = "{\"POS\":null}";

    Offset offset = new Offset(Offset.VERSION_ONE, fileName, offsetString);

    Assert.assertEquals(fileName, offset.getFile());
    Assert.assertEquals("0", offset.getOffset());
  }
}
