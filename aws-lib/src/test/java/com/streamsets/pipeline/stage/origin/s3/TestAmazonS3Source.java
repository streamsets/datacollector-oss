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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.stage.common.AmazonS3TestSuite;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import org.junit.Assert;
import org.junit.Test;

public class TestAmazonS3Source extends AmazonS3TestSuite {

  @Test
  public void testNormalizePrefix() {
    String prefix = "/";
    String delimiter = "/";
    prefix = AWSUtil.normalizePrefix(prefix, delimiter);
    Assert.assertEquals("", prefix);

    prefix = "/foo";
    delimiter = "/";
    prefix = AWSUtil.normalizePrefix(prefix, delimiter);
    Assert.assertEquals("foo/", prefix);
  }

  @Test
  public void testToStringExcelFile() {
    S3Offset s3Offset = new S3Offset("test.xlsx", "test sheet::1234", "0dd65bf073ad061", "1534360");
    Assert.assertEquals("test.xlsx::test sheet:\\:1234::0dd65bf073ad061::1534360", s3Offset.toString());
  }

  @Test
  public void testToStringTextFile() {
    S3Offset s3Offset = new S3Offset("test.txt", "1234", "0dd65bf073ad061", "1534360");
    Assert.assertEquals("test.txt::1234::0dd65bf073ad061::1534360", s3Offset.toString());
  }

  @Test
  public void testFromStringExcelFile() throws Exception {
    String offset = "FL_insurance.xlsx::Sheet 1 - FL_insurance:\\:1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    S3Offset s3Offset = S3Offset.fromString(offset);
    Assert.assertEquals("FL_insurance.xlsx", s3Offset.getKey());
    Assert.assertEquals("Sheet 1 - FL_insurance::1000", s3Offset.getOffset());
    Assert.assertEquals("0dd65bf073ad0616a91901c9349dd5a4", s3Offset.geteTag());
    Assert.assertEquals("1534360", s3Offset.getTimestamp());
  }

  @Test
  public void testFromStringTextFile() throws Exception {
    String offset = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    S3Offset s3Offset = S3Offset.fromString(offset);
    Assert.assertEquals("FL_insurance.txt", s3Offset.getKey());
    Assert.assertEquals("1000", s3Offset.getOffset());
    Assert.assertEquals("0dd65bf073ad0616a91901c9349dd5a4", s3Offset.geteTag());
    Assert.assertEquals("1534360", s3Offset.getTimestamp());
  }
}
