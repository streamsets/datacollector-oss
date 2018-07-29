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
package com.streamsets.pipeline.stage.common;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

public class TestUtil {

  public static final String TEST_STRING = "Hello StreamSets";
  private static final String MIME = "text/plain";

  public static int getFreePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }

  public static void createBucket(AmazonS3 s3client, String bucketName) {
    if(s3client.doesBucketExist(bucketName)) {
      for(S3ObjectSummary s : S3Objects.inBucket(s3client, bucketName)) {
        s3client.deleteObject(bucketName, s.getKey());
      }
      s3client.deleteBucket(bucketName);
    }
    Assert.assertFalse(s3client.doesBucketExist(bucketName));
    // Note that CreateBucketRequest does not specify region. So bucket is
    // bucketName
    s3client.createBucket(new CreateBucketRequest(bucketName));
  }

  public static List<Record> createStringRecords(String bucket) {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("s", "s:" + i, (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create((TEST_STRING + i)));
      r.getHeader().setAttribute("bucket", bucket);
      records.add(r);
    }
    return records;
  }

  public static void assertStringRecords(AmazonS3 s3client, String bucket, String prefix) throws IOException {
    //check that prefix contains 1 file
    ObjectListing objectListing = s3client.listObjects(bucket, prefix);
    Assert.assertEquals(1, objectListing.getObjectSummaries().size());

    for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
      Assert.assertTrue(objectSummary.getKey().endsWith(".txt"));
    }

    S3ObjectSummary objectSummary = objectListing.getObjectSummaries().get(0);

    //get contents of file and check data - should have 9 lines
    S3Object object = s3client.getObject(bucket, objectSummary.getKey());
    S3ObjectInputStream objectContent = object.getObjectContent();

    List<String> stringList = IOUtils.readLines(objectContent);
    Assert.assertEquals(9, stringList.size());
    for (int i = 0; i < 9; i++) {
      Assert.assertEquals("\"" + TestUtil.TEST_STRING + i + "\"", stringList.get(i));
    }
  }

}
