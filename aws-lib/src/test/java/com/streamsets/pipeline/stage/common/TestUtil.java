/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.sdk.RecordCreator;
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

  public static List<Record> createStringRecords() {
    List<Record> records = new ArrayList<>(9);
    for (int i = 0; i < 9; i++) {
      Record r = RecordCreator.create("s", "s:" + i, (TEST_STRING + i).getBytes(), MIME);
      r.set(Field.create((TEST_STRING + i)));
      records.add(r);
    }
    return records;
  }

}
