/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.UUID;

@Ignore
public class TestAmazonS3Perf {

  @Ignore
  @Test
  public void testUpload() {
    AWSCredentials credentials = new BasicAWSCredentials("AKIAJ6S5Q43F4BT6ZJLQ", "tgKMwR5/GkFL5IbkqwABgdpzjEsN7n7qOEkFWgWX");
    AmazonS3Client s3Client = new AmazonS3Client(credentials, new ClientConfiguration());
    s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    s3Client.setRegion(Region.getRegion(Regions.US_WEST_1));

    for(int i = 0; i < 10; i++) {
      InputStream in = new ByteArrayInputStream("Hello World".getBytes());
      PutObjectRequest putObjectRequest = new PutObjectRequest("hari-perf-test",
        "folder1/folder4/" + UUID.randomUUID().toString(), in, new ObjectMetadata());
      s3Client.putObject(putObjectRequest);
    }
  }

  @Ignore
  @Test
  public void testBrowse() {
    AWSCredentials credentials = new BasicAWSCredentials("AKIAJ6S5Q43F4BT6ZJLQ", "tgKMwR5/GkFL5IbkqwABgdpzjEsN7n7qOEkFWgWX");
    AmazonS3Client s3Client = new AmazonS3Client(credentials, new ClientConfiguration());
    s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    s3Client.setRegion(Region.getRegion(Regions.US_WEST_1));

    //Browse all objects in folder1/folder2
    System.out.println("Browse all objects in folder1/folder2");

    int n = 1;
    int count = 0;
    long cummulativeSum = 0;
    for(int i = 0; i < n; i++) {
      long start = System.currentTimeMillis();
      count = 0;
      //error-bucket/error-folder756847
      for (S3ObjectSummary s : S3Objects.withPrefix(s3Client, "hari-perf-test", "folder1/folder-archive/")) {
        System.out.println(s.getKey());
        count++;
      }
      long end = System.currentTimeMillis();
      cummulativeSum += (end - start);
    }
    System.out.println("Time to browse " + count + " objects is : " + cummulativeSum/n + " ms");

  }

}
