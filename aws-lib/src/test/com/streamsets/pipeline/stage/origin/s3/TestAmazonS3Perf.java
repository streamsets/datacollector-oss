/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
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
