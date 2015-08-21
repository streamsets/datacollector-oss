/**
 * (c) 2015 StreamSets, Inc. All rights reserved. May not
 * be copied, modified, or distributed in whole or part without
 * written consent of StreamSets, Inc.
 */
package com.streamsets.pipeline.stage.origin.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;

public class AmazonS3Util {

  static List<S3ObjectSummary> listObjectsChronologically(AmazonS3Client s3Client, S3Config config, int fetchSize)
    throws AmazonClientException {

    TreeSet<S3ObjectSummary> treeSet = new TreeSet<>(
      new Comparator<S3ObjectSummary>() {
        @Override
        public int compare(S3ObjectSummary o1, S3ObjectSummary o2) {
          int result = o1.getLastModified().compareTo(o2.getLastModified());
          if(result != 0) {
            //same modified time. Use name to sort
            return result;
          }
          return o1.getKey().compareTo(o2.getKey());
        }
      });

    ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
    listObjectsRequest
      .withBucketName(config.bucket)
      .withPrefix(config.folder + config.prefix)
      .withDelimiter(config.delimiter);

    ObjectListing objectListing;

    do {
      objectListing = s3Client.listObjects(listObjectsRequest);
      treeSet.addAll(objectListing.getObjectSummaries());
      while(treeSet.size() > fetchSize) {
        treeSet.pollLast();
      }
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    } while (objectListing.isTruncated());

    return new ArrayList<>(treeSet);
  }

  static void move(AmazonS3Client s3Client, String srcBucket, String sourceKey, String destBucket,
                          String destKey) throws AmazonClientException {
    CopyObjectRequest cp = new CopyObjectRequest(srcBucket, sourceKey, destBucket, destKey);
    s3Client.copyObject(cp);
    s3Client.deleteObject(new DeleteObjectRequest(srcBucket, sourceKey));
  }

  static S3Object getObject(AmazonS3Client s3Client, String bucket, String objectKey)
    throws AmazonClientException {
    return s3Client.getObject(bucket, objectKey);
  }

  static S3ObjectSummary getObjectSummary(AmazonS3Client s3Client, String bucket, String objectKey) {
    S3ObjectSummary s3ObjectSummary = null;
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucket).withPrefix(objectKey);
    ObjectListing objectListing;

    do {
      objectListing = s3Client.listObjects(listObjectsRequest);
      for (S3ObjectSummary s : objectListing.getObjectSummaries()) {
        if(s.getKey().equals(objectKey)) {
          s3ObjectSummary = s;
          break;
        }
      }
      if(s3ObjectSummary != null) {
        break;
      }
      listObjectsRequest.setMarker(objectListing.getNextMarker());
    } while (objectListing.isTruncated());

    return s3ObjectSummary;
  }
}
