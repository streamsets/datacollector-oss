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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

public class AmazonS3Util {

  public static final int BATCH_SIZE = 1000;

  /**
   * Lists objects from AmazonS3 in chronological order [lexicographical order if 2 files have same timestamp] which are
   * later than or equal to the timestamp of the previous offset object
   *
   * @param s3Client
   * @param s3ConfigBean
   * @param pathMatcher glob patterns to match file name against
   * @param s3Offset current offset which provides the timestamp of the previous object
   * @param fetchSize number of objects to fetch in one go
   * @return
   * @throws AmazonClientException
   */
  static List<S3ObjectSummary> listObjectsChronologically(AmazonS3Client s3Client, S3ConfigBean s3ConfigBean,
                                                          PathMatcher pathMatcher, AmazonS3Source.S3Offset s3Offset,
                                                          int fetchSize)
    throws AmazonClientException {

    //Algorithm:
    // - Full scan all objects that match the file name pattern and which are later than the file in the offset
    // - Select the oldest "fetchSize" number of files and return them.
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

    S3Objects s3ObjectSummaries = S3Objects
      .withPrefix(s3Client, s3ConfigBean.s3Config.bucket, s3ConfigBean.s3Config.folder)
      .withBatchSize(BATCH_SIZE);
    for(S3ObjectSummary s : s3ObjectSummaries) {
      String fileName = s.getKey().substring(s3ConfigBean.s3Config.folder.length(), s.getKey().length());
      if(!fileName.isEmpty()) {
        //fileName can be empty.
        //If the user manually creates a folder "myFolder/mySubFolder" in bucket "myBucket" and uploads "myObject",
        // then the first objects returned here are:
        // myFolder/mySubFolder
        // myFolder/mySubFolder/myObject
        //
        // All is good when pipeline is run but preview returns with no data. So we should ignore the empty file as it
        // has no data
        if (pathMatcher.matches(Paths.get(fileName)) && isEligible(s, s3Offset)) {
          treeSet.add(s);
        }
        if (treeSet.size() > fetchSize) {
          treeSet.pollLast();
        }
      }
    }

    return new ArrayList<>(treeSet);
  }

  private static boolean isEligible(S3ObjectSummary s, AmazonS3Source.S3Offset s3Offset) {
    return (s.getLastModified().compareTo(new Date(Long.parseLong(s3Offset.getTimestamp()))) > 0) ||
      ((s.getLastModified().compareTo(new Date(Long.parseLong(s3Offset.getTimestamp()))) == 0) &&
          (s.getKey().compareTo(s3Offset.getKey()) > 0));
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

  static S3Object getObjectRange(AmazonS3Client s3Client, String bucket, String objectKey, long range)
    throws AmazonClientException {
    GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, objectKey).withRange(0, range);
    return s3Client.getObject(getObjectRequest);
  }

  static S3ObjectSummary getObjectSummary(AmazonS3Client s3Client, String bucket, String objectKey) {
    S3ObjectSummary s3ObjectSummary = null;
    S3Objects s3ObjectSummaries = S3Objects
      .withPrefix(s3Client, bucket, objectKey);
    for(S3ObjectSummary s : s3ObjectSummaries) {
      if(s.getKey().equals(objectKey)) {
        s3ObjectSummary = s;
        break;
      }
    }
    return s3ObjectSummary;
  }
}
