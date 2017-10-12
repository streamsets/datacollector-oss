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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.common.InterfaceAudience;
import com.streamsets.pipeline.common.InterfaceStability;
import com.streamsets.pipeline.lib.util.AntPathMatcher;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class AmazonS3Util {

  public static final int BATCH_SIZE = 1000;

  private AmazonS3Util() {}

  /**
   * Lists objects from AmazonS3 in lexicographical order
   *
   * @param s3Client
   * @param s3ConfigBean
   * @param pathMatcher glob patterns to match file name against
   * @param s3Offset current offset which provides the key name of the previous object
   * @param fetchSize number of objects to fetch in one go
   * @return
   * @throws AmazonClientException
   */
  static List<S3ObjectSummary> listObjectsLexicographically(
      AmazonS3 s3Client,
      S3ConfigBean s3ConfigBean,
      AntPathMatcher pathMatcher,
      AmazonS3Source.S3Offset s3Offset,
      int fetchSize
  ) {
    // Incrementally scan objects after the marker (s3Offset).
    List<S3ObjectSummary> list = new ArrayList<>(fetchSize);

    ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
    listObjectsRequest.setBucketName(s3ConfigBean.s3Config.bucket);
    listObjectsRequest.setPrefix(s3ConfigBean.s3Config.commonPrefix);
    listObjectsRequest.setMaxKeys(BATCH_SIZE);
    if (s3Offset.getKey() != null) {
      listObjectsRequest.setMarker(s3Offset.getKey());
    }

    ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

    while (true) {
      for (S3ObjectSummary s : objectListing.getObjectSummaries()) {
        String fullPrefix = s.getKey();
        String remainingPrefix = fullPrefix.substring(s3ConfigBean.s3Config.commonPrefix.length(), fullPrefix.length());
        if (!remainingPrefix.isEmpty()) {
          if (pathMatcher.match(s3ConfigBean.s3FileConfig.prefixPattern, remainingPrefix)) {
            list.add(s);
          }
          // We've got enough objects.
          if (list.size() == fetchSize) {
            return list;
          }
        }
      }
      // Listing is complete. No more objects to be listed.
      if (!objectListing.isTruncated()) {
        break;
      }
      objectListing = s3Client.listNextBatchOfObjects(objectListing);
    }

    return list;
  }

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
  static List<S3ObjectSummary> listObjectsChronologically(
      AmazonS3 s3Client,
      S3ConfigBean s3ConfigBean,
      AntPathMatcher pathMatcher,
      AmazonS3Source.S3Offset s3Offset,
      int fetchSize
  ) {

    //Algorithm:
    // - Full scan all objects that match the file name pattern and which are later than the file in the offset
    // - Select the oldest "fetchSize" number of files and return them.
    TreeSet<S3ObjectSummary> treeSet = new TreeSet<>((o1, o2) -> {
      int result = o1.getLastModified().compareTo(o2.getLastModified());
      if(result != 0) {
        //same modified time. Use name to sort
        return result;
      }
      return o1.getKey().compareTo(o2.getKey());
    });

    S3Objects s3ObjectSummaries = S3Objects
      .withPrefix(s3Client, s3ConfigBean.s3Config.bucket, s3ConfigBean.s3Config.commonPrefix)
      .withBatchSize(BATCH_SIZE);
    for (S3ObjectSummary s : s3ObjectSummaries) {
      String fullPrefix = s.getKey();
      String remainingPrefix = fullPrefix.substring(s3ConfigBean.s3Config.commonPrefix.length(), fullPrefix.length());
      if (!remainingPrefix.isEmpty()) {
        // remainingPrefix can be empty.
        // If the user manually creates a prefix "myFolder/mySubFolder" in bucket "myBucket" and uploads "myObject",
        // then the first objects returned here are:
        // myFolder/mySubFolder
        // myFolder/mySubFolder/myObject
        //
        // All is good when pipeline is run but preview returns with no data. So we should ignore the empty file as it
        // has no data
        if (pathMatcher.match(s3ConfigBean.s3FileConfig.prefixPattern, remainingPrefix) && isEligible(s, s3Offset)) {
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

    //The object is eligible if
    //1. The timestamp is greater than that of the current object in offset
    //2. The timestamp is same but the name is lexicographically greater than the current object [can happen when multiple objects are uploaded in one go]
    //3. Same timestamp, same name [same as the current object in offset], eligible if it was not completely processed [offset != -1]

    boolean isEligible = false;
    if(s.getLastModified().compareTo(new Date(Long.parseLong(s3Offset.getTimestamp()))) > 0) {
      isEligible = true;
    } else if(s.getLastModified().compareTo(new Date(Long.parseLong(s3Offset.getTimestamp()))) == 0) {
      //same timestamp
      //compare names
      if(s.getKey().compareTo(s3Offset.getKey()) > 0) {
        isEligible = true;
      } else if (s.getKey().compareTo(s3Offset.getKey()) == 0 && !"-1".equals(s3Offset.getOffset())) {
        //same time stamp, same name
        //If the current offset is not -1, return the file. It means the previous file was partially processed.
        isEligible = true;
      }
    }
    return isEligible;
  }

  static void copy(
      AmazonS3 s3Client,
      String srcBucket,
      String sourceKey,
      String destBucket,
      String destKey,
      boolean isMove
  ) {
    CopyObjectRequest cp = new CopyObjectRequest(srcBucket, sourceKey, destBucket, destKey);
    s3Client.copyObject(cp);
    if (isMove) {
      s3Client.deleteObject(new DeleteObjectRequest(srcBucket, sourceKey));
    }
  }

  static S3Object getObject(
      AmazonS3 s3Client,
      String bucket,
      String objectKey,
      boolean useSSE,
      CredentialValue customerKey,
      CredentialValue customerKeyMd5
  ) throws StageException {
    GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, objectKey);
    if (useSSE) {
      SSECustomerKey sseCustomerKey = new SSECustomerKey(customerKey.get());
      sseCustomerKey.setMd5(customerKeyMd5.get());
      getObjectRequest.setSSECustomerKey(sseCustomerKey);
    }
    return s3Client.getObject(getObjectRequest);
  }

  static Map<String, Object> getMetaData(S3Object s3Object) {
    Map<String, Object> metaDataMap = new HashMap<>();

    // put the metadata of S3 Object
    metaDataMap.put(Headers.CACHE_CONTROL, s3Object.getObjectMetadata().getCacheControl());
    metaDataMap.put(Headers.CONTENT_DISPOSITION, s3Object.getObjectMetadata().getContentDisposition());
    metaDataMap.put(Headers.CONTENT_ENCODING, s3Object.getObjectMetadata().getContentEncoding());
    metaDataMap.put(Headers.CONTENT_LENGTH, s3Object.getObjectMetadata().getContentLength());
    metaDataMap.put(Headers.CONTENT_RANGE, s3Object.getObjectMetadata().getInstanceLength());
    metaDataMap.put(Headers.CONTENT_MD5, s3Object.getObjectMetadata().getContentMD5());
    metaDataMap.put(Headers.CONTENT_TYPE, s3Object.getObjectMetadata().getContentType());
    metaDataMap.put(Headers.EXPIRES, s3Object.getObjectMetadata().getExpirationTime());
    metaDataMap.put(Headers.ETAG, s3Object.getObjectMetadata().getETag());
    metaDataMap.put(Headers.LAST_MODIFIED, s3Object.getObjectMetadata().getLastModified());

    // put user metadata
    Map<String, String> userMetaMap = s3Object.getObjectMetadata().getUserMetadata();
    if(userMetaMap != null) {
      for (Map.Entry<String, String> entry : userMetaMap.entrySet()) {
        if (entry.getValue() != null) {
          metaDataMap.put(entry.getKey(), entry.getValue());
        }
      }
    }
    return metaDataMap;
  }

  static S3Object getObjectRange(
      AmazonS3 s3Client,
      String bucket,
      String objectKey,
      long range,
      boolean useSSE,
      CredentialValue customerKey,
      CredentialValue customerKeyMd5
  ) throws StageException {
    GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, objectKey).withRange(0, range);
    if (useSSE) {
      SSECustomerKey sseCustomerKey = new SSECustomerKey(customerKey.get());
      sseCustomerKey.setMd5(customerKeyMd5.get());
      getObjectRequest.setSSECustomerKey(sseCustomerKey);
    }
    return s3Client.getObject(getObjectRequest);
  }

  static S3ObjectSummary getObjectSummary(AmazonS3 s3Client, String bucket, String objectKey) {
    S3ObjectSummary s3ObjectSummary = null;
    S3Objects s3ObjectSummaries = S3Objects.withPrefix(s3Client, bucket, objectKey);
    for (S3ObjectSummary s : s3ObjectSummaries) {
      if (s.getKey().equals(objectKey)) {
        s3ObjectSummary = s;
        break;
      }
    }
    return s3ObjectSummary;
  }
}
