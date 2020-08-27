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
package com.streamsets.pipeline.stage.common.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.UnknownHostException;

public class TestAwsS3ConnectionVerifier {

  @Test
  public void testCheckConnection() throws Exception {
    AmazonS3 s3Client = Mockito.mock(AmazonS3.class);
    AwsS3ConnectionVerifier.checkConnection(s3Client);
  }

  @Test
  public void testCheckConnectionSomeUnknownHostException() throws Exception {
    AmazonS3 s3Client = Mockito.mock(AmazonS3.class);
    SdkClientException ex = new SdkClientException(new UnknownHostException());
    Mockito.when(s3Client.doesBucketExistV2(Mockito.anyString()))
        .thenThrow(ex).thenThrow(ex).thenThrow(ex).thenThrow(ex).thenReturn(false);
    AwsS3ConnectionVerifier.checkConnection(s3Client);
    Mockito.verify(s3Client, Mockito.times(5)).doesBucketExistV2(Mockito.anyString());
  }

  @Test
  public void testCheckConnectionTooManyUnknownHostException() throws Exception {
    AmazonS3 s3Client = Mockito.mock(AmazonS3.class);
    SdkClientException ex = new SdkClientException(new UnknownHostException());
    Mockito.when(s3Client.doesBucketExistV2(Mockito.anyString())).thenThrow(ex);
    try {
      AwsS3ConnectionVerifier.checkConnection(s3Client);
    } catch (SdkClientException e) {
      Assert.assertEquals(ex, e);
    }
    Mockito.verify(s3Client, Mockito.times(5)).doesBucketExistV2(Mockito.anyString());
  }

  @Test
  public void testCheckConnectionOtherException() throws Exception {
    AmazonS3 s3Client = Mockito.mock(AmazonS3.class);
    SdkClientException ex = new SdkClientException(new AmazonS3Exception("foo"));
    Mockito.when(s3Client.doesBucketExistV2(Mockito.anyString())).thenThrow(ex);
    try {
      AwsS3ConnectionVerifier.checkConnection(s3Client);
    } catch (SdkClientException e) {
      Assert.assertEquals(ex, e);
    }
    Mockito.verify(s3Client, Mockito.times(1)).doesBucketExistV2(Mockito.anyString());
  }
}
