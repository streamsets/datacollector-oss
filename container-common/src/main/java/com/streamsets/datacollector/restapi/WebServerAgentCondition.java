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
package com.streamsets.datacollector.restapi;

import com.google.common.annotations.VisibleForTesting;

import javax.ws.rs.core.Response;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class WebServerAgentCondition {

  private static final AtomicBoolean receivedCredentials = new AtomicBoolean(false);
  private static final CountDownLatch blockOtherTasks = new CountDownLatch(1);
  private static final boolean isCredentialsRequiredPropertyEnabled =
      Boolean.valueOf(System.getProperty("streamsets.cluster.manager.credentials.required", "false"));

  private static boolean shouldCheckForCredentials() {
    return isCredentialsRequiredPropertyEnabled;
  }

  public static boolean canContinue() {
    return !shouldCheckForCredentials() || getReceivedCredentials();
  }

  public static boolean getReceivedCredentials() {
    return receivedCredentials.get();
  }

  public static void setCredentialsReceived() {
    receivedCredentials.set(true);
    blockOtherTasks.countDown();
  }

  public static Response fail() {
    return Response.status(Response.Status.FORBIDDEN).entity("Credentials not received").build();
  }

  public static void waitForCredentials() throws InterruptedException {
    if (shouldCheckForCredentials()) {
      blockOtherTasks.await();
    }
  }

  // When there are multiple tests, we should reset this to ensure that each test behaves as expected.
  @VisibleForTesting
  public static void reset() {
    receivedCredentials.set(false);
  }
}
