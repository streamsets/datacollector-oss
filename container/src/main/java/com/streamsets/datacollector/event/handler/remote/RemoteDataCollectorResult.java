/*
 * Copyright 2018 StreamSets Inc.
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

package com.streamsets.datacollector.event.handler.remote;

import com.streamsets.datacollector.event.dto.AckEvent;

import java.util.concurrent.Future;

/**
 * A class for encapsulating the results of executing a server event (either locally or remotely)
 */
public class RemoteDataCollectorResult {
  private final Future<AckEvent> futureAck;
  private final Object immediateResult;
  private final String errorMessage;
  private final boolean error;

  private RemoteDataCollectorResult(
      Future<AckEvent> futureAck,
      Object immediateResult,
      boolean error,
      String errorMessage) {
    this.futureAck = futureAck;
    this.immediateResult = immediateResult;
    this.error = error;
    this.errorMessage = errorMessage;
  }

  /**
   * Creates an immediate result from the given parameter.
   *
   * @param immediateResult the value to be used as the immediate result
   * @return an instance of this class with the given immediate result
   */
  public static RemoteDataCollectorResult immediate(Object immediateResult) {
    return new RemoteDataCollectorResult(null, immediateResult, false, null);
  }

  /**
   * Creates a future ack result from the given parameter.  It is expected that the caller will eventually wait for the
   * future ack result that is returned.
   *
   * @param futureResult the future ack
   * @return an instance of this class with the given future ack
   */
  public static RemoteDataCollectorResult futureAck(Future<AckEvent> futureResult) {
    return new RemoteDataCollectorResult(futureResult, null, false, null);
  }

  /**
   * Creates an error result from the given parameter.
   *
   * @param errorMessage the relevant error message
   * @return an instance of this class with the given error message
   */
  public static RemoteDataCollectorResult error(String errorMessage) {
    return new RemoteDataCollectorResult(null, null, true, errorMessage);
  }

  /**
   * Creates an empty result.
   *
   * @return an instance of this class with nothing set (i.e. empty)
   */
  public static RemoteDataCollectorResult empty() {
    return new RemoteDataCollectorResult(null, null, false, null);
  }

  /**
   * Gets the ack future for this result, if any
   *
   * @return the ack future, if set (null otherwise)
   */
  public Future<AckEvent> getFutureAck() {
    return futureAck;
  }

  /**
   * Gets the immediate result, if any
   *
   * @return the immediate result, if set (null otherwise)
   */
  public Object getImmediateResult() {
    return immediateResult;
  }

  /**
   * Gets the error message, if any
   *
   * @return the error message, if set (null otherwise)
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /**
   * Indicates whether this result represents an error.  Will only return true if constructed via
   * {@link #error(String)}.
   * @return true if this result is an error, false otherwise
   */
  public boolean isError() {
    return error;
  }
}
