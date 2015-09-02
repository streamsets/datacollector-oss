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
package com.streamsets.pipeline.api;

/**
 * Typifies error codes with built-in localization support. Pipeline exceptions use error codes.
 * <p></p>
 * Error code implementations are typically enums, i.e.:
 *
 * <pre>
 *
 * public enum BaseError implements ErrorCode {
 *   API_00("Stage '{}', there should be 1 output lane but there are '{}'");
 *
 *   private final String msg;
 *
 *   BaseError(String msg) {
 *     this.msg = msg;
 *   }
 *
 *   public String getCode() {
 *     return name();
 *   }
 *
 *   public String getMessage() {
 *     return msg;
 *   }
 *
 * }
 * </pre>
 *
 * Built in localization looks for a <code>Properties</code> based <code>ResourceBundle</code> matching the
 * <code>ErrorCode</code> implementation. The * <code>ErrorCode</code>'s <code>code</code> is used as the key within the
 * <code>ResourceBundle</code>. If the bundle is not available, or the key is not defined within the bundle, the
 * <code>ErrorCode</code>'s <code>message</code> will be used.
 * <p></p>
 * Typically, the message can be a template, using <code>{}</code> as positional placeholders for values.
 * {@link com.streamsets.pipeline.api.StageException} take an <code>ErrorCode</code> plus variable arguments on its
 * constructors and generates the exception message using the <code>ErrorCode</code> message as template then variable
 * argument as the values for it.
 */
public interface ErrorCode {

  /**
   * Returns the error code.
   *
   * @return the error code.
   */
  public String getCode();

  /**
   * Returns the built-in default message for the error code.
   *
   * @return the default message template
   */
  public String getMessage();

}
