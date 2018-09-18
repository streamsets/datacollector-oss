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
package com.streamsets.pipeline.lib.couchbase;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
  COUCHBASE_01("Error executing pipeline"),
  COUCHBASE_02("Error connecting to Couchbase. Check pipeline logs."),
  COUCHBASE_03("Error authenticating to Couchbase."),
  COUCHBASE_04("Bucket does not exist: {}"),
  COUCHBASE_05("SDK Environment property name is invalid: {}"),
  COUCHBASE_06("SDK Environment property cannot be changed: {}"),
  COUCHBASE_07("Document key is required"),
  COUCHBASE_08("Unparsable CDC operation"),
  COUCHBASE_09("Unsupported CDC operation"),
  COUCHBASE_10("Unable to perform full document write"),
  COUCHBASE_11("Unable to perform sub-document write"),
  COUCHBASE_12("Unable to serialize sub-document fragment"),
  COUCHBASE_13("Unparsable sub-document operation:"),
  COUCHBASE_14("Unsupported sub-document operation"),
  COUCHBASE_15("Unparsable CAS header value (couchbase.cas)"),
  COUCHBASE_16("Unparsable document key"),
  COUCHBASE_17("Unparsable TTL value"),
  COUCHBASE_18("Unparsable sub-document path"),
  COUCHBASE_19("Unable to serialize record."),
  COUCHBASE_20("Unable to perform sub-document lookup"),
  COUCHBASE_21("Unable to perform full document lookup"),
  COUCHBASE_22("Unparsable N1QL query"),
  COUCHBASE_23("Error executing N1QL query"),
  COUCHBASE_24("No results for N1QL query"),
  COUCHBASE_25("Sub-document path not found"),
  COUCHBASE_26("Document does not exist"),
  COUCHBASE_27("Configured property not returned for N1QL query"),
  COUCHBASE_28("Data Format is required"),
  COUCHBASE_29("Node list is required"),
  COUCHBASE_30("KV Timeout must be greater than 0"),
  COUCHBASE_31("Connect Timeout must be greater than 0"),
  COUCHBASE_32("Disconnect Timeout must be greater than 0"),
  COUCHBASE_33("Authentication Mode is required"),
  COUCHBASE_34("User Name is required in user authentication mode"),
  COUCHBASE_35("Password is required in user authentication mode"),
  COUCHBASE_36("Document Time-To-Live cannot be negative"),
  COUCHBASE_37("Sub-document lookup requires at least one operation"),
  ;

  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return msg;
  }
}
