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
package com.streamsets.datacollector.validation;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

// we are using the annotation for reference purposes only.
// the annotation processor does not work on this maven project
// we have a hardcoded 'datacollector-resource-bundles.json' file in resources
@GenerateResourceBundle
public enum ValidationError implements ErrorCode {
  VALIDATION_0000("Unsupported pipeline schema version '{}'"),

  VALIDATION_0001("The pipeline is empty"),
  VALIDATION_0002("The pipeline includes unconnected stages. Cannot reach the following stages: {}"),
  VALIDATION_0003("The first stage must be an origin"),
  VALIDATION_0004("This stage cannot be an origin"),
  VALIDATION_0005("Stage name already defined"),
  VALIDATION_0006("Stage definition does not exist, library '{}', name '{}', version '{}'"),
  VALIDATION_0007("Configuration value is required"),
  VALIDATION_0008("Invalid configuration: '{}'"),
  VALIDATION_0009("Configuration should be a '{}'"),
  VALIDATION_0010("Output streams '{}' are already defined by stage '{}'"),
  VALIDATION_0011("Stage has open output streams"),
    // Issues are augmented with an array of the open streams in the additionalInfo map property

  VALIDATION_0012("{} cannot have input streams '{}'"),
  VALIDATION_0013("{} cannot have output streams '{}'"),
  VALIDATION_0014("{} must have input streams"),
  VALIDATION_0015("Stage must have '{}' output stream(s) but has '{}'"),
  VALIDATION_0016("Invalid stage name '{}'. Names can include the following characters '{}'"),
  VALIDATION_0017("Invalid input stream names '{}'. Streams can include the following characters '{}'"),
  VALIDATION_0018("Invalid output stream names '{}'. Streams can include the following characters '{}'"),

  VALIDATION_0019("Stream condition at index '{}' is not a map"),
  VALIDATION_0020("Stream condition at index '{}' must have a '{}' entry"),
  VALIDATION_0021("Stream condition at index '{}' entry '{}' cannot be NULL"),
  VALIDATION_0022("Stream condition at index '{}' entry '{}' must be a string"),
  VALIDATION_0023("Stream condition at index '{}' entry '{}' cannot be empty"),

  VALIDATION_0024("Configuration of type Map expressed as a list of key/value pairs cannot have null elements in the " +
                  "list. The element at index '{}' is NULL."),
  VALIDATION_0025("Configuration of type Map expressed as a list of key/value pairs must have the 'key' and 'value'" +
                  "entries in the List's Map elements. The element at index '{}' is does not have those 2 entries"),
  VALIDATION_0026("Configuration of type Map expressed as List of key/value pairs must have Map entries in the List," +
                  "element at index '{}' has '{}'"),

  VALIDATION_0027("Data rule '{}' refers to a stream '{}' which is not found in the pipeline configuration"),
  VALIDATION_0028("Data rule '{}' refers to a stage '{}' which is not found in the pipeline configuration"),

  VALIDATION_0029("Configuration must be a string, instead of a '{}'"),
  VALIDATION_0030("The expression value '{}' must be within '${...}'"),

  VALIDATION_0031("The property '{}' should be a single character"),
  VALIDATION_0032("Stage must have at least one output stream"),
  VALIDATION_0033("Invalid Configuration, {}"),

  VALIDATION_0034("Value for configuration '{}' cannot be greater than '{}'"),
  VALIDATION_0035("Value for configuration '{}' cannot be less than '{}'"),
  VALIDATION_0036("{} cannot have event streams '{}'"),
  VALIDATION_0037("Stage can't be on the main pipeline canvas"),
  VALIDATION_0038("Connection definition does not exist, type '{}', version '{}'"),
  VALIDATION_0039("Stage '{}' and '{}' have more then one shared lane"),

  //Rule Validation Errors
  VALIDATION_0040("The data rule property '{}' must be defined"),
  VALIDATION_0041("The Sampling Percentage property must have a value between 0 and 100"),
  VALIDATION_0042("Email alert is enabled, but no email is specified"),
  VALIDATION_0043("The value defined for Threshold Value is not a number"),
  VALIDATION_0044("The Threshold Value property must have a value between 0 and 100"),
  VALIDATION_0045("The condition '{}' defined for the data rule is not valid: {}"),
  VALIDATION_0046("The condition must use the following format: '${value()<operator><number>}'"),
  VALIDATION_0047("The condition '{}' defined for the metric alert is not valid"),
  VALIDATION_0050("The property '{}' must be defined for the metric alert"),
  VALIDATION_0051("Unsupported rule definition schema version '{}'"),

  VALIDATION_0060("Define the error record handling for the pipeline"),
  VALIDATION_0061("Define the directory for error record files"),

  VALIDATION_0070("Pipeline does not define its execution mode"),
  VALIDATION_0071("Stage '{}' from '{}' library does not support '{}' execution mode, supported modes: {}"),
  VALIDATION_0072("Data Collector is in standalone mode, cannot run pipeline cluster mode"),
  VALIDATION_0073("Data Collector is in cluster mode, cannot run pipeline standalone mode"),
  VALIDATION_0074("Origin '{}' of type '{}' is not bisectable, and can't be used for Advanced Error Handling"),

  VALIDATION_0080("Precondition '{}' must begin with '${' and end with '}'"),
  VALIDATION_0081("Invalid precondition '{}': {}"),
  VALIDATION_0082("Cannot create runner with execution mode '{}', another runner with execution mode '{}'"
            + " is active"),
  VALIDATION_0090("Encountered exception while validating configuration : {}"),
  VALIDATION_0091("Found more than one Target stage that triggers offset commit"),
  VALIDATION_0092("Delivery Guarantee can only be {} if pipeline contains a destination that triggers offset commit"),
  VALIDATION_0093("The pipeline title is empty"),
  VALIDATION_0094("Stage expects {} input lanes, but only {} given"),
  VALIDATION_0095("Stage library {} is a legacy library and must be installed separately"),
  VALIDATION_0096("This pipeline was created in version {} and is not compatible with current version {}"),

  // Event related validations
  VALIDATION_0100("Invalid event stream name '{}'. Streams can include the following characters: '{}'"),
  VALIDATION_0101("Stage has more than one event lane"),
  VALIDATION_0102("Stage has configured event lane even though it doesn't produce events"),
  VALIDATION_0103("Stage '{}' has input from both data and event branches of the pipeline. This is not allowed."),
  VALIDATION_0104("Stage has open event streams"),
  VALIDATION_0105("Invalid pipeline lifecycle specification: {}"),
  VALIDATION_0106("Pipeline lifecycle events are not supported in mode: {}"),

  // Service related validations
  VALIDATION_0200("Invalid services declaration, expected definition for '{}', but got '{}'"),

  // cluster config validations
  VALIDATION_0300("Stage '{}' using the '{}' stage library cannot be used with the '{}' cluster manager type. Supported modes: {}"),
  VALIDATION_0301("Cannot specify keytab if Kerberos is disabled via the {} configuration property"),
  VALIDATION_0302("Specified path {} was relative{}. Keytab path must be absolute. "),
  VALIDATION_0303("No file was found at {}; please double check the keytab path{}"),
  VALIDATION_0304("{} was not a regular file; please double check the keytab path{}"),
  VALIDATION_0305("Impersonation is required by {} configuration property, so an explicit user may not be specified."),
  VALIDATION_0306("Invalid URL for Standalone Spark Cluster Type, The Master URL must use the following format: 'spark://HOST:PORT'"),
  VALIDATION_0307("No keytab was specified{}"),
  VALIDATION_0401(
      "The {} property was set to {}, therefore you are not allowed to specify a keytab. Please contact your system " +
          "administrator with any questions."
  ),
  ;

  private final String msg;

  ValidationError(String msg) {
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
