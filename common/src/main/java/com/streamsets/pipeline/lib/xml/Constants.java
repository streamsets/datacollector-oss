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
package com.streamsets.pipeline.lib.xml;

public abstract class Constants {
  public static final char PATH_SEPARATOR_CHAR = '/';
  public static final String PATH_SEPARATOR=String.valueOf(PATH_SEPARATOR_CHAR);
  public static final String WILDCARD="*";
  public static final String ROOT_ELEMENT_PATH = Constants.WILDCARD+"[1]";
  public static final char NAMESPACE_PREFIX_SEPARATOR = ':';

  public static final String XML_RECORD_ELEMENT_DESCRIPTION = "XML element that acts as a record delimiter." +
      "  This may be an element name, with elements having that local name directly under the root as records." +
      "  Or, it can be a simplified XPath expression (see docs), with elements matching the XPath expression" +
      " as records.  Leaving it blank will treat the whole XML document as one record.";

  public static final String XML_PRESERVE_ROOT_ELEMENT = "Include the root element in the output records." +
      " The root element is the last element defined in the record element delimiter or is the root element" +
      " of the XML document when no delimiter is defined";

  public static final String INCLUDE_FIELD_XPATH_ATTRIBUTES_DESCRIPTION = "Include XPath expressions that indicate " +
      "the path to the input node that was parsed to create each field.  If enabled, each field will receive an " +
      "attribute named \"xpath\" whose value is the XPath expression.  Any namespace prefixes will be mapped to " +
      "the full URI via new xmlns:* attributes in the record header.";

  public static final String XPATH_NAMESPACE_CONTEXT_DESCRIPTION = "Namespace context to use if the delimiter" +
      " is an XPath expression.  This should map namespace prefixes to URIs.  Any namespace prefix that is used" +
      " in the record separator expression must be defined here.";

  public static final String OUTPUT_FIELD_ATTRIBUTES_DESCRIPTION = "Generate field attributes in output record for" +
      " XML namespace declarations, and XML attributes.  Without this option, they will continue to be output as" +
      " individual fields (separate from the element value).";

  public static final String ERROR_EMPTY_EXPRESSION = "expression cannot be empty";
  public static final String ERROR_INVALID_ELEMENT_NAME_PREFIX = "invalid element name: ";
  public static final String ERROR_XPATH_MUST_START_WITH_SEP = "XPath expression must start with ";
  public static final String ERROR_DESCENDENT_OR_SELF_NOT_SUPPORTED = "descendent-or-self (//) is not allowed";
  public static final String ERROR_INVALID_PREDICATE_PREFIX = "invalid predicate: ";
  public static final String ERROR_INVALID_ATTRIBUTE_PREFIX = "attribute name is not valid: ";

}
