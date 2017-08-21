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
package com.streamsets.pipeline.lib.xml.xpath;

import com.google.common.base.Strings;
import com.streamsets.pipeline.lib.xml.Constants;

import javax.xml.namespace.QName;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class ElementMatcherImpl implements ElementMatcher {

  private boolean byIndex = false;
  private boolean byAttribute = false;
  private int index = 0;
  private String attributeName = null;
  private String attributeValue = null;

  private boolean wildcardElement = false;
  private final String elementName;
  private int numElementsSeen = 0;

  private final String namespacePrefix;
  private final Map<String, String> namespaces = new HashMap<>();
  private final boolean ignoreNamespaces;

  ElementMatcherImpl(String xPathPart, Map<String, String> namespaces, boolean ignoreNamespaces) {

    this.ignoreNamespaces = ignoreNamespaces;
    final int nsSeparatorIndex = xPathPart.indexOf(Constants.NAMESPACE_PREFIX_SEPARATOR);
    String xPathLocalPart = xPathPart;
    if (nsSeparatorIndex > 0) {
      namespacePrefix = xPathPart.substring(0, nsSeparatorIndex);
      xPathLocalPart = xPathPart.substring(nsSeparatorIndex+1);
    } else {
      namespacePrefix = null;
    }

    if (namespaces != null) {
      this.namespaces.putAll(namespaces);
    }

    final int qualifierStart = xPathLocalPart.lastIndexOf('[');
    if (qualifierStart > 0) {
      final int qualifierEnd = xPathLocalPart.lastIndexOf(']');
      if (xPathLocalPart.charAt(qualifierStart + 1) == '@') {
        byAttribute = true;
        final int equalsIndex = xPathLocalPart.lastIndexOf('=');
        attributeName = xPathLocalPart.substring(qualifierStart + 2, equalsIndex);

        // knock off one character on each end for quotes
        attributeValue = xPathLocalPart.substring(equalsIndex + 2, qualifierEnd - 1);
      } else {
        byIndex = true;
        index = Integer.parseInt(xPathLocalPart.substring(qualifierStart + 1, qualifierEnd));
      }
      elementName = xPathLocalPart.substring(0, qualifierStart);
    } else {
      elementName = xPathLocalPart;
    }
    if (Constants.WILDCARD.equals(elementName)) {
      wildcardElement = true;
    }
  }

  @Override
  public boolean checkStartElement(StartElement startElement) {
    if (wildcardElement || isQualifiedMatch(startElement.getName())) {
      numElementsSeen++;
      if (byIndex) {
        return numElementsSeen == index;
      } else if (byAttribute) {
        final Iterator<?> attrIter = startElement.getAttributes();
        while (attrIter.hasNext()) {
          Attribute attrib = (Attribute) attrIter.next();
          if (attrib.getName().getLocalPart().equals(attributeName) &&
              (Constants.WILDCARD.equals(attributeValue)) || attrib.getValue().equals(attributeValue)) {
            return true;
          }
        }
        return false;
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  private boolean isQualifiedMatch(QName elementQName) {
    boolean namespaceMatches;
    if (namespacePrefix == null) {
      // xpath has no prefix; the element should therefore also have no namespace if namespaces are not ignored
      namespaceMatches = ignoreNamespaces || Strings.isNullOrEmpty(elementQName.getNamespaceURI());
    } else {
      namespaceMatches = namespaces.containsKey(namespacePrefix) &&
          namespaces.get(namespacePrefix).equals(elementQName.getNamespaceURI());
    }
    return elementQName.getLocalPart().equals(this.elementName) && namespaceMatches;
  }
}
