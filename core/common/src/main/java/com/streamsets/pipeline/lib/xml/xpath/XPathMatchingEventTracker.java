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
import com.streamsets.pipeline.api.impl.XMLChar;
import com.streamsets.pipeline.lib.xml.Constants;

import javax.xml.stream.events.XMLEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class XPathMatchingEventTracker {

  private final Map<String, String> namespaces = new HashMap<>();
  private final ArrayList<ElementMatcher> matchersByDepth = new ArrayList<>();

  private int depth = 0;
  private int matchesThroughDepth = 0;

  public XPathMatchingEventTracker(String xPath, Map<String, String> namespaces) {
    if (namespaces != null) {
      this.namespaces.putAll(namespaces);
    }
    if (xPath != null) {
      boolean ignoreNamespaces;
      if (xPath != null && XMLChar.isValidName(xPath)) {
        //this is simply a field name with no "/" separator; adjust it to support previous syntax
        xPath = Constants.ROOT_ELEMENT_PATH + Constants.PATH_SEPARATOR + xPath;
        ignoreNamespaces = true;
      } else {
        ignoreNamespaces = false;
      }

      for (String xPathPart : xPath.split(Constants.PATH_SEPARATOR)) {
        if (Strings.isNullOrEmpty(xPathPart)) {
          continue;
        }
        matchersByDepth.add(new ElementMatcherImpl(xPathPart, namespaces, ignoreNamespaces));
      }
    }
  }

  public MatchStatus isMatch(XMLEvent event) {
    if (event.isStartElement()) {
      depth++;

      if (depth > matchersByDepth.size()) {
        return MatchStatus.ELEMENT_NOT_MATCH;
      } else if (depth-1 > matchesThroughDepth) {
        return MatchStatus.ELEMENT_NOT_MATCH;
      } else {
        final ElementMatcher matcher = matchersByDepth.get(depth-1);
        if (matcher.checkStartElement(event.asStartElement())) {
          matchesThroughDepth = depth;
          if (matchesThroughDepth == matchersByDepth.size()) {
            // we have matched all levels through the current
            return MatchStatus.ELEMENT_MATCH;
          } else {
            // at least one more level needs to match
            return MatchStatus.UNDETERMINED;
          }
        } else {
          // did not match at this level
          return MatchStatus.ELEMENT_NOT_MATCH;
        }
      }
    } else if (event.isEndElement()) {
      depth--;
      if (matchesThroughDepth > depth) {
        matchesThroughDepth = depth;
      }
      return MatchStatus.UNDETERMINED;
    } else {
      return MatchStatus.UNDETERMINED;
    }
  }

}
