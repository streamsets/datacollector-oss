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

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.EventReaderDelegate;
import java.util.Map;

public class XPathMatchingEventReader extends EventReaderDelegate {

  private final XPathMatchingEventTracker eventTracker;
  private MatchStatus lastElementMatchResult = MatchStatus.UNDETERMINED;
  private XMLEvent lastMatchingEvent;

  public XPathMatchingEventReader(XMLEventReader delegate, String xPath, Map<String, String> namespaces) {
    super(delegate);
    this.eventTracker = new XPathMatchingEventTracker(xPath, namespaces);
  }

  @Override
  public XMLEvent nextEvent() throws XMLStreamException {
    final XMLEvent event = super.nextEvent();
    MatchStatus result = eventTracker.isMatch(event);
    if (!result.equals(MatchStatus.UNDETERMINED)) {
      // it is a definitive element match result, one way or the other
      lastElementMatchResult = result;
      if (result.equals(MatchStatus.ELEMENT_MATCH)) {
        lastMatchingEvent = event;
      }
    }
    return event;
  }

  public MatchStatus getLastElementMatchResult() {
    return lastElementMatchResult;
  }

  public void clearLastMatch() {
    lastElementMatchResult = MatchStatus.UNDETERMINED;
    lastMatchingEvent = null;
  }

  public XMLEvent getLastMatchingEvent() {
    return lastMatchingEvent;
  }
}
