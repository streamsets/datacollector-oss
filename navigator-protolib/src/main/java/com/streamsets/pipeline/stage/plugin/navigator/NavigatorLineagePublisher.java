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
package com.streamsets.pipeline.stage.plugin.navigator;

import com.cloudera.nav.sdk.client.NavigatorPlugin;
import com.cloudera.nav.sdk.client.writer.ResultSet;
import com.cloudera.nav.sdk.model.entities.Entity;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineagePublisher;
import com.streamsets.pipeline.api.lineage.LineagePublisherDef;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@LineagePublisherDef(
  label = "Navigator",
  description = "Publishes lineage data to Cloudera Navigator."
)
public class NavigatorLineagePublisher implements LineagePublisher {
  private static final Logger LOG = LoggerFactory.getLogger(NavigatorLineagePublisher.class);
  private static final String REGEX = "[^A-Za-z0-9\\-_.]";
  private static final String REPLACEMENT = "_";
  private static final long MAX_EVENTS = 1000;

  // configuration information which is required to publish to Navigator
  private static final String[] PROPERTY_NAMES = {
      "application_url",
      "navigator_url",
      "metadata_parent_uri",
      "namespace",
      "username",
      "password",
      "autocommit"
  };

  private NavigatorPlugin plugin;
  private Map<String, Object> navigatorProperties = new HashMap<>();
  private Map<String, List<LineageEvent>> lineageEventCache = new HashMap<>();

  private List<LineageEvent> sources = new ArrayList<>();
  private List<LineageEvent> targets = new ArrayList<>();
  private long nodeCount;

  @Override
  public List<ConfigIssue> init(Context context) {
    LOG.info("Navigator Lineage Plugin initializing");

    final List<ConfigIssue> issues = new ArrayList<>();

    // add ths property - it's not exposed in the sdc.properties file.
    navigatorProperties.put("navigator_api_version", 9);
    for (String prop : PROPERTY_NAMES) {
      navigatorProperties.put(prop, context.getConfig(prop));
    }

    try {
      plugin = NavigatorPlugin.fromConfigMap(navigatorProperties);
    } catch (Exception ex) {
      issues.add(context.createConfigIssue(Errors.NAVIGATOR_02, navigatorProperties.get("navigator_url")));
    }

    return issues;
  }

  @Override
  public boolean publishEvents(List<LineageEvent> events) {
    LOG.debug("Navigator Lineage Plugin processing.  {} event(s)  cache {}/{}", events.size(), nodeCount, MAX_EVENTS);

    Set<String> keys = new HashSet<>();
    for (LineageEvent event : events) {
      LOG.info("eventType {} ", event.getEventType(), event.getPipelineTitle(), event.getPipelineStartTime());
      // ensure the property names do not have invalid characters.
      event.setProperties(checkPermittedCharacters(event.getProperties()));

      String key = NavigatorHelper.makePipelineIdentity(event.getPipelineId(), event.getPipelineStartTime());

      if (event.getEventType() == LineageEventType.START) {
        // should never happen, but let's check...
        if (lineageEventCache.get(key) != null) {
          LOG.error(Errors.NAVIGATOR_04.getMessage(), key);
          lineageEventCache.remove(key);
        }
        lineageEventCache.put(key, new ArrayList<>());

      } else {
        if (lineageEventCache.get(key) == null) {
          LOG.error(
              Errors.NAVIGATOR_05.getMessage(),
              event.getEventType(),
              key,
              event.toString()
          );
          continue;     // nothing else we can do...
        }
      }

      lineageEventCache.get(key).add(event);
      nodeCount++;
      if(nodeCount > MAX_EVENTS) {
        LOG.error("nodeCount ({}) > MAX_EVENTS ({})", nodeCount, MAX_EVENTS);
        //terminate? or kill the pipeline with the most nodes?
      }

      keys.add(key);   // add to Map keys we'll process in next phase.
    }

    boolean rcode = false;
    for (String k : keys) {
      if (!lineageEventCache.get(k).isEmpty()) {
        createNavigator(lineageEventCache.get(k));

        if (hasStopEvent(lineageEventCache.get(k))) {
          nodeCount -= lineageEventCache.get(k).size();
          lineageEventCache.remove(k);
        }
      } else {
        LOG.error(Errors.NAVIGATOR_06.getMessage(), k);
      }
    }

    return rcode;
  }

  private boolean hasStopEvent(List<LineageEvent> events) {
    for (LineageEvent event : events) {
      if (event.getEventType() == LineageEventType.STOP) {
        return true;
      }
    }
    return false;
  }

  private void createNavigator(List<LineageEvent> events) {

    LineageEvent stop = null;
    LineageEvent start = null;

    sources.clear();
    targets.clear();

    // sort them out by type.
    // we need to know whether there are any sources
    // or targets for the next part...
    for (LineageEvent e : events) {
      if (e.getEventType() == LineageEventType.ENTITY_READ) {
        sources.add(e);

      } else if (e.getEventType().isOneOf(LineageEventType.ENTITY_WRITTEN, LineageEventType.ENTITY_CREATED)) {
        targets.add(e);

      } else if (e.getEventType() == LineageEventType.STOP) {
        if (stop != null) {
          LOG.error(Errors.NAVIGATOR_07.getMessage());
          printEvents(events);
        }
        stop = e;

      } else if (e.getEventType() == LineageEventType.START) {
        if (start != null) {
          LOG.error(Errors.NAVIGATOR_08.getMessage());
          printEvents(events);
        }
        start = e;

      }
    }

    // need all combinations of:
    // source and target, source only, target only, neither source or target,
    // since Navigator SDK crashes when:
    // we provide uninitialized ArrayList for source or target,
    // or an initialized, but empty ArrayList().

    NavigatorPipelineModel model = null;

    // Navigator workaround.  need to have a "parentId" field
    // in each object which inherits from Entity.  this may be pulled out
    // later (or redefined) when the Navigator server-side bugs are fixed.
    // this variable is actually the MD5 hash of the "parent" of
    // each node to which it's applied.
    String toplevelId = NavigatorHelper.makePipelineIdentity(start.getPipelineId(), start.getPipelineStartTime());

    // both empty?
    if (sources.isEmpty() && targets.isEmpty()) {
      lastPublishedTime(start.getProperties());
      model = new NavigatorPipelineModel(plugin.getNamespace(), start, toplevelId);

      // both have entries?
    } else if (!sources.isEmpty() && !targets.isEmpty()) {
      lastPublishedTime(start.getProperties());
      start.setProperties(start.getProperties());

      model = new NavigatorPipelineModelSourceAndTarget(plugin.getNamespace(), start, toplevelId);
      for (LineageEvent s : sources) {
        ((NavigatorPipelineModelSourceAndTarget) model).addInput(new NavigatorDataset(plugin.getNamespace(), s, toplevelId));
      }

      for (LineageEvent t : targets) {
        ((NavigatorPipelineModelSourceAndTarget) model).addOutput(new NavigatorDataset(plugin.getNamespace(), t,toplevelId));
      }

      // target(s) only.
    } else if (!targets.isEmpty()) {
      lastPublishedTime(start.getProperties());
      model = new NavigatorPipelineModelTarget(plugin.getNamespace(), start, toplevelId);

      for (LineageEvent t : targets) {
        ((NavigatorPipelineModelTarget) model).addOutput(new NavigatorDataset(plugin.getNamespace(), t, toplevelId));
      }

      // oh well, sources(s) only.
    } else {
      lastPublishedTime(start.getProperties());
      model = new NavigatorPipelineModelSource(plugin.getNamespace(), start, toplevelId);

      for (LineageEvent s : sources) {
        ((NavigatorPipelineModelSource) model).addInput(new NavigatorDataset(plugin.getNamespace(), s, toplevelId));
      }
    }

    if (stop != null) {
      NavigatorPipelineInstance ins = new NavigatorPipelineInstance(plugin.getNamespace(), stop, model, toplevelId);
      ins.setTemplate(model);
      sendIt(ins);

    } else {
      sendIt(model);

    }

  }

  private void printEvents(List<LineageEvent> events) {
    for (LineageEvent e : events) {
      LOG.info(e.toString());
    }

  }

  /**
   * Enforce Navigator's restriction on key length and characters which are permitted in
   * a property name.
   *
   * @param map of properties.
   * @return
   */
  private Map<String, String> checkPermittedCharacters(Map<String, String> map) {
    Map<String, String> newMap = new HashMap<>();
    for(Map.Entry<String, String> entry : map.entrySet()) {
      String key = entry.getKey();
      String val = entry.getValue();

      NavigatorHelper.parameterChopper(val);
      NavigatorHelper.nameChopper(key);

      newMap.put(key.replaceAll(REGEX, REPLACEMENT), val);
    }
    return newMap;
  }

  /**
   * debugging routine.
   *
   * @param map of input properties
   * @return same as above with additional last published property.
   */
  private void lastPublishedTime(Map<String, String> map) {
    map.put("last_published", new Instant(System.currentTimeMillis()).toString());
  }

  /**
   * Wrapper which supports Navigator API for one-at-a-time sending of Entity objects.
   *
   * @param entity
   * @return true if no error, false if error.
   */

  private boolean sendIt(Entity entity) {
    ResultSet results = plugin.write(entity);
    return errorCheck(results);
  }

  /**
   * Checks error status from Navigator API write calls.
   *
   * @param results
   * @return true if no error, false if error.
   */
  private boolean errorCheck(ResultSet results) {
    if (results.hasErrors()) {
      LOG.error(Errors.NAVIGATOR_01.getMessage(), results.toString());
      return false;
    } else {
      return true;
    }
  }

  @Override
  public void destroy() {
    LOG.info("Navigator Lineage Plugin destroying");
  }
}
