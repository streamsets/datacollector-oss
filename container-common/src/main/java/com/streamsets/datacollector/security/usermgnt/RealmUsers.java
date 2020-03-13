/**
 * Copyright 2020 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.datacollector.security.usermgnt;

import com.streamsets.pipeline.api.impl.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RealmUsers {
  static final Pattern ESCAPED_PATTERN = Pattern.compile("\\\\([:= ])");
  private final List<Line> lines;

  private RealmUsers(List<Line> lines) {
    this.lines = lines;
  }

  public static RealmUsers parse(BufferedReader reader) throws IOException {
    Utils.checkNotNull(reader, "reader");
    List<Line> lines = new ArrayList<>();
    String str = reader.readLine();
    while (str != null) {
      // handle escaped characters from some property file serializers
      str = ESCAPED_PATTERN.matcher(str).replaceAll("$1");
      Line line;
      if (EmptyLine.PATTERN.matcher(str).matches()) {
        line = new EmptyLine();
      } else if (CommentLine.PATTERN.matcher(str).matches()) {
        line = new CommentLine(str);
      } else if (MD5UserLine.PATTERN.matcher(str).matches()) {
        line = new MD5UserLine(str);
      } else if (ClearUserLine.PATTERN.matcher(str).matches()) {
        line = new ClearUserLine(str);
      } else {
        throw new IOException("Invalid Realm file line: " + str);
      }
      lines.add(line);
      str = reader.readLine();
    }
    return new RealmUsers(lines);
  }

  @SuppressWarnings("unchecked")
  List<UserLine> list() {
    return (List<UserLine>) (List) lines.stream().filter(l -> l.getId() != null).collect(Collectors.toList());
  }

  public UserLine find(String user) {
    Utils.checkNotNull(user, "user");
    return (UserLine) lines.stream().filter(l -> user.equals(l.getId())).findFirst().orElse(null);
  }

  public boolean add(UserLine userLine) {
    Utils.checkNotNull(userLine, "userLine");
    boolean added = false;
    if (find(userLine.getUser()) == null) {
      lines.add(userLine);
      added = true;
    }
    return added;
  }

  public void delete(String user) {
    Utils.checkNotNull(user, "user");
    lines.removeIf(line -> user.equals(line.getId()));
  }

  public void write(Writer writer) throws IOException {
    for (Line line : lines) {
      writer.write(line.getValue() + "\n");
    }
    writer.flush();
  }

}
