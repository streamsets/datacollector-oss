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
package com.streamsets.pipeline.lib.jdbc.parser.sql;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;

import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Date;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.streamsets.pipeline.lib.jdbc.JdbcErrors.JDBC_37;

public class DateTimeColumnHandler {

  public static final String DEFAULT_LOCAL_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]";
  public static final String DEFAULT_ZONED_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss[.SSSSSSSSS] VV";;
  public static final String DEFAULT_DATETIME_FORMAT = "dd-MM-yyyy HH:mm:ss";
  private final Pattern toDatePattern = Pattern.compile("TO_DATE\\('(.*)',.*");
  // If a date is set into a timestamp column (or a date field is widened to a timestamp,
  // a timestamp ending with "." is returned (like 2016-04-15 00:00:00.), so we should also ignore the trailing ".".
  private final Pattern toTimestampPattern = Pattern.compile("TO_TIMESTAMP\\('(.*[^\\.]).*'");
  // TIMESTAMP WITH LOCAL TIME ZONE contains a "." at the end just like timestamp, so ignore that.
  private final Pattern toTimeStampTzPatternLocalTz = Pattern.compile("TO_TIMESTAMP_TZ\\('(.*[^\\.]).*'");
  private final Pattern toTimeStampTzPatternTz = Pattern.compile("TO_TIMESTAMP_TZ\\('(.*).*'");
  public final DateTimeFormatter dateFormatter;
  private final DateTimeFormatter localDtFormatter;
  private final DateTimeFormatter zonedDtFormatter;

  public static final String DT_SESSION_FORMAT = "'DD-MM-YYYY HH24:MI:SS'";
  // Oracle cannot return offset and zone id together -
  // so we use offset since that uniquely represents an instant in time. TZH:TZM TZR as format will throw an exception.
  // Oracle can represent 3 letter times (PST/PDT) and zone id (America/Los_Angeles) and expects that can be used to
  // figure out if the time was in an overlapping period (ex: PDT -> PST), but this is not useful as SHORT zone id
  // is no longer used by Java for detecting overlap, and will always fall to the "standard" time, not to summer time.
  // So we only use offsets, no zone id
  public static final String ZONED_DATETIME_SESSION_FORMAT = "'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'";
  public static final String TIMESTAMP_SESSION_FORMAT = "'YYYY-MM-DD HH24:MI:SS.FF'";

  private final ZoneId zoneId;
  private final boolean timestampAsString;

  public DateTimeColumnHandler(ZoneId zoneId, boolean timestampAsString) {
    this(zoneId, timestampAsString, DEFAULT_DATETIME_FORMAT, DEFAULT_LOCAL_DATETIME_FORMAT, DEFAULT_ZONED_DATETIME_FORMAT);
  }

  public DateTimeColumnHandler(
      ZoneId zoneId,
      boolean timestampAsString,
      String dateFormat,
      String localDateTimeFormat,
      String zonedDatetimeFormat
  ) {
    this.zoneId = zoneId;
    this.timestampAsString = timestampAsString;
    dateFormatter =
        new DateTimeFormatterBuilder()
            .parseLenient()
            .appendPattern(dateFormat)
            .toFormatter();
    localDtFormatter =
        new DateTimeFormatterBuilder()
            .parseLenient()
            .appendPattern(localDateTimeFormat)
            .toFormatter();

    zonedDtFormatter =
        new DateTimeFormatterBuilder()
            .parseLenient()
            .appendPattern(zonedDatetimeFormat)
            .toFormatter();
  }

  /**
   * This method returns a {@linkplain Field} that represents a DATE, TIME or DATETIME. The value of the returned
   * {@linkplain Field} is parsed from the {@param columnValue}.
   *
   * @param column Name of the corresponding column in the database.
   * @param columnValue String containing the TO_TIMESTAMP or TO_DATE function invocation to be parsed.
   * @param columnType java.sql type of the column in the database (DATE, TIME or TIMESTAMP).
   *
   * @return A {@linkplain Field} of type DATE, TIME or DATETIME, depending on the {@code columnType} value.
   *
   */
  public Field getDateTimeStampField(String column, String columnValue, int columnType) throws StageException {
    Field.Type type;
    if (columnType == Types.DATE) {
      type = Field.Type.DATE;
    } else if (columnType == Types.TIME) {
      type = Field.Type.TIME;
    } else if (columnType == Types.TIMESTAMP) {
      type = Field.Type.DATETIME;
    } else {
      throw new StageException(JDBC_37, columnType, column);
    }
    if (columnValue == null) {
      return Field.create(type, null);
    } else {
      Optional<String> ts = matchDateTimeString(toTimestampPattern.matcher(columnValue));
      if (ts.isPresent()) {
        if (timestampAsString) {
          return Field.create(Field.Type.STRING, ts.get());
        }
        Timestamp timestamp = Timestamp.valueOf(ts.get());
        Field field = Field.create(type, timestamp);
        JdbcUtil.setNanosecondsinAttribute(timestamp.getNanos(), field);
        return field;
      }
      // We did not find TO_TIMESTAMP, so try TO_DATE
      Optional<String> dt = matchDateTimeString(toDatePattern.matcher(columnValue));
      return Field.create(type, dt.map(s -> Date.from(getDate(s).atZone(zoneId).toInstant())).orElse(null));
    }
  }

  public Field getTimestampWithTimezoneField(String columnValue) {
    if (columnValue == null) {
      return Field.createZonedDateTime(null);
    }
    Matcher m = toTimeStampTzPatternTz.matcher(columnValue);
    if (m.find()) {
      if (timestampAsString) {
        return Field.create(Field.Type.STRING, m.group(1));
      }
      // Zoned Timestamp can maintain fractional seconds precision
      return Field.createZonedDateTime(ZonedDateTime.parse(m.group(1), zonedDtFormatter));
    }
    return Field.createZonedDateTime(null);
  }

  public Field getTimestampWithLocalTimezone(String columnValue) {
    if (columnValue == null) {
      return Field.createZonedDateTime(null);
    }
    Matcher m = toTimeStampTzPatternLocalTz.matcher(columnValue);
    if (m.find()) {
      if (timestampAsString) {
        return Field.create(Field.Type.STRING, m.group(1));
      }
      // Zoned Timestamp can maintain fractional seconds precision
      return Field.createZonedDateTime(ZonedDateTime.of(LocalDateTime.parse(m.group(1), localDtFormatter), zoneId));
    }
    return Field.createZonedDateTime(null);
  }

  private static Optional<String> matchDateTimeString(Matcher m) {
    if (!m.find()) {
      return Optional.empty();
    }
    return Optional.of(m.group(1));
  }

  public LocalDateTime getDate(String s) {
    return LocalDateTime.parse(s, dateFormatter);
  }
}