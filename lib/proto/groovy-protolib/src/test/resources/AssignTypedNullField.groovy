/**
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
for (record in records) {
    record.value['null_short'] = NULL_SHORT
    record.value['null_char'] = NULL_CHAR
    record.value['null_int'] = NULL_INTEGER
    record.value['null_long'] = NULL_LONG
    record.value['null_float'] = NULL_FLOAT
    record.value['null_double'] = NULL_DOUBLE
    record.value['null_date'] = NULL_DATE
    record.value['null_datetime'] = NULL_DATETIME
    record.value['null_boolean'] = NULL_BOOLEAN
    record.value['null_decimal'] = NULL_DECIMAL
    record.value['null_byteArray'] = NULL_BYTE_ARRAY
    record.value['null_string'] = NULL_STRING
    record.value['null_list'] = NULL_LIST
    output.write(record)
}