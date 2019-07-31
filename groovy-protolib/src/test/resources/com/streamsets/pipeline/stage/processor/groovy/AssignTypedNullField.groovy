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
for (record in sdc.records) {
    record.value['null_short'] = sdc.NULL_SHORT
    record.value['null_char'] = sdc.NULL_CHAR
    record.value['null_int'] = sdc.NULL_INTEGER
    record.value['null_long'] = sdc.NULL_LONG
    record.value['null_float'] = sdc.NULL_FLOAT
    record.value['null_double'] = sdc.NULL_DOUBLE
    record.value['null_date'] = sdc.NULL_DATE
    record.value['null_datetime'] = sdc.NULL_DATETIME
    record.value['null_boolean'] = sdc.NULL_BOOLEAN
    record.value['null_decimal'] = sdc.NULL_DECIMAL
    record.value['null_byteArray'] = sdc.NULL_BYTE_ARRAY
    record.value['null_string'] = sdc.NULL_STRING
    record.value['null_list'] = sdc.NULL_LIST
    sdc.output.write(record)
}