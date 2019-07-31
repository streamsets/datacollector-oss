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
  if(sdc.getFieldNull(record, "/null_int") == sdc.NULL_INTEGER)
    record.value['null_int'] = 123;
  if(sdc.getFieldNull(record, "/null_string") == sdc.NULL_STRING)
    record.value['null_string'] = "test";
  if(sdc.getFieldNull(record, '/null_boolean') == sdc.NULL_BOOLEAN)
    record.value['null_boolean'] = true;
  if(sdc.getFieldNull(record, '/null_list') == sdc.NULL_LIST)
    record.value['null_list'] = ['elem1', 'elem2'];
  if(sdc.getFieldNull(record, '/null_map') == sdc.NULL_MAP)
    record.value['null_map'] = [x: 'X', y: 'Y'];
  if(sdc.getFieldNull(record, '/null_datetime') == sdc.NULL_DATETIME)  // this should be false
    record.value['null_datetime'] = sdc.NULL_DATETIME
  sdc.output.write(record);
}