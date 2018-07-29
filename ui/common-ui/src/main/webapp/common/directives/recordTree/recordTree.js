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
/**
 * Record Tree Directive
 */

angular.module('recordTreeDirectives', ['RecursionHelper'])
  .directive('recordTree', function(RecursionHelper, $modal) {
    'use strict';

    var linkFunction = function (scope) {
      var mapListLimit = 50;

      angular.extend(scope, {
        updatedValue: false,
        updatedField: false,
        limit: mapListLimit,
        listMapKey: undefined,
        mapKeys: (scope.recordValue && scope.recordValue.value) ? Object.keys(scope.recordValue.value) : [],

        onClick: function($event) {
          $event.preventDefault();
          if(scope.isRoot) {
            scope.isOpen = !scope.record.expand;
            scope.record.expand = scope.isOpen;

            if(scope.isOpen && scope.diffRecord) {
              var diffRecords = scope.diffRecord;
              if(!_.isArray(diffRecords)) {
                diffRecords = [diffRecords];
              }

              angular.forEach(diffRecords, function(diffRecord) {
                diffRecord.expand = true;
              });
            }

          } else {
            scope.isOpen = !scope.isOpen;
          }
        },

        /**
         * Set dirty flag to true when record is updated in Preview Mode.
         *
         * @param recordUpdated
         * @param recordValue
         */
        recordValueUpdated: function(recordUpdated, recordValue) {
          scope.$emit('recordUpdated', recordUpdated, recordValue);
        },


        /**
         * On Record Value updated.
         *
         * @param recordUpdated
         * @param recordValue
         * @param dateRecordValue
         */
        recordDateValueUpdated: function(recordUpdated, recordValue, dateRecordValue) {
          recordValue.value = dateRecordValue.getTime();
          scope.$emit('recordUpdated', recordUpdated, recordValue);
        },

        /**
         * Returns Date
         * @param milliSeconds
         * @returns {Date}
         */
        getDate: function(milliSeconds) {
          return new Date(milliSeconds);
        },

        /**
         * Callback function when Show more link clicked.
         *
         * @param $event
         */
        onShowMoreClick: function($event) {
          $event.preventDefault();
          scope.limit += mapListLimit;
        },

        /**
         * Callback function when Show all link clicked.
         *
         * @param $event
         */
        onShowAllClick: function($event) {
          $event.preventDefault();
          scope.limit = scope.valueLength;
        },

        /**
         * Returns key for listMap value
         * @param recordValue
         */
        getListMapKey: function(recordValue) {
          var path = recordValue.sqpath,
            pathSplit = path ? path.split('/') : undefined,
            lastFieldName;

          if(pathSplit && pathSplit.length > 0 ) {
            lastFieldName = pathSplit[pathSplit.length - 1];

            // handle special case field name containing slash eg. /'foo/bar'
            if(lastFieldName.indexOf("'") !== -1 &&
              !(lastFieldName.charAt(0) === '\'' && lastFieldName.charAt(lastFieldName.length - 1) === '\'')) {

              // If path contains slash inside name, split it by "/'"
              pathSplit = path.split("/'");
              if(pathSplit.length > 0) {
                lastFieldName = "'" + pathSplit[pathSplit.length - 1];
              }
            }
          }

          //return scope.singleQuoteUnescape(lastFieldName);
          return lastFieldName;
        },

        /**
         * Unescape single quote escaped field path
         * @param path
         * @returns {*}
         */
        singleQuoteUnescape: function(path) {
          if(path != null && /[\[\]\/"']/.test(path) && path.length > 2) {
            path = path.replace("\\\"", "\"").replace("\\\\\'", "'");
            return path.substring(1, path.length - 1);
          }
          return path;
        },

        /**
         * Display stack trace in modal dialog.
         *
         * @param header
         */
        showStackTrace: function (header) {
          $modal.open({
            templateUrl: 'errorModalContent.html',
            controller: 'ErrorModalInstanceController',
            size: 'lg',
            backdrop: true,
            resolve: {
              errorObj: function () {
                return {
                  RemoteException: {
                    localizedMessage: header.errorMessage,
                    stackTrace: header.errorStackTrace
                  }
                };
              }
            }
          });
        }
      });

      if(scope.diffType && scope.recordValue) {
        if(scope.recordValue.type !== 'MAP' && scope.recordValue.type !== 'LIST' && scope.recordValue.type !== 'LIST_MAP') {
          if(!scope.diffRecordValue || scope.recordValue.path !== scope.diffRecordValue.path) {
            scope.updatedField = true;
          } else if(scope.recordValue.value !== scope.diffRecordValue.value ||
            scope.recordValue.type !== scope.diffRecordValue.type){
            scope.updatedValue = true;
          }
        } else {
          if(!scope.diffRecordValue || !angular.equals(scope.recordValue.value, scope.diffRecordValue.value) ) {
            scope.updatedField = true;
          }
        }
      }

      if(scope.recordValue && (scope.recordValue.type === 'MAP' || scope.recordValue.type === 'LIST' ||
        scope.recordValue.type === 'LIST_MAP')) {
        scope.valueLength = _.size(scope.recordValue.value);
      }

    };

    return {
      restrict: 'E',
      replace: true,
      scope: {
        record: '=',
        recordValue: '=',
        diffType: '=',
        diffRecord: '=',
        diffRecordValue: '=',
        fieldName: '=',
        fieldIndex: '=',
        isRoot: '=',
        isError: '=',
        editable: '=',
        selectable: '=',
        selectedPath: '=',
        showHeader: '=',
        showFieldType: '='
      },
      templateUrl: 'common/directives/recordTree/recordTree.tpl.html',
      compile: function (element) {
        // Use the compile function from the RecursionHelper,
        // And return the linking function(s) which it returns
        return RecursionHelper.compile(element, linkFunction);
      }
    };

  });
