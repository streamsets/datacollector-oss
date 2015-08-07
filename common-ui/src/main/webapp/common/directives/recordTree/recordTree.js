/**
 * Record Tree Directive
 */

angular.module('recordTreeDirectives', ['RecursionHelper'])
  .directive('recordTree', function(RecursionHelper) {
    'use strict';

    var linkFunction = function (scope) {
      var mapListLimit = 50;

      angular.extend(scope, {
        updatedValue: false,
        updatedField: false,
        limit: mapListLimit,

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
        }
      });

      if(scope.diffType && scope.recordValue) {
        if(scope.recordValue.type !== 'MAP' && scope.recordValue.type !== 'LIST') {
          if(!scope.diffRecordValue || scope.recordValue.path !== scope.diffRecordValue.path) {
            scope.updatedField = true;
          } else if(scope.recordValue.value !== scope.diffRecordValue.value){
            scope.updatedValue = true;
          }
        } else {
          if(!scope.diffRecordValue || !angular.equals(scope.recordValue.value, scope.diffRecordValue.value) ) {
            scope.updatedField = true;
          }
        }
      }

      if(scope.recordValue && (scope.recordValue.type === 'MAP' || scope.recordValue.type === 'LIST')) {
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
        isRoot: '=',
        isError: '=',
        editable: '=',
        selectable: '=',
        selectedPath: '='
      },
      templateUrl: 'common/directives/recordTree/recordTree.tpl.html',
      compile: function (element) {
        // Use the compile function from the RecursionHelper,
        // And return the linking function(s) which it returns
        return RecursionHelper.compile(element, linkFunction);
      }
    };

  });