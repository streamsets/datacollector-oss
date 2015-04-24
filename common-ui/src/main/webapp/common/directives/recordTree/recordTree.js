/**
 * Record Tree Directive
 */

angular.module('recordTreeDirectives', ['RecursionHelper'])
  .directive('recordTree', function(RecursionHelper) {
    'use strict';

    var linkFunction = function (scope) {

      angular.extend(scope, {
        updatedValue: false,
        updatedField: false,

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


        recordDateValueUpdated: function(recordUpdated, recordValue, dateRecordValue) {
          recordValue.value = dateRecordValue.getTime();
          scope.$emit('recordUpdated', recordUpdated, recordValue);
        },

        getDate: function(milliSeconds) {
          return new Date(milliSeconds);
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
          if(!scope.diffRecordValue) {
            scope.updatedField = true;
          }
        }
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