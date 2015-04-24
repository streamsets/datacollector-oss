/**
 * Controller for Preview/Snapshot Common Table View.
 */

angular
  .module('dataCollectorApp.home')
  .controller('PreviewCommonTableViewController', function ($scope, pipelineService) {
    angular.extend($scope, {
      inputFieldPaths: [],
      outputFieldPaths: [],

      getFlattenRecord: function(record) {
        if(record) {
          var flattenRecord = {};
          pipelineService.getFlattenRecord(record.value, flattenRecord);
          return flattenRecord;
        }
      }
    });

    $scope.$watch('stagePreviewData', function(event, options) {
      updateFieldPaths();
    });

    var updateFieldPaths = function() {
      var output = $scope.stagePreviewData.output,
        input = $scope.stagePreviewData.input;

      $scope.inputFieldPaths = [];
      if(input && input.length) {
        pipelineService.getFieldPaths(input[0].value, $scope.inputFieldPaths, true);
      }


      $scope.outputFieldPaths = [];
      if(output && output.length) {
        pipelineService.getFieldPaths(output[0].value, $scope.outputFieldPaths, true);
      }
    };


    updateFieldPaths();

  });