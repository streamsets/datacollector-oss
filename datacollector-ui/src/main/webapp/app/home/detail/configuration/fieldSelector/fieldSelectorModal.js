/**
 * Controller for Field Selector Modal dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('FieldSelectorModalInstanceController', function ($scope, $timeout, $modalInstance, previewService,
            currentSelectedPaths, activeConfigInfo, detailPaneConfig) {
    angular.extend($scope, {
      common: {
        errors: []
      },
      showLoading: true,
      noPreviewRecord: false,
      recordObject: {},
      selectedPath: _.reduce(currentSelectedPaths, function(obj, path){
        obj[path] = true;
        return obj;
      }, {}),

      save: function() {
        var selectedFieldPaths = [];
        angular.forEach($scope.selectedPath, function(value, key) {
          if(value === true) {
            selectedFieldPaths.push(key);
          }
        });

        $modalInstance.close(selectedFieldPaths);
      },

      close: function() {
        $modalInstance.dismiss('cancel');
      }
    });

    $timeout(function() {
      previewService.getInputRecordsFromPreview(activeConfigInfo.name, detailPaneConfig, 1).
        then(
        function (inputRecords) {
          $scope.showLoading = false;
          if(_.isArray(inputRecords) && inputRecords.length) {
            $scope.recordObject = inputRecords[0];
          } else {
            $scope.noPreviewRecord = true;
          }
        },
        function(res) {
          $scope.showLoading = false;
          $scope.common.errors = [res.data];
        }
      );
    }, 300);
  });