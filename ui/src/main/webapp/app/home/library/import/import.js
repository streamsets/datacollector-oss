/**
 * Controller for Import Modal Dialog.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('ImportModalInstanceController', function ($scope, $modalInstance, api, pipelineInfo) {
    var errorMsg = 'Not a valid Pipeline Configuration file.';

    angular.extend($scope, {
      common: {
        errors: []
      },
      showLoading: true,
      uploadFile: {},

      /**
       * Import button callback function.
       */
      import: function () {
        var reader = new FileReader();

        reader.onload = function (loadEvent) {
          try {
            var jsonConfigObj = JSON.parse(loadEvent.target.result);
            if(jsonConfigObj.uuid) {

              jsonConfigObj.uuid = pipelineInfo.uuid;
              api.pipelineAgent.savePipelineConfig(pipelineInfo.name, jsonConfigObj).
                success(function() {
                  $modalInstance.close();
                }).error(function(data) {
                  $scope.common.errors = [data];
                });

            } else {
              $scope.$apply(function() {
                $scope.common.errors = [errorMsg];
              });
            }
          } catch(e) {
            $scope.$apply(function() {
              $scope.common.errors = [errorMsg];
            });
          }
        };
        reader.readAsText($scope.uploadFile);
      },

      /**
       * Cancel button callback.
       */
      cancel: function () {
        $modalInstance.dismiss('cancel');
      }
    });






  });