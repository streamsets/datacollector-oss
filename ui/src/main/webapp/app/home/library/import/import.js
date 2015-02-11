/**
 * Controller for Import Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
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

              if(pipelineInfo) { //If pipeline config already exists
                jsonConfigObj.uuid = pipelineInfo.uuid;
                api.pipelineAgent.savePipelineConfig(pipelineInfo.name, jsonConfigObj).
                  success(function(res) {
                    $modalInstance.close();
                  }).error(function(data) {
                    $scope.common.errors = [data];
                  });
              } else { //If no pipeline exist

                api.pipelineAgent.createNewPipelineConfig(jsonConfigObj.info.name, jsonConfigObj.info.description)
                  .then(function(res) {
                    var newPipelineObject = res.data;
                    newPipelineObject.configuration = jsonConfigObj.configuration;
                    newPipelineObject.errorStage = jsonConfigObj.errorStage;
                    newPipelineObject.uiInfo = jsonConfigObj.uiInfo;
                    newPipelineObject.stages = jsonConfigObj.stages;
                    return api.pipelineAgent.savePipelineConfig(jsonConfigObj.info.name, newPipelineObject);
                  })
                  .then(function(res) {
                    $modalInstance.close(res.data);
                  },function(res) {
                    $scope.common.errors = [res.data];
                  });
              }

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