/**
 * Controller for Library Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('LibraryController', function ($scope, $modal) {
    $scope.addPipelineConfig = function() {
      var modalInstance = $modal.open({
        templateUrl: 'app/home/library/createModal.tpl.html',
        controller: 'CreateModalInstanceController',
        size: '',
        backdrop: true
      });

      modalInstance.result.then(function (configObject) {
        $scope.pipelines.push(configObject.info);
      }, function () {

      });
    };
  })

  .controller('CreateModalInstanceController', function ($scope, $modalInstance, api) {
    $scope.issues = [];
    $scope.newConfig = {
      name: '',
      description: ''
    };

    $scope.save = function () {

      api.pipelineAgent.createNewPipelineConfig($scope.newConfig.name, $scope.newConfig.description).
        success(function(configObject) {
          console.log('Save new configuration');
          console.log(configObject);
          $modalInstance.close(configObject);
        }).
        error(function(data, status, headers, config) {
          $scope.issues = [data];
        });



    };

    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };

  });