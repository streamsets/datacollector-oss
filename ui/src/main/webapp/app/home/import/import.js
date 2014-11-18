/**
 * Controller for Import Modal Dialog.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('ImportModalInstanceController', function ($scope, $modalInstance) {
    var errorMsg = 'Not a valid Pipeline Configuration file.';

    $scope.uploadFile = {};

    $scope.import = function () {
      var reader = new FileReader();

      reader.onload = function (loadEvent) {
        try {
          var jsonConfigObj = JSON.parse(loadEvent.target.result);
          if(jsonConfigObj.uuid) {
            $modalInstance.close(jsonConfigObj);
          } else {
            $scope.$apply(function() {
              $scope.issues = [errorMsg];
            });
          }
        } catch(e) {
          $scope.$apply(function() {
            $scope.issues = [errorMsg];
          });
        }
      };
      reader.readAsText($scope.uploadFile);
    };

    $scope.cancel = function () {
      $modalInstance.dismiss('cancel');
    };

  });