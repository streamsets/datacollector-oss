/**
 * Controller for Error Modal Dialog.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('ErrorModalInstanceController', function ($scope, $modalInstance, errorObj) {

    $scope.errorObject = errorObj;

    $scope.close = function () {
      $modalInstance.dismiss('cancel');
    };

  });