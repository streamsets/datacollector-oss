/**
 * Controller for Error Modal Dialog.
 */

angular
  .module('pipelineAgentApp.home')
  .controller('ErrorModalInstanceController', function ($scope, $modalInstance, errorObj) {

    angular.extend($scope, {
      errorObject: errorObj,

      close: function () {
        $modalInstance.dismiss('cancel');
      }
    });

  });