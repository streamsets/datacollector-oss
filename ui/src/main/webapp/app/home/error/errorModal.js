/**
 * Controller for Error Modal Dialog.
 */

angular
  .module('dataCollectorApp.home')
  .controller('ErrorModalInstanceController', function ($scope, $modalInstance, errorObj) {

    angular.extend($scope, {
      errorObject: errorObj,

      close: function () {
        $modalInstance.dismiss('cancel');
      }
    });

  });