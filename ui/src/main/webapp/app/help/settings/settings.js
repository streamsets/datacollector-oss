/**
 * Controller for Settings Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('SettingsModalInstanceController', function ($scope, $modalInstance) {
    angular.extend($scope, {
      done: function() {
        $modalInstance.dismiss('cancel');
      }
    });
  });