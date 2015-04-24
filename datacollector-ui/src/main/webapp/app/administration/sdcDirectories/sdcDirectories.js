/**
 * Controller for SDC Directories Modal Dialog.
 */

angular
  .module('dataCollectorApp')
  .controller('SDCDirectoriesModalInstanceController', function ($scope, $modalInstance, api) {
    angular.extend($scope, {
      sdcDirectories: [],
      close: function() {
        $modalInstance.dismiss('cancel');
      }
    });

    api.admin.getSDCDirectories().then(function(res) {
      $scope.sdcDirectories = res.data;
    }, function() {

    });
  });