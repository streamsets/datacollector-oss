/**
 * Controller for Error Configuration.
 */
angular
  .module('dataCollectorApp.home')
  .controller('ErrorConfigurationController', function ($scope) {
    $scope.detailPaneConfig = $scope.errorStageConfig;
    $scope.$watch('errorStageConfig', function() {
      $scope.detailPaneConfig = $scope.errorStageConfig;
    });
  });