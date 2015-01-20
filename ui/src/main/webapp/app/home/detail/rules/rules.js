/**
 * Controller for Rules tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('RulesController', function ($scope) {
    angular.extend($scope, {
      showLoading: false,
      rules: [],

      refreshRules: function() {
        updateRules($scope.activeConfigInfo.name);
      }
    });

    var updateRules = function(pipelineName) {
      $scope.showLoading = true;
      $scope.showLoading = false;
    };

    updateRules();
  });