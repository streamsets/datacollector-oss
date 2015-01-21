/**
 * Controller for Rules tab.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('RulesController', function ($scope) {
    angular.extend($scope, {
      showLoading: false,
      metricAlertRuleList: [],
      alertRuleList: [],
      counterRuleList: [],
      samplingRuleList: [],

      /**
       * Refresh Rules
       */
      refreshRules: function() {
        updateRules($scope.activeConfigInfo.name);
      },

      /**
       * Callback function for Create New Alert Rule button.
       *
       */
      createAlertRule: function() {

      },

      /**
       * Callback function for Create New Counter Rule button.
       */
      createCounterRule: function() {

      },

      /**
       * Callback function for Create New Sampling Rule button.
       */
      createSamplingRule: function() {

      },

      /**
       * Callback function for Create New Metric Alert Rule button.
       */
      createMetricAlertRule: function() {

      }
    });

    var updateRules = function(pipelineName) {
      $scope.showLoading = true;
      $scope.showLoading = false;
    };

    updateRules();
  });