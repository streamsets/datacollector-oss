/**
 * Controller for Graph Pane.
 */

angular
  .module('pipelineAgentApp.home')

  .controller('GraphController', function ($scope) {

    angular.extend($scope, {
      iconOnly: true,
      selectedSource: {},
      connectStage: {},

      /**
       * Callback function when Selecting Source from alert div.
       *
       */
      onSelectSourceChange: function() {
        var selectedStage = $scope.selectedSource.selected;
        $scope.pipelineConfig.issues = [];
        $scope.selectedSource = {};
        $scope.addStageInstance(selectedStage);
      },

      /**
       * Callback function when selecting Processor/Target from alert div.
       */
      onConnectStageChange: function() {
        var connectStage = $scope.connectStage.selected;
        $scope.addStageInstance(connectStage, $scope.firstOpenLane);
        $scope.connectStage = {};
        $scope.firstOpenLane.stageInstance = undefined;
      },

      stageDrop: function(e, stage) {
        if(e && stage) {
          $scope.addStageInstance(stage, undefined, e.x, e.y);
        }
      }
    });

  });