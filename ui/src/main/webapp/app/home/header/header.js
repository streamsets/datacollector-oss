/**
 * Controller for Header Pane.
 */

angular
  .module('dataCollectorApp.home')

  .controller('HeaderController', function ($scope, $rootScope, $timeout, _, api, $translate,
                                           pipelineService, pipelineConstant, $modal) {


    var pipelineValidationInProgress = 'Validating Pipeline...',
      pipelineValidationSuccess = 'Validation Successful.';

    $translate('global.messages.validate.pipelineValidationInProgress').then(function(translation) {
      pipelineValidationInProgress = translation;
    });

    $translate('global.messages.validate.pipelineValidationSuccess').then(function(translation) {
      pipelineValidationSuccess = translation;
    });

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
        $scope.addStageInstance({
          stage: selectedStage
        });
      },

      /**
       * Callback function when selecting Processor/Target from alert div.
       */
      onConnectStageChange: function() {
        var connectStage = $scope.connectStage.selected;
        $scope.addStageInstance({
          stage: connectStage,
          firstOpenLane: $scope.firstOpenLane
        });
        $scope.connectStage = {};
        $scope.firstOpenLane.stageInstance = undefined;
      },

      /**
       * Validate Pipeline
       */
      validatePipeline: function() {
        $scope.$storage.maximizeDetailPane = false;
        $scope.$storage.minimizeDetailPane = false;
        $rootScope.common.infoList.push({
          message:pipelineValidationInProgress
        });
        api.pipelineAgent.validatePipeline($scope.activeConfigInfo.name).
          then(
          function (res) {
            $rootScope.common.infoList = [];
            $rootScope.common.successList.push({
              message: pipelineValidationSuccess
            });
          },
          function (res) {
            $rootScope.common.infoList = [];
            $rootScope.common.errors = [res.data];
          }
        );
      },

      /**
       * On Start Pipeline button click.
       *
       */
      startPipeline: function() {
        if($rootScope.common.pipelineStatus.state !== 'RUNNING') {
          var startResponse;
          $scope.$storage.maximizeDetailPane = false;
          $scope.$storage.minimizeDetailPane = false;
          $scope.$storage.readNotifications = [];
          api.pipelineAgent.startPipeline($scope.activeConfigInfo.name, 0).
            then(
            function (res) {
              $scope.moveGraphToCenter();
              startResponse = res.data;
              return api.pipelineAgent.getPipelineMetrics();
            },
            function (res) {
              $rootScope.common.errors = [res.data];
            }
          ).
            then(
            function (res) {
              if(res) {
                $rootScope.common.pipelineMetrics = res.data;
                $rootScope.common.pipelineStatus = startResponse;

                $timeout(function() {
                  $scope.refreshGraph();
                });
              }
            },
            function (res) {
              $rootScope.common.errors = [res.data];
            }
          );
        } else {
          $translate('home.graphPane.startErrorMessage', {
            name: $rootScope.common.pipelineStatus.name
          }).then(function(translation) {
            $rootScope.common.errors = [translation];
          });
        }
      },

      /**
       * On Stop Pipeline button click.
       *
       */
      stopPipeline: function() {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/graph/stop/stopConfirmation.tpl.html',
          controller: 'StopConfirmationModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return $scope.activeConfigInfo;
            }
          }
        });

        modalInstance.result.then(function(status) {
          $scope.moveGraphToCenter();
          $rootScope.common.pipelineStatus = status;
          $scope.refreshGraph();
        }, function () {

        });
      },


      /**
       * Reset Offset of pipeline
       *
       */
      resetOffset: function() {
        var modalInstance = $modal.open({
          templateUrl: 'app/home/resetOffset/resetOffset.tpl.html',
          controller: 'ResetOffsetModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return $scope.activeConfigInfo;
            }
          }
        });
      },

      /**
       * Callback function when Notification is clicked.
       *
       * @param alert
       */
      onNotificationClick: function(alert) {
        var edges = $scope.edges,
          edge;
        $scope.$storage.maximizeDetailPane = false;
        $scope.$storage.minimizeDetailPane = false;

        if(alert.type === 'METRIC_ALERT') {
          //Select Pipeline Config
          $scope.$broadcast('selectNode');
          $scope.changeStageSelection({
            selectedObject: undefined,
            type: pipelineConstant.PIPELINE
          });
        } else {
          //Select edge
          edge = _.find(edges, function(ed) {
            return ed.outputLane === alert.rule.lane;
          });

          $scope.changeStageSelection({
            selectedObject: edge,
            type: pipelineConstant.LINK
          });
        }
      },

      /**
       * Delete Pipeline Configuration
       */
      deletePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.deletePipelineConfigCommand(pipelineInfo, $event);
      },

      /**
       * Duplicate Pipeline Configuration
       */
      duplicatePipelineConfig: function(pipelineInfo, $event) {
        pipelineService.duplicatePipelineConfigCommand(pipelineInfo, $event);
      },

      /**
       * Import link command handler
       */
      importPipelineConfig: function(pipelineInfo, $event) {
        pipelineService.importPipelineConfigCommand(pipelineInfo, $event);
      },

      /**
       * Export link command handler
       */
      exportPipelineConfig: function(pipelineInfo, $event) {
        api.pipelineAgent.exportPipelineConfig(pipelineInfo.name);
      }
    });

  });