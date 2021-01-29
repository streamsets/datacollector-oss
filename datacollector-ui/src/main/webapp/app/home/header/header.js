/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Controller for Header Pane.
 */

angular
  .module('dataCollectorApp.home')
  .controller('HeaderController', function (
    $scope, $rootScope, $timeout, _, api, $translate, $location, authService, pipelineService, pipelineConstant,
    $modal, $q, $route, pipelineTracking, tracking, trackingEvent
  ) {

    var pipelineValidationInProgress;
    var pipelineValidationSuccess;
    var validateConfigStatusTimer;

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
        $scope.trackEvent(pipelineConstant.STAGE_CATEGORY, pipelineConstant.ADD_ACTION, selectedStage.label, 1);
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
        $scope.trackEvent(pipelineConstant.STAGE_CATEGORY, pipelineConstant.CONNECT_ACTION, connectStage.label, 1);
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
        pipelineTracking.trackValidationSelected($scope.pipelineConfig);
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Validate Pipeline', 1);
        $scope.$storage.maximizeDetailPane = false;
        $scope.$storage.minimizeDetailPane = false;

        // Cancel previous validation status check timer
        if (validateConfigStatusTimer) {
          $timeout.cancel(validateConfigStatusTimer);
        }

        var pipelineBeingValidated = $scope.activeConfigInfo;
        $rootScope.common.infoList.push({
          message: pipelineValidationInProgress
        });

        api.pipelineAgent.validatePipeline($scope.activeConfigInfo.pipelineId, $scope.edgeHttpUrl).
          then(
          function (res) {
            var defer = $q.defer();
            checkForValidateConfigStatus($scope.activeConfigInfo.pipelineId, res.data.previewerId, defer);

            defer.promise.then(function(previewData) {
              if (pipelineBeingValidated.pipelineId !== $rootScope.$storage.activeConfigInfo.pipelineId) {
                return;
              }
              $rootScope.common.infoList = [];
              if (previewData.status === 'VALID') {
                // clear previous errors if any
                var commonErrors = $rootScope.common.errors;
                if (commonErrors && commonErrors.length) {
                  $rootScope.common.errors = [];
                  $scope.refreshGraph();
                }

                $rootScope.common.successList.push({
                  message: pipelineValidationSuccess
                });
                pipelineTracking.trackValidationComplete($scope.pipelineConfig, true);
              } else {
                if (previewData.issues) {
                  $rootScope.common.errors = [previewData.issues];
                } else if (previewData.message) {
                  $rootScope.common.errors = [previewData.message];
                }
                pipelineTracking.trackValidationComplete($scope.pipelineConfig, false, previewData);
              }
            });

          },
          function (res) {
            if (pipelineBeingValidated.pipelineId !== $rootScope.$storage.activeConfigInfo.pipelineId) {
              return;
            }
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
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Start Pipeline', 1);
        pipelineTracking.trackRunSelected($scope.pipelineConfig, false);
        if ($rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.pipelineId].status !== 'RUNNING') {
          $scope.$storage.maximizeDetailPane = false;
          $scope.$storage.minimizeDetailPane = false;
          $scope.$storage.readNotifications = [];
          $rootScope.common.pipelineMetrics = {};
          $rootScope.common.errors = [];
          api.pipelineAgent.startPipeline($scope.activeConfigInfo.pipelineId, 0).
            then(
            function (res) {
              $scope.clearTabSelectionCache();
              $scope.selectPipelineConfig();

              var currentStatus = $rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.pipelineId];
              if (!currentStatus || (res.data && currentStatus.timeStamp < res.data.timeStamp)) {
                $rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.pipelineId] = res.data;
              }
              $rootScope.common.previousInputRecordCount[$scope.activeConfigInfo.pipelineId] = 0;
              $rootScope.common.previousBatchCount[$scope.activeConfigInfo.pipelineId] = 0;

              $timeout(function() {
                $scope.refreshGraph();
              });
              $scope.startMonitoring();
            },
            function (res) {
              $rootScope.common.errors = [res.data];
            }
          );
        } else {
          $translate('home.graphPane.startErrorMessage', {
            name: $scope.activeConfigInfo.pipelineId
          }).then(function(translation) {
            $rootScope.common.errors = [translation];
          });
        }
      },

      /**
       * On Start Pipeline With Parameters Click
       */
      startPipelineWithParameters: function() {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Start Pipeline', 1);
        pipelineTracking.trackRunSelected($scope.pipelineConfig, true);
        if ($rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.pipelineId].status !== 'RUNNING') {
          $scope.$storage.maximizeDetailPane = false;
          $scope.$storage.minimizeDetailPane = false;
          $scope.$storage.readNotifications = [];
          $rootScope.common.pipelineMetrics = {};

          var modalInstance = $modal.open({
            templateUrl: 'app/home/header/start/start.tpl.html',
            controller: 'StartModalInstanceController',
            size: '',
            backdrop: 'static',
            resolve: {
              pipelineConfig: function () {
                return $scope.pipelineConfig;
              }
            }
          });

          modalInstance.result.then(function(res) {
            $rootScope.common.errors = [];
            $scope.clearTabSelectionCache();
            $scope.selectPipelineConfig();

            var currentStatus = $rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.pipelineId];
            if (!currentStatus || (res.data && currentStatus.timeStamp < res.data.timeStamp)) {
              $rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.pipelineId] = res.data;
            }

            $timeout(function() {
              $scope.refreshGraph();
            });
            $scope.startMonitoring();
          }, function () {

          });
        }
      },

      /**
       * On Stop Pipeline button click.
       *
       */
      stopPipeline: function(forceStop) {
        if (forceStop) {
          $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Force Stop Pipeline', 1);
        } else {
          $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Stop Pipeline', 1);
        }
        var modalInstance = $modal.open({
          templateUrl: 'app/home/header/stop/stopConfirmation.tpl.html',
          controller: 'StopConfirmationModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return $scope.activeConfigInfo;
            },
            forceStop: function() {
              return forceStop;
            },
            pipelineConfig: function() {
              return $scope.pipelineConfig;
            }
          }
        });

        modalInstance.result.then(function(status) {
          var pipelineId = $scope.activeConfigInfo.pipelineId;

          $scope.clearTabSelectionCache();
          $scope.selectPipelineConfig();

          var currentStatus = $rootScope.common.pipelineStatusMap[pipelineId];
          if (!currentStatus || (status && currentStatus.timeStamp < status.timeStamp)) {
            $rootScope.common.pipelineStatusMap[pipelineId] = status;
          }

          var alerts = $rootScope.common.alertsMap[pipelineId];

          if (alerts) {
            delete $rootScope.common.alertsMap[pipelineId];
            $rootScope.common.alertsTotalCount -= alerts.length;
          }

          $scope.refreshGraph();
          $scope.stopMonitoring();
        }, function () {

        });
      },

      /**
       * View the available snapshots.
       *
       */
      viewSnapshots: function() {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'View Snapshots', 1);
        var modalInstance = $modal.open({
          templateUrl: 'app/home/snapshot/modal/snapshotModal.tpl.html',
          controller: 'SnapshotModalInstanceController',
          size: 'lg',
          backdrop: 'static',
          resolve: {
            pipelineConfig: function () {
              return $scope.pipelineConfig;
            },
            isPipelineRunning: function() {
              return $scope.isPipelineRunning;
            },
            canExecute: function () {
              return $scope.canExecute;
            }
          }
        });

        modalInstance.result.then(function(snapshotName) {
          if (snapshotName) {
            $scope.viewSnapshot(snapshotName);
          }
        }, function () {

        });

      },

      /**
       * Reset Offset of pipeline
       *
       */
      resetOffset: function() {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Reset Offset', 1);

        var originStageInstance = $scope.stageInstances[0];
        var originStageDef = _.find($scope.stageLibraries, function (stageDef) {
          return stageDef.name === originStageInstance.stageName;
        });

        $modal.open({
          templateUrl: 'app/home/resetOffset/resetOffset.tpl.html',
          controller: 'ResetOffsetModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return $scope.activeConfigInfo;
            },
            originStageDef: function() {
              return originStageDef;
            }
          }
        });
      },

      /**
       * Reset Offset & start pipeline
       */
      resetOffsetAndStart: function() {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Reset Offset & Start', 1);

        var originStageInstance = $scope.stageInstances[0];
        var originStageDef = _.find($scope.stageLibraries, function (stageDef) {
          return stageDef.name === originStageInstance.stageName;
        });

        var modalInstance = $modal.open({
          templateUrl: 'app/home/resetOffsetAndStart/resetOffsetAndStart.tpl.html',
          controller: 'ResetOffsetAndStartModalInstanceController',
          size: '',
          backdrop: 'static',
          resolve: {
            pipelineInfo: function () {
              return $scope.activeConfigInfo;
            },
            originStageDef: function() {
              return originStageDef;
            }
          }
        });

        modalInstance.result.then(function(res) {
          $rootScope.common.errors = [];
          $scope.clearTabSelectionCache();
          $scope.selectPipelineConfig();

          var currentStatus = $rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.pipelineId];
          if (!currentStatus || (res.data && currentStatus.timeStamp < res.data.timeStamp)) {
            $rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.pipelineId] = res.data;
          }

          $timeout(function() {
            $scope.refreshGraph();
          });
          $scope.startMonitoring();
        }, function () {

        });
      },

      /**
       * Delete Selected Stage Instance/Stream
       */
      deleteSelection: function() {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Delete Selection', 1);
        $rootScope.$broadcast('deleteSelectionInGraph');
      },

      /**
       * Duplicate Stage
       */
      duplicateStage: function() {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Duplicate Stage', 1);
        if ($scope.selectedType === pipelineConstant.STAGE_INSTANCE) {
          $scope.$emit('onPasteNode', $scope.selectedObject);
        }
      },

      /**
       * Auto arrange stages
       */
      autoArrange: function() {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Auto Arrange', 1);
        pipelineService.autoArrange($scope.stageInstances);
        $scope.refreshGraph();
      },

      /**
       * Share Pipeline Configuration
       */
      sharePipelineConfig: function(pipelineInfo, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Share Pipeline', 1);
        pipelineService.sharePipelineConfigCommand(pipelineInfo, $event);
      },

      /**
       * Delete Pipeline Configuration
       */
      deletePipelineConfig: function(pipelineInfo, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Delete Pipeline', 1);
        pipelineService.deletePipelineConfigCommand(pipelineInfo, $event)
          .then(function(pipelines) {
            $location.path('/');
          });
      },

      /**
       * Duplicate Pipeline Configuration
       */
      duplicatePipelineConfig: function(pipelineInfo, $event) {
        if ($scope.isSamplePipeline) {
          tracking.mixpanel.track(trackingEvent.SAMPLE_PIPELINE_DUPLICATED, {
            'Sample Pipeline ID': pipelineInfo.pipelineId,
            'Sample Pipeline Title': pipelineInfo.title
          });
        } else {
          $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Duplicate Pipeline', 1);
        }

        pipelineService.duplicatePipelineConfigCommand(pipelineInfo, $event, $scope.isSamplePipeline)
          .then(function(newPipelineConfig) {
            if (!angular.isArray(newPipelineConfig)) {
              $location.search({}).path('/collector/pipeline/' + newPipelineConfig.info.pipelineId);
            } else {
              $location.search({}).path('/collector/pipeline/' + newPipelineConfig[0].info.pipelineId);
            }
          });
      },

      /**
       * Import link command handler
       */
      importPipelineConfig: function(pipelineInfo, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Import Pipeline', 1);
        pipelineService.importPipelineConfigCommand(pipelineInfo, $event);
      },

      /**
       * Export link command handler
       */
      exportPipelineConfig: function(pipelineInfo, includeDefinitions, includePlainTextCredentials, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Export Pipeline', 1);
        api.pipelineAgent.exportPipelineConfig(pipelineInfo.pipelineId, includeDefinitions, includePlainTextCredentials);
      },

      publishPipeline: function (pipelineInfo, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Publish Pipeline', 1);

        pipelineService.publishPipelineCommand(pipelineInfo, $event)
          .then(
            function(metadata) {
              $scope.clearUndoRedoArchive();
              $route.reload();

              $timeout(function() {
                $rootScope.common.successList.push({
                  message: 'Successfully Published Pipeline to Control Hub Pipeline Repository: ' +
                  authService.getRemoteBaseUrl() + ' New Pipeline Commit Version - ' + metadata['dpm.pipeline.version']
                });
              });
            });
      },

      showCommitHistory: function (pipelineInfo, metadata) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Show Commit History', 1);
        pipelineService.showCommitHistoryCommand(pipelineInfo, metadata)
          .then(
            function(updatedPipelineConfig) {
              if (updatedPipelineConfig) {
                $route.reload();
                $rootScope.common.successList.push({
                  message: 'Successfully Fetched Pipeline Commit Version - ' +
                  updatedPipelineConfig.metadata['dpm.pipeline.version']
                });
              }
            });
      },

      revertDPMPipelineChanges: function (pipelineInfo, metadata) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Revert Changes', 1);

        pipelineService.revertChangesCommand(pipelineInfo, metadata)
          .then(
            function(metadata) {
              $scope.clearUndoRedoArchive();
              $route.reload();

              $timeout(function() {
                $rootScope.common.successList.push({
                  message: 'Successfully reverted pipeline changes'
                });
              });
            });
      },

      downloadEdgeExecutable: function () {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Download Edge Executable', 1);
        if ($scope.executionMode === 'EDGE') {
          var modalInstance = $modal.open({
            templateUrl: 'app/home/header/downloadExecutable/downloadExecutable.tpl.html',
            controller: 'DownloadEdgeExecutableController',
            size: '',
            backdrop: 'static',
            resolve: {
              pipelineConfig: function () {
                return $scope.pipelineConfig;
              }
            }
          });
        }
      },

      /**
       * Returns true if pipeline is Edge pipeline
       */
      isEdgePipeline: function () {
        return $scope.executionMode === 'EDGE';
      },

      publishToEdge: function () {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Publish Pipeline to Edge', 1);
        api.pipelineAgent.publishPipelinesToEdge([$scope.activeConfigInfo.pipelineId])
          .then(
            function (res) {
              $rootScope.common.successList.push({
                message: 'Successfully published pipeline to Data Collector Edge'
              });
            },
            function (res) {
              $rootScope.common.errors = [res.data];
            }
          );
      },

      viewPipelineLog: function () {
        if ($scope.executionMode === 'EDGE') {
          $rootScope.common.infoList = [{
            message: 'View edge pipeline logs in SDC_EDGE_HOME/log/edge.log file'
          }];
        } else {
          $location.path(
            '/collector/logs/' + $scope.pipelineConfig.info.title + '/' + $scope.pipelineConfig.info.pipelineId
          );
        }
      },

      trackTutorialLinkClicked: function(pipeline) {
        tracking.trackTutorialLinkClicked(pipeline);
      }
    });

    $scope.$on('bodyDeleteKeyPressed', function() {
      $scope.deleteSelection();
    });

    /**
     * Check for Validate Config Status for every 1 seconds, once done open the snapshot view.
     *
     */
    var checkForValidateConfigStatus = function(pipelineId, previewerId, defer) {
      validateConfigStatusTimer = $timeout(
        function() {
        },
        1000
      );

      validateConfigStatusTimer.then(
        function() {
          api.pipelineAgent.getPreviewStatus(pipelineId, previewerId, $scope.edgeHttpUrl)
            .then(function(res) {
              var data = res.data;
              if (data && _.contains(['INVALID', 'VALIDATION_ERROR', 'START_ERROR', 'RUN_ERROR', 'CONNECT_ERROR', 'STOP_ERROR', 'VALID'], data.status)) {
                fetchValidateConfigData(pipelineId, previewerId, defer);
              } else {
                checkForValidateConfigStatus(pipelineId, previewerId, defer);
              }
            })
            .catch(function(res) {
              $rootScope.common.infoList = [];
              $scope.common.errors = [res.data];
            });
        },
        function() {
          console.log( "Timer rejected!" );
        }
      );
    };

    var fetchValidateConfigData = function(pipelineId, previewerId, defer) {
      api.pipelineAgent.getPreviewData(pipelineId, previewerId, $scope.edgeHttpUrl)
        .then(function(res) {
          defer.resolve(res.data);
        })
        .catch(function(res) {
          $rootScope.common.infoList = [];
          $scope.common.errors = [res.data];
        });
    };

    $scope.$on('$destroy', function() {
      if (validateConfigStatusTimer) {
        $timeout.cancel(validateConfigStatusTimer);
      }
    });

  });
