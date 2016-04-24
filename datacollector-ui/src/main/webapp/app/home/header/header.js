/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

  .controller('HeaderController', function ($scope, $rootScope, $timeout, _, api, $translate,
                                           pipelineService, pipelineConstant, $modal, $q, $route) {


    var pipelineValidationInProgress = 'Validating Pipeline...',
      pipelineValidationSuccess = 'Validation Successful.',
      validateConfigStatusTimer;

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
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Validate Pipeline', 1);
        $scope.$storage.maximizeDetailPane = false;
        $scope.$storage.minimizeDetailPane = false;
        $rootScope.common.infoList.push({
          message:pipelineValidationInProgress
        });
        api.pipelineAgent.validatePipeline($scope.activeConfigInfo.name).
          then(
          function (res) {
            var defer = $q.defer();
            checkForValidateConfigStatus(res.data.previewerId, defer);

            defer.promise.then(function(previewData) {
              $rootScope.common.infoList = [];
              if(previewData.status === 'VALID') {
                // clear previous errors if any
                var commonErrors = $rootScope.common.errors;
                if(commonErrors && commonErrors.length) {
                  $rootScope.common.errors = [];
                  $scope.refreshGraph();
                }

                $rootScope.common.successList.push({
                  message: pipelineValidationSuccess
                });
              } else {
                if(previewData.issues) {
                  $rootScope.common.errors = [previewData.issues];
                } else if(previewData.message) {
                  $rootScope.common.errors = [previewData.message];
                }
              }
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
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Start Pipeline', 1);
        if($rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.name].status !== 'RUNNING') {
          var startResponse;
          $scope.$storage.maximizeDetailPane = false;
          $scope.$storage.minimizeDetailPane = false;
          $scope.$storage.readNotifications = [];
          $rootScope.common.pipelineMetrics = {};
          api.pipelineAgent.startPipeline($scope.activeConfigInfo.name, 0).
            then(
            function (res) {
              $scope.clearTabSelectionCache();
              $scope.selectPipelineConfig();
              $rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.name] = res.data;

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
            name: $scope.activeConfigInfo.name
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
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Stop Pipeline', 1);
        var modalInstance = $modal.open({
          templateUrl: 'app/home/header/stop/stopConfirmation.tpl.html',
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
          $scope.clearTabSelectionCache();
          $scope.selectPipelineConfig();
          $rootScope.common.pipelineStatusMap[$scope.activeConfigInfo.name] = status;
          var alerts = $rootScope.common.alertsMap[$scope.activeConfigInfo.name];

          if(alerts) {
            delete $rootScope.common.alertsMap[$scope.activeConfigInfo.name];
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
            }
          }
        });

        modalInstance.result.then(function(snapshotName) {
          if(snapshotName) {
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

        var originStageInstance = $scope.pipelineConfig.stages[0],
            originStageDef = _.find($scope.stageLibraries, function(stageDef) {
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
        if($scope.selectedType === pipelineConstant.STAGE_INSTANCE) {
          $scope.$emit('onPasteNode', $scope.selectedObject);
        }
      },

      /**
       * Auto arrange stages
       */
      autoArrange: function() {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Auto Arrange', 1);
        pipelineService.autoArrange($scope.pipelineConfig);
        $scope.refreshGraph();
      },

      /**
       * Delete Pipeline Configuration
       */
      deletePipelineConfig: function(pipelineInfo, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Delete Pipeline', 1);
        pipelineService.deletePipelineConfigCommand(pipelineInfo, $event);
      },

      /**
       * Duplicate Pipeline Configuration
       */
      duplicatePipelineConfig: function(pipelineInfo, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Duplicate Pipeline', 1);
        pipelineService.duplicatePipelineConfigCommand(pipelineInfo, $event);
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
      exportPipelineConfig: function(pipelineInfo, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Export Pipeline', 1);
        api.pipelineAgent.exportPipelineConfig(pipelineInfo.name);
      },

      publishPipeline: function (pipelineInfo, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Publish Pipeline', 1);

        pipelineService.publishPipelineCommand(pipelineInfo, $event)
          .then(
            function(metadata) {
              $scope.pipelineConfig.metadata = metadata;
              $rootScope.common.successList.push({
                message: 'Successfully Published Pipeline. New Pipeline Commit Version - ' +
                metadata['dpm.pipeline.version']
              });
              $scope.clearUndoRedoArchive();
            });
      },

      showCommitHistory: function (pipelineInfo, $event) {
        $scope.trackEvent(pipelineConstant.BUTTON_CATEGORY, pipelineConstant.CLICK_ACTION, 'Show Commit History', 1);
        pipelineService.showCommitHistoryCommand(pipelineInfo, $event)
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
      }
    });

    $scope.$on('bodyDeleteKeyPressed', function() {
      $scope.deleteSelection();
    });

    /**
     * Check for Validate Config Status for every 1 seconds, once done open the snapshot view.
     *
     */
    var checkForValidateConfigStatus = function(previewerId, defer) {
      validateConfigStatusTimer = $timeout(
        function() {
        },
        1000
      );

      validateConfigStatusTimer.then(
        function() {
          api.pipelineAgent.getPreviewStatus(previewerId)
            .success(function(data) {
              if(data && _.contains(['INVALID', 'VALIDATION_ERROR', 'START_ERROR', 'RUN_ERROR', 'CONNECT_ERROR', 'VALID'], data.status)) {
                fetchValidateConfigData(previewerId, defer);
              } else {
                checkForValidateConfigStatus(previewerId, defer);
              }
            })
            .error(function(data, status, headers, config) {
              $rootScope.common.infoList = [];
              $scope.common.errors = [data];
            });
        },
        function() {
          console.log( "Timer rejected!" );
        }
      );
    };

    var fetchValidateConfigData = function(previewerId, defer) {
      api.pipelineAgent.getPreviewData(previewerId)
        .success(function(validateConfigData) {
          defer.resolve(validateConfigData);
        })
        .error(function(data, status, headers, config) {
          $rootScope.common.infoList = [];
          $scope.common.errors = [data];
        });
    };



    $scope.$on('$destroy', function() {
      if(validateConfigStatusTimer) {
        $timeout.cancel(validateConfigStatusTimer);
      }
    });

  });
