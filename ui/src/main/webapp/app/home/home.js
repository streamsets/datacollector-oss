/**
 * Home module for displaying home page content.
 */

angular
  .module('pipelineAgentApp.home')
  .config(['$routeProvider', function ($routeProvider) {
    $routeProvider.when('/',
      {
        templateUrl: 'app/home/home.tpl.html',
        controller: 'HomeController'
      }
    );
  }])
  .controller('HomeController', function ($scope, $rootScope, $timeout, api, configuration, _, $q, $modal, pipelineConstant) {
    var stageCounter = 0,
      timeout,
      dirty = false,
      ignoreUpdate = false,
      pipelineStatusTimer,
      edges = [];

    angular.extend($scope, {
      loaded: false,
      isPipelineRunning: false,
      pipelines: [],
      sourceExists: false,
      stageLibraries: [],
      pipelineGraphData: {},
      previewMode: false,
      snapshotMode: false,
      hideLibraryPanel: true,
      activeConfigInfo: undefined,
      activeConfigStatus:{
        state: 'STOPPED'
      },
      minimizeDetailPane: false,
      maximizeDetailPane: false,

      /**
       * Add New Pipeline Configuration
       */
      addPipelineConfig: function() {
        $scope.$broadcast('addPipelineConfig');
      },

      /**
       * Utility function for checking object is empty.
       *
       * @param obj
       * @returns {*|boolean}
       */
      isEmptyObject : function (obj) {
        return angular.equals({},obj);
      },

      /**
       * Value format function for D3 NVD3 charts.
       *
       * @returns {Function}
       */
      valueFormatFunction: function() {
        return function(d){
          return d3.format(',d')(d);
        };
      },

      /**
       * Fetches preview data for the pipeline and sets previewMode flag to true.
       *
       * @param nextBatch - By default it starts fetching from sourceOffset=0, if nextBatch is true sourceOffset is
       * updated to fetch next batch.
       */
      previewPipeline: function (nextBatch) {
        $scope.previewMode = true;
        $scope.$broadcast('previewPipeline', nextBatch);
      },

      /**
       * Sets previewMode flag to false.
       */
      closePreview: function () {
        $scope.previewMode = false;
      },

      /**
       * Capture the snapshot of running pipeline.
       *
       */
      captureSnapshot: function() {
        $scope.snapshotMode = true;
        $scope.$broadcast('snapshotPipeline');
      },


      /**
       * Sets previewMode flag to false.
       */
      closeSnapshot: function () {
        $scope.snapshotMode = false;
      },

      /**
       * Update Preview Stage Instance.
       *
       * @param stageInstance
       */
      changeStageSelection: function(stageInstance) {
        $scope.$broadcast('selectNode', stageInstance);
        updateDetailPane(stageInstance);
      },

      /**
       * Toggle Library Panel
       */
      toggleLibraryPanel: function() {
        $scope.hideLibraryPanel = ! $scope.hideLibraryPanel;
      },

      /**
       * On Detail Pane Minimize button is clicked.
       */
      onMinimizeDetailPane: function() {
        $scope.maximizeDetailPane = false;
        $scope.minimizeDetailPane = !$scope.minimizeDetailPane;
      },

      /**
       * On Detail Pane Maximize button is clicked.
       */
      onMaximizeDetailPane: function() {
        $scope.minimizeDetailPane = false;
        $scope.maximizeDetailPane = !$scope.maximizeDetailPane;
      },

      /**
       * Update detailPaneConfig & detailPaneConfigDefn from child scope.
       *
       * @param stageInstance
       * @param stage
       */
      updateDetailPaneObject: function(stageInstance, stage) {
        $scope.detailPaneConfig = stageInstance;
        $scope.detailPaneConfigDefn = stage;
      }

    });


    /**
     * Fetch definitions for Pipeline and Stages, fetch all pipeline configuration info, status and metric.
     */
    $q.all([
      api.pipelineAgent.getDefinitions(),
      api.pipelineAgent.getPipelines(),
      api.pipelineAgent.getPipelineStatus(),
      api.pipelineAgent.getPipelineMetrics(),
      configuration.init()
    ])
      .then(function (results) {
        var definitions = results[0].data,
          pipelines = results[1].data,
          pipelineStatus = results[2].data,
          pipelineMetrics= results[3].data;

        //Definitions
        $scope.pipelineConfigDefinition = definitions.pipeline[0];
        $scope.stageLibraries = definitions.stages;

        $scope.sources = _.filter($scope.stageLibraries, function (stageLibrary) {
          return stageLibrary.type === pipelineConstant.SOURCE_STAGE_TYPE;
        });

        $scope.processors = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === pipelineConstant.PROCESSOR_STAGE_TYPE &&
            stageLibrary.name !== 'com_streamsets_pipeline_lib_stage_processor_selector_SelectorProcessor');
        });

        //TODO: Remove hard coding once backend supports modeling selector type
        $scope.selectorProcessors = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === pipelineConstant.PROCESSOR_STAGE_TYPE &&
            stageLibrary.name === 'com_streamsets_pipeline_lib_stage_processor_selector_SelectorProcessor');
        });

        $scope.targets = _.filter($scope.stageLibraries, function (stageLibrary) {
          return (stageLibrary.type === pipelineConstant.TARGET_STAGE_TYPE);
        });

        //Pipelines
        $scope.pipelines = pipelines;

        $rootScope.common.pipelineStatus = pipelineStatus;

        if(pipelineStatus && pipelineStatus.name) {
          $scope.activeConfigInfo = _.find($scope.pipelines, function(pipelineDefn) {
            return pipelineDefn.name === pipelineStatus.name;
          });
        }

        if(!$scope.activeConfigInfo && $scope.pipelines && $scope.pipelines.length) {
          $scope.activeConfigInfo =   $scope.pipelines[0];
        }

        $rootScope.common.pipelineMetrics = pipelineMetrics;

        refreshPipelineStatus();
        refreshPipelineMetrics();

        if($scope.activeConfigInfo) {
          return api.pipelineAgent.getPipelineConfig($scope.activeConfigInfo.name);
        }

      },function(data, status, headers, config) {
          $rootScope.common.errors = [data];
      })
      .then(function(res) {
        //Pipeline Configuration
        if(res && res.data) {
          updateGraph(res.data);
        }
        $scope.loaded = true;
      },function(data, status, headers, config) {
        $rootScope.common.errors = [data];
      });

    /**
     * Load Pipeline Configuration by fetching it from server for the given Pipeline Configuration name.
     * @param configName
     */
    var loadPipelineConfig = function(configName) {
      api.pipelineAgent.getPipelineConfig(configName).
        success(function(res) {
          $rootScope.common.errors = [];
          updateGraph(res);
        }).
        error(function(data, status, headers, config) {
          $rootScope.common.errors = [data];
        });
    };

    /**
     * Save Updates
     * @param config
     */
    var saveUpdates = function (config) {
      if ($rootScope.common.saveOperationInProgress) {
        return;
      }

      if (!config) {
        config = _.clone($scope.pipelineConfig);
      }

      dirty = false;
      $rootScope.common.saveOperationInProgress = true;
      api.pipelineAgent.savePipelineConfig($scope.activeConfigInfo.name, config).
        success(function (res) {
          $rootScope.common.saveOperationInProgress = false;

          if (dirty) {
            config = _.clone($scope.pipelineConfig);
            config.uuid = res.uuid;

            //Updated new changes in return config
            res.configuration = config.configuration;
            res.uiInfo = config.uiInfo;
            res.stages = config.stages;

            saveUpdates(config);
          }
          updateGraph(res);
        }).
        error(function(data, status, headers, config) {
          $rootScope.common.errors = [data];
        });
    };

    /**
     * Update Pipeline Graph
     *
     * @param pipelineConfig
     */
    var updateGraph = function (pipelineConfig) {
      var selectedStageInstance,
        stageErrorCounts,
        pipelineMetrics = $rootScope.common.pipelineMetrics,
        pipelineStatus = $rootScope.common.pipelineStatus;

      ignoreUpdate = true;

      //Force Validity Check - showErrors directive
      $scope.$broadcast('show-errors-check-validity');

      $scope.pipelineConfig = pipelineConfig || {};
      $scope.activeConfigInfo = pipelineConfig.info;

      //Update Pipeline Info list
      var index = _.indexOf($scope.pipelines, _.find($scope.pipelines, function(pipeline){
        return pipeline.name === pipelineConfig.info.name;
      }));
      $scope.pipelines[index] = pipelineConfig.info;

      stageCounter = ($scope.pipelineConfig && $scope.pipelineConfig.stages) ?
        $scope.pipelineConfig.stages.length : 0;

      //Determine edges from input lanes and output lanes
      //And also set flag sourceExists if pipeline Config contains source
      edges = [];
      $scope.sourceExists = false;
      angular.forEach($scope.pipelineConfig.stages, function (sourceStageInstance) {
        if(sourceStageInstance.uiInfo.stageType === pipelineConstant.SOURCE_STAGE_TYPE) {
          $scope.sourceExists = true;
        }

        if (sourceStageInstance.outputLanes && sourceStageInstance.outputLanes.length) {
          angular.forEach(sourceStageInstance.outputLanes, function (outputLane) {
            angular.forEach($scope.pipelineConfig.stages, function (targetStageInstance) {
              if (targetStageInstance.inputLanes && targetStageInstance.inputLanes.length &&
                _.contains(targetStageInstance.inputLanes, outputLane)) {
                edges.push({
                  source: sourceStageInstance,
                  target: targetStageInstance,
                  outputLane: outputLane
                });
              }
            });
          });
        }
      });



      if(pipelineStatus && pipelineStatus.name === pipelineConfig.info.name &&
        pipelineMetrics && pipelineMetrics.meters) {
        stageErrorCounts = getStageErrorCounts();
      }

      $scope.$broadcast('updateGraph', {
        nodes: $scope.pipelineConfig.stages,
        edges: edges,
        issues: $scope.pipelineConfig.issues,
        selectNode: ($scope.detailPaneConfig && !$scope.detailPaneConfig.stages) ? $scope.detailPaneConfig : undefined,
        stageErrorCounts: stageErrorCounts
      });

      $scope.stageSelected = false;

      if ($scope.detailPaneConfig === undefined) {
        //First time
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.detailPaneConfig = $scope.pipelineConfig;
      } else {
        //Check
        if ($scope.detailPaneConfig.stages) {
          //In case of detail pane is Pipeline Configuration
          $scope.detailPaneConfig = $scope.pipelineConfig;
        } else {
          //In case of detail pane is stage instance
          angular.forEach($scope.pipelineConfig.stages, function (stageInstance) {
            if (stageInstance.instanceName === $scope.detailPaneConfig.instanceName) {
              selectedStageInstance = stageInstance;
            }
          });

          if (selectedStageInstance) {
            $scope.detailPaneConfig = selectedStageInstance;
            $scope.stageSelected = true;
          } else {
            $scope.detailPaneConfig = $scope.pipelineConfig;
            $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
          }

        }
      }
    };

    /**
     * Update Detail Pane when selection changes in Pipeline Graph.
     *
     * @param stageInstance
     */
    var updateDetailPane = function(stageInstance) {

      if(stageInstance) {
        $scope.stageSelected = true;
        //Stage Instance Configuration
        $scope.detailPaneConfig = stageInstance;
        $scope.detailPaneConfigDefn = _.find($scope.stageLibraries, function (stageLibrary) {
          return stageLibrary.name === stageInstance.stageName &&
            stageLibrary.version === stageInstance.stageVersion;
        });
      } else {
        //Pipeline Configuration
        $scope.stageSelected = false;
        $scope.detailPaneConfigDefn = $scope.pipelineConfigDefinition;
        $scope.detailPaneConfig = $scope.pipelineConfig;
      }
      $scope.$broadcast('onStageSelection', stageInstance);

      $timeout(function () {
        $scope.$broadcast('show-errors-check-validity');
      }, 100);
    };


    /**
     * Fetch the Pipeline Status every 2 Seconds.
     *
     */
    var refreshPipelineStatus = function() {

      pipelineStatusTimer = $timeout(
        function() {
          //console.log( "Pipeline Status Timeout executed", Date.now() );
        },
        configuration.getRefreshInterval()
      );

      pipelineStatusTimer.then(
        function() {
          api.pipelineAgent.getPipelineStatus()
            .success(function(data) {
              $rootScope.common.pipelineStatus = data;
              refreshPipelineStatus();
            })
            .error(function(data, status, headers, config) {
              $rootScope.common.errors = [data];
            });
        },
        function() {
          console.log( "Timer rejected!" );
        }
      );
    };


    /**
     * Fetch the Pipeline Status every 2 Seconds.
     *
     */
    var refreshPipelineMetrics = function() {

      pipelineStatusTimer = $timeout(
        function() {
          //console.log( "Pipeline Metrics Timeout executed", Date.now() );
        },
        configuration.getRefreshInterval()
      );

      pipelineStatusTimer.then(
        function() {
          api.pipelineAgent.getPipelineMetrics()
            .success(function(data) {
              $rootScope.common.pipelineMetrics = data;
              refreshPipelineMetrics();
            })
            .error(function(data, status, headers, config) {
              $rootScope.common.errors = [data];
            });
        },
        function() {
          console.log( "Timer rejected!" );
        }
      );
    };


    var getStageErrorCounts = function() {
      var stageInstanceErrorCounts = {};

      angular.forEach($scope.pipelineConfig.stages, function(stageInstance) {
        stageInstanceErrorCounts[stageInstance.instanceName] = Math.round(
          $rootScope.common.pipelineMetrics.histograms['stage.' + stageInstance.instanceName + '.errorRecords.histogramM5'].mean +
          $rootScope.common.pipelineMetrics.histograms['stage.' + stageInstance.instanceName + '.stageErrors.histogramM5'].mean
        );
      });

      return stageInstanceErrorCounts;
    };

    //Event Handling

    $scope.$watch('pipelineConfig', function (newValue, oldValue) {
      if (ignoreUpdate) {
        $timeout(function () {
          ignoreUpdate = false;
        });
        return;
      }
      if (!angular.equals(newValue, oldValue)) {
        dirty = true;
        if (timeout) {
          $timeout.cancel(timeout);
        }
        timeout = $timeout(saveUpdates, 1000);
      }
    }, true);

    $scope.$on('onNodeSelection', function (event, stageInstance) {
      updateDetailPane(stageInstance);
    });

    $scope.$on('onRemoveNodeSelection', function () {
      updateDetailPane();
    });

    $scope.$on('onPipelineConfigSelect', function(event, configInfo) {
      if(configInfo) {
        $scope.activeConfigInfo = configInfo;
        $scope.closePreview();
        loadPipelineConfig($scope.activeConfigInfo.name);
      } else {
        //No Pipieline config exists
        ignoreUpdate = true;
        $scope.pipelineConfig = undefined;
      }
    });

    //Preview Panel Events
    $scope.$on('changeStateInstance', function (event, stageInstance) {
      updateDetailPane(stageInstance);
    });

    var derivePipelineRunning = function() {
      var pipelineStatus = $rootScope.common.pipelineStatus,
        config = $scope.pipelineConfig;
      return (pipelineStatus && config && pipelineStatus.name === config.info.name &&
          pipelineStatus.state === 'RUNNING');
    };

    var derivePipelineStatus = function() {
      var pipelineStatus = $rootScope.common.pipelineStatus,
        config = $scope.pipelineConfig;

      if(pipelineStatus && config && pipelineStatus.name === config.info.name) {
        return pipelineStatus;
      } else {
        return {
          state: 'STOPPED'
        };
      }
    };

    $scope.$watch('pipelineConfig.info.name', function() {
      $scope.isPipelineRunning = derivePipelineRunning();
      $scope.activeConfigStatus = derivePipelineStatus();
    });

    $rootScope.$watch('common.pipelineStatus', function() {
      $scope.isPipelineRunning = derivePipelineRunning();
      $scope.activeConfigStatus = derivePipelineStatus();
    });

    $rootScope.$watch('common.pipelineMetrics', function() {
      var pipelineStatus = $rootScope.common.pipelineStatus,
        config = $scope.pipelineConfig;
      if(pipelineStatus && config && pipelineStatus.name === config.info.name &&
        $scope.isPipelineRunning && $rootScope.common.pipelineMetrics) {
        $scope.$broadcast('updateErrorCount', getStageErrorCounts());
      }
    });

  });