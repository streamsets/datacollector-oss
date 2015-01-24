/**
 * Service for providing access to the Pipeline utility functions.
 */
angular.module('pipelineAgentApp.common')
  .service('pipelineService', function(pipelineConstant) {

    var self = this;

    var getXPos = function(pipelineConfig, firstOpenLane) {
      var prevStage = (firstOpenLane && firstOpenLane.stageInstance) ? firstOpenLane.stageInstance :
        ((pipelineConfig.stages && pipelineConfig.stages.length) ? pipelineConfig.stages[pipelineConfig.stages.length - 1] : undefined);

      return prevStage ? prevStage.uiInfo.xPos + 220 : 60;
    };

    var getYPos = function(pipelineConfig, firstOpenLane, xPos) {
      var maxYPos = 0;

      if(firstOpenLane) {
        maxYPos = firstOpenLane.stageInstance.uiInfo.yPos - 130;
      }

      angular.forEach(pipelineConfig.stages, function(stage) {
        if(stage.uiInfo.xPos === xPos && stage.uiInfo.yPos > maxYPos) {
          maxYPos = stage.uiInfo.yPos;
        }
      });

      return maxYPos ? maxYPos + 130 : 50;
    };

    /**
     * Construct new instance for give Stage Defintion
     * @param stage
     * @param pipelineConfig
     * @param labelSuffix [Optional]
     * @param firstOpenLane [Optional]
     * @param relativeXPos [Optional]
     * @param relativeYPos [Optional]
     * @returns {{instanceName: *, library: (*|stageInstance.library|library|e.library), stageName: *, stageVersion: *, configuration: Array, uiInfo: {label: *, description: string, xPos: *, yPos: number, stageType: *}, inputLanes: Array, outputLanes: Array}}
     */
    this.getNewStageInstance = function (stage, pipelineConfig, labelSuffix, firstOpenLane, relativeXPos, relativeYPos) {
      var xPos = relativeXPos || getXPos(pipelineConfig, firstOpenLane),
        yPos = relativeYPos || getYPos(pipelineConfig, firstOpenLane, xPos),
        stageLabel = self.getStageLabel(stage, pipelineConfig),
        stageInstance = {
          instanceName: stage.name + (new Date()).getTime() + (labelSuffix ? labelSuffix : ''),
          library: stage.library,
          stageName: stage.name,
          stageVersion: stage.version,
          configuration: [],
          uiInfo: {
            label: stageLabel,
            description: '', //stage.description,
            xPos: xPos,
            yPos: yPos,
            stageType: stage.type
          },
          inputLanes: [],
          outputLanes: []
        };

      if(firstOpenLane && firstOpenLane.laneName) {
        stageInstance.inputLanes.push(firstOpenLane.laneName);
      }

      if (stage.type !== pipelineConstant.TARGET_STAGE_TYPE) {
        stageInstance.outputLanes = [stageInstance.instanceName + 'OutputLane' + (new Date()).getTime()];
      }

      angular.forEach(stage.configDefinitions, function (configDefinition) {
        stageInstance.configuration.push(self.setDefaultValueForConfig(configDefinition, stageInstance));
      });

      if(stage.rawSourceDefinition && stage.rawSourceDefinition.configDefinitions) {

        stageInstance.uiInfo.rawSource = {
          configuration: []
        };

        angular.forEach(stage.rawSourceDefinition.configDefinitions, function (configDefinition) {
          stageInstance.uiInfo.rawSource.configuration.push(self.setDefaultValueForConfig(configDefinition, stageInstance));
        });
      }

      stageInstance.uiInfo.icon = self.getStageIconURL(stage);

      return stageInstance;
    };

    /**
     * Return Stage Label
     *
     * @param stage
     * @param pipelineConfig
     * @returns {*}
     */
    this.getStageLabel = function(stage, pipelineConfig) {
      var label = stage.label,
        similarStageInstances = _.filter(pipelineConfig.stages, function(stageInstance) {
          return stageInstance.uiInfo.label.indexOf(label) !== -1;
        });

      return label + (similarStageInstances.length + 1);
    };


    /**
     * Sets default value for config.
     *
     * @param configDefinition
     * @param stageInstance
     * @returns {{name: *, value: (defaultValue|*|string|Variable.defaultValue|undefined)}}
     */
    this.setDefaultValueForConfig = function(configDefinition, stageInstance) {
      var config = {
          name: configDefinition.name,
          value: configDefinition.defaultValue || undefined
        };

      if(configDefinition.type === 'MODEL') {
        if(configDefinition.model.modelType === 'FIELD_SELECTOR_MULTI_VALUED') {
          config.value = [];
        } else if(configDefinition.model.modelType === 'LANE_PREDICATE_MAPPING') {
          config.value = [{
            outputLane: stageInstance.outputLanes[0],
            predicate: ''
          }];
        } else if(configDefinition.model.modelType === 'COMPLEX_FIELD') {
          var complexFieldObj = {};
          angular.forEach(configDefinition.model.configDefinitions, function (complexFiledConfigDefinition) {
            var complexFieldConfig = self.setDefaultValueForConfig(complexFiledConfigDefinition, stageInstance);
            complexFieldObj[complexFieldConfig.name] = complexFieldConfig.value || '';
          });
          config.value = [complexFieldObj];
        }
      } else if(configDefinition.type === 'INTEGER') {
        if(config.value) {
          config.value = parseInt(config.value);
        } else {
          config.value = 0;
        }
      } else if(configDefinition.type === 'BOOLEAN' && config.value === undefined) {
        config.value = false;
      } else if(configDefinition.type === 'LIST') {
        config.value = [];
      } else if(configDefinition.type === 'MAP') {
        config.value = [];
      }

      return config;
    };

    /**
     * Return Stage Icon URL
     *
     * @param stage
     * @returns {string}
     */
    this.getStageIconURL = function(stage) {
      if(stage.icon) {
        return 'rest/v1/definitions/stages/icon?name=' + stage.name +
        '&library=' + stage.library + '&version=' + stage.version;
      } else {
        switch(stage.type) {
          case pipelineConstant.SOURCE_STAGE_TYPE:
            return 'assets/stage/defaultSource.svg';
          case pipelineConstant.PROCESSOR_STAGE_TYPE:
            return 'assets/stage/defaultProcessor.svg';
          case pipelineConstant.TARGET_STAGE_TYPE:
            return 'assets/stage/defaultTarget.svg';
        }
      }
    };

    /**
     * Returns Metric element list
     */
    this.getMetricElementList = function() {
      return {
        COUNTER: [
          {
            value: 'COUNTER_COUNT',
            label: 'count'
          }
        ],
        HISTOGRAM: [
          {
            value: 'HISTOGRAM_COUNT',
            label: 'count'
          },
          {
            value: 'HISTOGRAM_MAX',
            label: 'max'
          },
          {
            value: 'HISTOGRAM_MIN',
            label: 'mean'
          },
          {
            value: 'HISTOGRAM_MEAN',
            label: 'min'
          },
          {
            value: 'HISTOGRAM_P50',
            label: 'p50'
          },
          {
            value: 'HISTOGRAM_P75',
            label: 'p75'
          },
          {
            value: 'HISTOGRAM_P95',
            label: 'p95'
          },
          {
            value: 'HISTOGRAM_P98',
            label: 'p98'
          },
          {
            value: 'HISTOGRAM_P99',
            label: 'p99'
          },
          {
            value: 'HISTOGRAM_P999',
            label: 'p999'
          },
          {
            value: 'HISTOGRAM_STD_DEV',
            label: 'stddev'
          }
        ],
        METER: [
          {
            value: 'METER_COUNT',
            label: 'count'
          },
          {
            value: 'METER_M1_RATE',
            label: 'm1_rate'
          },
          {
            value: 'METER_M5_RATE',
            label: 'm5_rate'
          },
          {
            value: 'METER_M15_RATE',
            label: 'm15_rate'
          },
          {
            value: 'METER_M30_RATE',
            label: 'm30_rate'
          },
          {
            value: 'METER_H1_RATE',
            label: 'h1_rate'
          },
          {
            value: 'METER_H6_RATE',
            label: 'h6_rate'
          },
          {
            value: 'METER_H12_RATE',
            label: 'h12_rate'
          },
          {
            value: 'METER_H24_RATE',
            label: 'h24_rate'
          },
          {
            value: 'METER_MEAN_RATE',
            label: 'mean_rate'
          }
        ],
        TIMER: [
          {
            value: 'TIMER_COUNT',
            label: 'count'
          },
          {
            value: 'TIMER_MAX',
            label: 'max'
          },
          {
            value: 'TIMER_MEAN',
            label: 'mean'
          },
          {
            value: 'TIMER_MIN',
            label: 'min'
          },
          {
            value: 'TIMER_P50',
            label: 'p50'
          },
          {
            value: 'TIMER_P75',
            label: 'p75'
          },
          {
            value: 'TIMER_P95',
            label: 'p95'
          },
          {
            value: 'TIMER_P98',
            label: 'p98'
          },
          {
            value: 'TIMER_P99',
            label: 'p99'
          },
          {
            value: 'TIMER_P999',
            label: 'p999'
          },
          {
            value: 'TIMER_STD_DEV',
            label: 'stddev'
          },
          {
            value: 'TIMER_M1_RATE',
            label: 'm1_rate'
          },
          {
            value: 'TIMER_M5_RATE',
            label: 'm5_rate'
          },
          {
            value: 'TIMER_M15_RATE',
            label: 'm15_rate'
          },
          {
            value: 'TIMER_MEAN_RATE',
            label: 'mean_rate'
          }
        ]
      };
    };


    /**
     * Returns metric element list for the given pipeline.
     *
     * @param pipelineConfig
     */
    this.getMetricIDList = function(pipelineConfig) {
      var metricIDList = {
        COUNTER: [],
        HISTOGRAM: [
            {
              value: 'pipeline.inputRecordsPerBatch.histogramM5',
              label: 'Pipeline Input Records Per Batch Histogram M5'
            },
            {
              value: 'pipeline.outputRecordsPerBatch.histogramM5',
              label: 'Pipeline Output Records Per Batch Histogram M5'
            },
            {
              value: 'pipeline.errorRecordsPerBatch.histogramM5',
              label: 'Pipeline Error Records Per Batch Histogram M5'
            },
            {
              value: 'pipeline.errorsPerBatch.histogramM5',
              label: 'Pipeline Errors Per Batch Histogram M5'
            }
          ],
        METER: [
            {
              value: 'pipeline.batchCount.meter',
              label: 'Pipeline Batch Count Meter'
            },
            {
              value: 'pipeline.batchInputRecords.meter',
              label: 'Pipeline Batch Input Records Meter'
            },
            {
              value: 'pipeline.batchOutputRecords.meter',
              label: 'Pipeline Batch Output Records Meter '
            },
            {
              value: 'pipeline.batchErrorRecords.meter',
              label: 'Pipeline Batch Error Records Meter'
            },
            {
              value: 'pipeline.batchErrorMessages.meter',
              label: 'Pipeline Batch Error Messages Meter'
            }
          ],
        TIMER: [{
            value: 'pipeline.batchProcessing.timer',
            label: 'Pipeline Batch Processing Timer'
          }]
        };



      angular.forEach(pipelineConfig.stages, function(stage) {
        var instanceName = stage.instanceName,
          label = stage.uiInfo.label;

        //Counters
        metricIDList.COUNTER.push.apply(metricIDList.COUNTER, [
          {
            value: 'stage.' + instanceName + '.inputRecords.counter',
            label: label + ' Input Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.outputRecords.counter',
            label: label + ' Output Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.errorRecords.counter',
            label: label + ' Error Records Counter'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.counter',
            label: label + ' Stage Errors Counter'
          }
        ]);

        //histograms
        metricIDList.HISTOGRAM.push.apply(metricIDList.HISTOGRAM, [
          {
            value: 'stage.' + instanceName + '.inputRecords.histogramM5',
            label: label + ' Input Records Histogram'
          },
          {
            value: 'stage.' + instanceName + '.outputRecords.histogramM5',
            label: label + ' Output Records Histogram'
          },
          {
            value: 'stage.' + instanceName + '.errorRecords.histogramM5',
            label: label + ' Error Records Histogram'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.histogramM5',
            label: label + ' Stage Errors Histogram'
          }
        ]);

        //meters
        metricIDList.METER.push.apply(metricIDList.METER, [
          {
            value: 'stage.' + instanceName + '.inputRecords.meter',
            label: label + ' Input Records Meter'
          },
          {
            value: 'stage.' + instanceName + '.outputRecords.meter',
            label: label + ' Output Records Meter'
          },
          {
            value: 'stage.' + instanceName + '.errorRecords.meter',
            label: label + ' Error Records Meter'
          },
          {
            value: 'stage.' + instanceName + '.stageErrors.meter',
            label: label + ' Stage Errors Meter'
          }
        ]);


        metricIDList.TIMER.push({
          value: 'stage.' + instanceName + '.batchProcessing.timer',
          label: label + ' Batch Processing Timer'
        });

      });

      return metricIDList;
    };


  });