/**
 * Service for providing access to the Preview/Snapshot utility functions.
 */
angular.module('pipelineAgentApp.common')
  .constant('pipelineConstant', {
    SOURCE_STAGE_TYPE : 'SOURCE',
    PROCESSOR_STAGE_TYPE : 'PROCESSOR',
    SELECTOR_STAGE_TYPE : 'SELECTOR',
    TARGET_STAGE_TYPE : 'TARGET',
    STAGE_INSTANCE: 'STAGE_INSTANCE',
    LINK: 'LINK',
    PIPELINE: 'PIPELINE',
    DENSITY_COMFORTABLE: 'COMFORTABLE',
    DENSITY_COZY: 'COZY',
    DENSITY_COMPACT: 'COMPACT'
  });