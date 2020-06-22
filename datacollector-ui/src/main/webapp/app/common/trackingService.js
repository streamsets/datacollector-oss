/*
 * Copyright 2020 StreamSets Inc.
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
 * Service for calling tracking tools if possible
 */

angular.module('dataCollectorApp.common')
  .constant('trackingEvent', {
    ACTIVATION: 'Product Activated',
    ACTIVATION_EXPIRING_SOON: 'Activation key expiring soon',
    ACTIVATION_INVALID: 'Activation key invalid',
    ACTIVATION_EXPIRED: 'Activation key expired',
    LOGIN_COMPLETE: 'Login Complete',
    LOGOUT: 'Logout',
    PIPELINE_ERROR: 'Pipeline Error',
    VALIDATION_SELECTED: 'Validation Selected',
    VALIDATION_COMPLETE: 'Validation Complete',
    PREVIEW_SELECTED: 'Preview Selected',
    PREVIEW_CONFIG_COMPLETE: 'Preview Config Complete',
    PREVIEW_COMPLETE: 'Preview Complete',
    PREVIEW_CANCELLED: 'Preview Cancelled',
    PREVIEW_CLOSED: 'Preview Closed',
    RUN_SELECTED: 'Run Selected',
    RUN_REPORTED: 'Run Reported',
    FIRST_MICROBATCH: 'First Pipeline Microbatch',
    PIPELINE_STOP_REQUESTED: 'Pipeline Stop Requested',
    PIPELINE_STOPPED: 'Pipeline Stopped',
    ORIGIN_ADDED: 'Origin Added',
    PROCESSOR_ADDED: 'Processor Added',
    DESTINATION_ADDED: 'Destination Added',
    EXECUTOR_ADDED: 'Executor Added',
    UNKNOWN_ADDED: 'Stage with unknown type added',
    ORIGIN_REMOVED: 'Origin Removed',
    PROCESSOR_REMOVED: 'Processor Removed',
    DESTINATION_REMOVED: 'Destination Removed',
    EXECUTOR_REMOVED: 'Executor Removed',
    UNKNOWN_REMOVED: 'Stage with unknown type removed',
    PIPELINE_CREATED: 'New Pipeline Created',
    PIPELINE_IMPORT_START: 'Import Pipeline Started',
    PIPELINE_IMPORT_START_FROM_ARCHIVE: 'Import Pipelines From Archive Started',
    PIPELINE_IMPORT_START_FROM_URL: 'Import Pipeline From URL Started',
    PIPELINE_IMPORT_COMPLETE: 'Import Pipeline Completed',
    PIPELINE_IMPORT_COMPLETE_FROM_ARCHIVE: 'Import Pipeline From Archive Completed',
    PIPELINE_IMPORT_COMPLETE_FROM_URL: 'Import Pipeline From URL Completed',
    PIPELINE_IMPORT_UPDATE_COMPLETE: 'Import Pipeline Update Completed',
    PIPELINE_IMPORT_FAILED: 'Import Pipeline Failed',
    PIPELINE_IMPORT_FAILED_FROM_ARCHIVE: 'Import Pipeline From Archive Failed',
    PIPELINE_IMPORT_FAILED_FROM_URL: 'Import Pipeline From URL Failed',
    PIPELINE_EXPORT: 'Pipeline Exported',
    PIPELINE_EXPORT_BULK: 'Pipelines Bulk Exported',
    PIPELINE_DELETE: 'Pipeline Deleted',
    PIPELINE_DELETE_BULK: 'Pipelines Bulk Deleted',
    HELP_CLICKED: 'Help Documentation Clicked',
    HISTORY_VIEWED: 'Pipeline History Viewed',
    TAB_SELECTED: 'Tab Selected',
    PIPELINE_SETUP_VIEW: 'Pipeline Setup View',
    RULE_CREATED: 'Rule Created',
    METRIC_RULE_CREATED: 'Metric Rule Created'
  })
  .factory('tracking', function() {
    var tracking = {
      FS: {},
      mixpanel: {
        people: {}
      }
    };

    // Fullstory
    tracking.FS.setUserVars = function() {
      if(typeof FS !== 'undefined') {
        return FS.setUserVars.apply(FS, arguments);
      } else {
        return null;
      }
    };
    tracking.FS.restart = function() {
      if(typeof FS !== 'undefined') {
        return FS.restart.apply(FS, arguments);
      } else {
        return null;
      }
    };
    tracking.FS.shutdown = function() {
      if(typeof FS !== 'undefined') {
        return FS.shutdown.apply(FS, arguments);
      } else {
        return null;
      }
    };
    tracking.FS.event = function() {
      if(typeof FS !== 'undefined') {
        return FS.event.apply(FS, arguments);
      } else {
        return null;
      }
    };

    // Mixpanel
    tracking.mixpanel.opt_in_tracking = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.opt_in_tracking.apply(mixpanel, arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.opt_out_tracking = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.opt_out_tracking.apply(mixpanel, arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.identify = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.identify.apply(mixpanel, arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.people.set = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.people.set.apply(mixpanel.people, arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.register = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.register.apply(mixpanel, arguments);
      } else {
        return null;
      }
    };
    tracking.mixpanel.track = function() {
      if(typeof mixpanel === 'object') {
        return mixpanel.track.apply(mixpanel, arguments);
      } else {
        return null;
      }
    };


    return tracking;
});
