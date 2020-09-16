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
    METRIC_RULE_CREATED: 'Metric Rule Created',
    SAMPLE_PIPELINE_VIEW: 'Sample Pipeline View',
    SAMPLE_PIPELINE_DUPLICATED: 'Sample Pipeline Duplicated'
  })
  .factory('tracking', function($timeout, Analytics) {
    var tracking = {
      FS: {},
      mixpanel: {
        people: {}
      }
    };

    /**
     * This replaces the mixpanel "register" for properties where the
     * same user could have two engines open in different tabs
     * and have different properties
     */
    var localRegistry = {};

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
    tracking.mixpanel.track = function(track_event, properties) {
      if(typeof mixpanel === 'object') {
        properties = properties || {};
        properties = Object.assign(properties, localRegistry);
        return mixpanel.track(track_event, properties);
      } else {
        return null;
      }
    };

    /**
     * Set up user identity in all tracking services
     * @param {string} sdcId
     * @param {string} userName will be an email if using Aster, otherwise probably 'admin'
     * @param {string} activationInfo only available if activation enabled
     */
    tracking.initializeUser = function(sdcId, userName, activationInfo) {
      var orgId;
      if (activationInfo && activationInfo.info && activationInfo.info.userInfo) {
        var userInfo = activationInfo.info.userInfo;
        if (userInfo.startsWith && userInfo.startsWith('aster|')) {
          orgId = userInfo.split('|')[1];
        }
      }
      var userId;
      if (orgId) {
        // Matches Aster user tracking id format
        userId = userName + '::' + orgId;
      } else {
        userId = sdcId + userName;
      }
      tracking.mixpanel.identify(userId);
      tracking.mixpanel.people.set({
        userName: userName,
        UserID: userId,
        sdcId: sdcId,
        productName: 'Data Collector',
      });
      if (orgId) {
        // This is needed in case users activate with Aster but use form login
        tracking.mixpanel.people.set({
          organizationId: orgId
        });
      }
      localRegistry.userName = userName;
      localRegistry.sdcId = sdcId;
      localRegistry.productName = 'Data Collector';
      tracking.FS.setUserVars({
        userName: userName,
        UserID: userId,
        sdcId: sdcId,
      });
    };

    /**
     * Track extra user info that requires API calls
     */
    tracking.trackExtraUserInfo = function(buildResult, statsResult) {
      if (buildResult && buildResult.data) {
        localRegistry.sdcVersion = buildResult.data.version;
        tracking.FS.setUserVars({ 'sdcVersion': buildResult.data.version });
        tracking.mixpanel.people.set({ 'sdcVersion': buildResult.data.version });
        $timeout(function () {
          Analytics.set('dimension1', buildResult.data.version); // dimension1 is sdcVersion
        }, 1000);
      }
      if (statsResult.data.active &&
        statsResult.data.stats &&
        statsResult.data.stats.activeStats &&
        statsResult.data.stats.activeStats.extraInfo) {
        var stats = statsResult.data.stats;
        if (stats.activeStats.extraInfo) {
          if (stats.activeStats.extraInfo.cloudProvider) {
            localRegistry.cloudProvider = stats.activeStats.extraInfo.cloudProvider;
            tracking.FS.setUserVars({ 'cloudProvider': stats.activeStats.extraInfo.cloudProvider });
            tracking.mixpanel.people.set({ 'cloudProvider': stats.activeStats.extraInfo.cloudProvider });
            $timeout(function () {
              Analytics.set(
                'dimension2',
                stats.activeStats.extraInfo.cloudProvider
              ); // dimension2 is cloudProvider
            }, 1000);
          }
          if (stats.activeStats.extraInfo.distributionChannel) {
            localRegistry.distributionChannel = stats.activeStats.extraInfo.distributionChannel;
            tracking.FS.setUserVars({ 'distributionChannel': stats.activeStats.extraInfo.distributionChannel });
            tracking.mixpanel.people.set({ 'distributionChannel': stats.activeStats.extraInfo.distributionChannel });
            $timeout(function () {
              Analytics.set(
                "dimension3",
                stats.activeStats.extraInfo.distributionChannel
              ); // dimension3 is distributionChannel
            }, 1000);
          }
        }
      }
    };

    return tracking;
});
