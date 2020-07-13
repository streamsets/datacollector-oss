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
package com.streamsets.datacollector.usagestats;

import com.streamsets.datacollector.activation.Activation;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Contains everything but user info from {@link com.streamsets.datacollector.activation.Activation.Info}
 */
public class ActivationInfo {
    String activationType;
    String sdcId;
    boolean isValid;
    long firstUseOn;
    long expiration;
    List<String> validSdcIds;
    Map<String, Object> additionalInfo;

    /** For deserialization only */
    @Deprecated
    public ActivationInfo() {}

    public ActivationInfo(Activation activation) {
        if (activation == null) {
            return;
        }
        Activation.Info info = activation.getInfo();
        if (info == null) {
            return;
        }
        this.activationType = info.getType();
        this.sdcId = info.getSdcId();
        this.isValid = info.isValid();
        this.firstUseOn = info.getFirstUse();
        this.expiration = info.getExpiration();
        this.validSdcIds = info.getValidSdcIds();
        this.additionalInfo = null == info.getAdditionalInfo() ? null : info.getAdditionalInfo().entrySet().stream()
                .filter(e -> !e.getKey().startsWith("user"))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public String getActivationType() {
        return activationType;
    }

    public ActivationInfo setActivationType(String activationType) {
        this.activationType = activationType;
        return this;
    }

    public String getSdcId() {
        return sdcId;
    }

    public ActivationInfo setSdcId(String sdcId) {
        this.sdcId = sdcId;
        return this;
    }

    public boolean isValid() {
        return isValid;
    }

    public ActivationInfo setValid(boolean valid) {
        isValid = valid;
        return this;
    }

    public long getFirstUseOn() {
        return firstUseOn;
    }

    public ActivationInfo setFirstUseOn(long firstUseOn) {
        this.firstUseOn = firstUseOn;
        return this;
    }

    public long getExpiration() {
        return expiration;
    }

    public ActivationInfo setExpiration(long expiration) {
        this.expiration = expiration;
        return this;
    }

    public List<String> getValidSdcIds() {
        return validSdcIds;
    }

    public ActivationInfo setValidSdcIds(List<String> validSdcIds) {
        this.validSdcIds = validSdcIds;
        return this;
    }

    public Map<String, Object> getAdditionalInfo() {
        return additionalInfo;
    }

    public ActivationInfo setAdditionalInfo(Map<String, Object> additionalInfo) {
        this.additionalInfo = additionalInfo;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ActivationInfo that = (ActivationInfo) o;
        return isValid == that.isValid &&
                firstUseOn == that.firstUseOn &&
                expiration == that.expiration &&
                Objects.equals(activationType, that.activationType) &&
                Objects.equals(sdcId, that.sdcId) &&
                Objects.equals(validSdcIds, that.validSdcIds) &&
                Objects.equals(additionalInfo, that.additionalInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(activationType, sdcId, isValid, firstUseOn, expiration, validSdcIds, additionalInfo);
    }
}
