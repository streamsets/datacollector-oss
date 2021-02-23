/*
 * Copyright 2021 StreamSets Inc.
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
package com.streamsets.pipeline.lib.salesforce.connection;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestSalesforceConnectionUtils {
  SalesforceConnection connection = new SalesforceConnection();

  // Static test key defined for signing JWT.
  private static final String pk = "-----BEGIN PRIVATE KEY-----\n" +
      "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDHcTGC4HnXZ8z9\n" +
      "3rRSCHhjfrOSxEf/ZwrVJf73iQA/slpCsphabfhQUGuf64cdtqXaYUEAgfPi7PYs\n" +
      "sW3PoBxW/Mlmy94uQqsIoaj4J+103rK5O6S1+I1Ecr+OzNS5mGJAX1SoUwK7TEMh\n" +
      "gcCRRwdYlse2ccVaT1Vjnslgjl5Q8sFcsVFj3eu3DjI0mXKQedjtKmP/rUY32lD1\n" +
      "6vuCGMUKrVFooR+qU5nIhWBQegoaeFfvjfB1ErsrneXpeE2Fxicwg8YRiI46dizu\n" +
      "nneuPNeyrFD87AopaV3ba7vPdHSPUG6nO0z0WyRqVkPkETn29XRiyN705CzWqVwh\n" +
      "+4cQq5F9AgMBAAECggEANvgkzEywaEVMw5/xFTcOb2XJeqcsrOEqpCrPxkv1TJkM\n" +
      "tZth0HLE4OX5c99Ho1HnDnSFpO0sWhwzkYfpmzMm/Ha5z+Jav2dSmpNr7dGbaf3D\n" +
      "RriAokL6NKZn06Ty8KBmXBWqUKZod1UCn6aSe7gW9zy/mLJs0YJSJ53pJPq4VhjF\n" +
      "hO2GsR9S1F2BdFJ+Zimq5pIFwZWlgTzynbtSV1DqppeqfJDhxkyVhMn2RJw3Mbpu\n" +
      "mPGeg7DsIQgXWn2+So7lLakyDXqwy09/O7hZTlJUiehzGgcGKSD+Ux0VlU7fGosi\n" +
      "RWQhE90qZDy7XyLEd/r85EA74TZhdxsE5TCCfX+6IQKBgQDyOLK/q/AHf1E1y9m8\n" +
      "pCasjVzAZPBhVyOJtFjvkptlaAfQ2hPIQ30MPguOb7lI7nbKhMmc67CQuyTlQSOv\n" +
      "tk8EiIlA+wTnYfLyOochCvyjv80bRjo/UQEuDO5j2cod9wVn1lNTaHKZQoH+fcWG\n" +
      "0stnv6jElOo/ePKXnkMBz/80uQKBgQDSyYeqlQzb/OOGHN6RgR9M98cAbS5t/D61\n" +
      "wespu0KOxCkF4BicZDYJUbxuKlvR6Ro46iIV+wcJ1CyFi8HVbwSf1cPYkbwRrc5v\n" +
      "iTJSngpZVSxhLJ+BzCjfCQuKsSmlIje2Q/8OKvM2QYIakmJ+tMZyLj32f2hO4YbO\n" +
      "t0LHaQio5QKBgAp1pFJejFjmiI04JkjdFcZxvEWalj690o0JuqVtwUQZv+ym3h/R\n" +
      "uj6jF0CpVmjt0zdfkI00KEW3rxovO+lEiBj8BGFH9ahANIt9N4SXwt0XVTYOTEmb\n" +
      "p99jM5AgQXgVyKf5O1PouLohgxeIOtVdmOb8Ab+rZoojIOanMOGNJ8oRAoGAWUjk\n" +
      "Hm1kNQq5lWVFIX0ANSn/MT8OG6htJ7AsXFDlsHOGrOZvhk8sVGY62q82lYOXh+Qk\n" +
      "7AqYwKEO+sJoKHAOFWYGvwV8FED64GPM3RH0cEKTudWc+u3vognCycyhR0FnN901\n" +
      "fFrVCnZVFcxLzD/mjxbnSDJPjJoa8BTQRIdJE8ECgYBoJNRAf1CeBVGipVQngtYs\n" +
      "qRVCI4nKsxSmyxxzPT0EkJmyr4YRK/1TZq1i1HILImfrpDKHPevSjLxULh8Yv3Kd\n" +
      "97jd4KOaehqNprWNx1NxldTQhqfUyL/geft6NHxiKkgLq8D2WF6xz4aKlq1krTrq\n" +
      "RsePF52d2HMfiEykeRMDfg==\n" +
      "-----END PRIVATE KEY-----";

  @Before
  public void setUp() {
    connection.authEndpoint = "endpoint.mock.com";
    connection.username = () -> "user@email.com";
    connection.consumerKey = () -> "1234";
    connection.privateKey = () -> pk;
  }

  @Test
  public void testGetBearerToken() throws Exception {
    String token = connection.getBearerToken();
    assertNotNull(token);

    // Header of the JWT should be deterministic as per the spec.
    // The signature and payload vary by generation time and are not deterministic - but their length is always the same.
    String[] splitToken = token.split("\\.");
    assertEquals(splitToken.length, 3);
    assertEquals(splitToken[0], "eyJhbGciOiJSUzI1NiJ9");
    assertEquals(splitToken[1].length(), 140);
    assertEquals(splitToken[2].length(), 342);
  }

  @Test
  public void testNoPrivateKey() {
    connection.privateKey = () -> "no-key";
    Exception exception = new Exception();
    try {
      SalesforceJWTResponse salesforceJWTResponse = connection.getSessionConfig();
    } catch (Exception e) {
      exception = e;
    }
    assert exception instanceof JWTGenerationException;
    assertEquals(exception.getMessage(), "Problem generating JWT for Salesforce.");
  }

  @Test
  public void testBadSessionConfig() throws Exception {
    Exception exception = new Exception();
    try {
      SalesforceJWTResponse salesforceJWTResponse = connection.getSessionConfig();
    } catch (Exception e) {
      exception = e;
    }
    assert exception instanceof SessionIdFailureException;
    assertEquals(exception.getMessage(), "Failure to obtain Salesforce session ID.");
  }

}
