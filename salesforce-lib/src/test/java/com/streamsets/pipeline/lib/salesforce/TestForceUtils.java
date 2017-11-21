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
package com.streamsets.pipeline.lib.salesforce;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestForceUtils {
  private static final String[][] queries = {
      // All of the valid queries specified in the Salesforce SOQL Reference
      // https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql.htm
      {"SELECT AccountId\n" +
          "FROM Event\n" +
          "WHERE ActivityDate != null", "Event"},
      {"SELECT Id\n" +
          "FROM Case\n" +
          "WHERE Contact.LastName = null", "Case"},
      {"SELECT Company, toLabel(Recordtype.Name) FROM Lead", "Lead"},
      {"SELECT Company, toLabel(Status)\n" +
          "FROM Lead\n" +
          "WHERE toLabel(Status) = 'le Draft'", "Lead"},
      {"SELECT Id, MSP1__c FROM CustObj__c WHERE MSP1__c = 'AAA;BBB'", "CustObj__c"},
      {"SELECT Id, MSP1__c from CustObj__c WHERE MSP1__c includes ('AAA;BBB','CCC')", "CustObj__c"},
      {"SELECT Id\n" +
          "FROM Event\n" +
          "WHERE What.Type IN ('Account', 'Opportunity')", "Event"},
      {"SELECT AccountId, FirstName, lastname\n" +
          "FROM Contact\n" +
          "WHERE lastname LIKE 'appl%'", "Contact"},
      {"SELECT Name FROM Account\n" +
          "WHERE BillingState IN ('California', 'New York')", "Account"},
      {"SELECT Name FROM Account\n" +
          "WHERE BillingState NOT IN ('California', 'New York')", "Account"},
      {"SELECT Id, Name \n" +
          "FROM Account \n" +
          "WHERE Id IN \n" +
          "  ( SELECT AccountId\n" +
          "    FROM Opportunity\n" +
          "    WHERE StageName = 'Closed Lost' \n" +
          "  )", "Account"},
      {"SELECT Id\n" +
          "FROM Task \n" +
          "WHERE WhoId IN \n" +
          "  (\n" +
          "    SELECT Id\n" +
          "    FROM Contact\n" +
          "    WHERE MailingCity = 'Twin Falls'\n" +
          "  )", "Task"},
      {"SELECT Id \n" +
          "FROM Account \n" +
          "WHERE Id NOT IN\n" +
          "  (\n" +
          "    SELECT AccountId\n" +
          "    FROM Opportunity\n" +
          "    WHERE IsClosed = false\n" +
          "  )", "Account"},
      {"SELECT Id\n" +
          "FROM Opportunity\n" +
          "WHERE AccountId NOT IN \n" +
          "  (\n" +
          "    SELECT AccountId\n" +
          "    FROM Contact\n" +
          "    WHERE LeadSource = 'Web'\n" +
          "  )", "Opportunity"},
      {"SELECT Id, Name \n" +
          "FROM Account\n" +
          "WHERE Id IN\n" +
          "  (\n" +
          "    SELECT AccountId\n" +
          "    FROM Contact\n" +
          "    WHERE LastName LIKE 'apple%'\n" +
          "  )\n" +
          "  AND Id IN\n" +
          "  (\n" +
          "    SELECT AccountId\n" +
          "    FROM Opportunity\n" +
          "    WHERE isClosed = false\n" +
          "  )", "Account"},
      {"SELECT Id, (SELECT Id from OpportunityLineItems) \n" +
          "FROM Opportunity \n" +
          "WHERE Id IN\n" +
          "  (\n" +
          "    SELECT OpportunityId\n" +
          "    FROM OpportunityLineItem\n" +
          "    WHERE totalPrice > 10000\n" +
          "  )", "Opportunity"},
      {"SELECT Id \n" +
          " FROM Idea \n" +
          " WHERE (Id IN (SELECT ParentId FROM Vote WHERE CreatedDate > LAST_WEEK AND Parent.Type='Idea'))", "Idea"},
      {"SELECT Id, Name\n" +
          "FROM Account\n" +
          "WHERE Id IN\n" +
          "  (\n" +
          "    SELECT AccountId\n" +
          "    FROM Contact\n" +
          "    WHERE LastName LIKE 'Brown_%'\n" +
          "  )", "Account"},
      {"SELECT Id, Name\n" +
          "FROM Account\n" +
          "WHERE Parent.Name = 'myaccount'", "Account"},
      {"SELECT Id \n" +
          " FROM Idea \n" +
          " WHERE (Idea.Title LIKE 'Vacation%') \n" +
          "AND (Idea.LastCommentDate > YESTERDAY) \n" +
          "AND (Id IN (SELECT ParentId FROM Vote\n" +
          "            WHERE CreatedById = '005x0000000sMgYAAU'\n" +
          "             AND Parent.Type='Idea'))", "Idea"},
      {"SELECT Id FROM Contact WHERE LastName = 'foo' or Account.Name = 'bar'", "Contact"},
      {"SELECT Id\n" +
          "FROM Account\n" +
          "WHERE CreatedDate > 2005-10-08T01:02:03Z", "Account"},
      {"SELECT Name\n" +
          "FROM Account\n" +
          "ORDER BY Name DESC NULLS LAST", "Account"},
      {"SELECT Id, CaseNumber, Account.Id, Account.Name\n" +
          "FROM Case\n" +
          "ORDER BY Account.Name", "Case"},
      {"SELECT Name\n" +
          "FROM Account\n" +
          "WHERE industry = 'media'\n" +
          "ORDER BY BillingPostalCode ASC NULLS LAST LIMIT 125", "Account"},
      {"SELECT Name\n" +
          "FROM Account\n" +
          "WHERE Industry = 'Media' LIMIT 125", "Account"},
      {"SELECT Name\n" +
          "FROM Merchandise__c\n" +
          "WHERE Price__c > 5.0\n" +
          "ORDER BY Name\n" +
          "LIMIT 100\n" +
          "OFFSET 10", "Merchandise__c"},
      {"SELECT Name, Id\n" +
          "    (\n" +
          "        SELECT Name FROM Opportunity LIMIT 10 OFFSET 2\n" +
          "    )\n" +
          "FROM Account\n" +
          "ORDER BY Name\n" +
          "LIMIT 1", "Account"},
      {"SELECT Name, Id\n" +
          "FROM Merchandise__c\n" +
          "ORDER BY Name\n" +
          "LIMIT 100\n" +
          "OFFSET 0", "Merchandise__c"},
      {"SELECT Name, Id\n" +
          "FROM Merchandise__c\n" +
          "ORDER BY Name\n" +
          "LIMIT 100\n" +
          "OFFSET 100", "Merchandise__c"},
      {"SELECT Name\n" +
          "FROM Merchandise__c\n" +
          "ORDER BY Name\n" +
          "OFFSET 10", "Merchandise__c"},
      {"SELECT Title FROM FAQ__kav\n" +
          "WHERE Keyword='Apex' and\n" +
          "Language = 'en_US' and\n" +
          "KnowledgeArticleVersion = 'ka230000000PCiy'\n" +
          "UPDATE TRACKING", "FAQ__kav"},
      {"SELECT Title FROM FAQ__kav\n" +
          "   WHERE PublishStatus='online' and\n" +
          "   Language = 'en_US' and\n" +
          "   KnowledgeArticleVersion = 'ka230000000PCiy'\n" +
          "   UPDATE VIEWSTAT", "FAQ__kav"},
      {"SELECT Title FROM KnowledgeArticleVersion WHERE PublishStatus='online' WITH DATA CATEGORY Geography__c ABOVE usa__c", "KnowledgeArticleVersion"},
      {"SELECT Id FROM UserProfileFeed WITH UserId='005D0000001AamR' ORDER BY CreatedDate DESC, Id DESC LIMIT 20", "UserProfileFeed"},
      {"SELECT Title FROM Question WHERE LastReplyDate > 2005-10-08T01:02:03Z WITH DATA CATEGORY Geography__c AT (usa__c, uk__c)", "Question"},
      {"SELECT UrlName FROM KnowledgeArticleVersion WHERE PublishStatus='draft' WITH DATA CATEGORY Geography__c AT usa__c AND Product__c ABOVE_OR_BELOW mobile_phones__c", "KnowledgeArticleVersion"},
      {"SELECT Title FROM Question WHERE LastReplyDate < 2005-10-08T01:02:03Z WITH DATA CATEGORY Product__c AT mobile_phones__c", "Question"},
      {"SELECT Title, Summary FROM KnowledgeArticleVersion WHERE PublishStatus='Online' AND Language = 'en_US' WITH DATA CATEGORY Geography__c ABOVE_OR_BELOW europe__c AND Product__c BELOW All__c", "KnowledgeArticleVersion"},
      {"SELECT Id, Title FROM Offer__kav WHERE PublishStatus='Draft' AND Language = 'en_US' WITH DATA CATEGORY Geography__c AT (france__c,usa__c) AND Product__c ABOVE dsl__c", "Offer__kav"},
      {"SELECT LeadSource FROM Lead", "Lead"},
      {"SELECT LeadSource, COUNT(Name)\n" +
          "FROM Lead\n" +
          "GROUP BY LeadSource", "Lead"},
      {"SELECT LeadSource\n" +
          "FROM Lead\n" +
          "GROUP BY LeadSource", "Lead"},
      {"SELECT Name, Max(CreatedDate)\n" +
          "FROM Account\n" +
          "GROUP BY Name\n" +
          "LIMIT 5", "Account"},
      {"SELECT Name n, MAX(Amount) max\n" +
          "FROM Opportunity\n" +
          "GROUP BY Name", "Opportunity"},
      {"SELECT Name, MAX(Amount), MIN(Amount)\n" +
          "FROM Opportunity\n" +
          "GROUP BY Name", "Opportunity"},
      {"SELECT Name, MAX(Amount), MIN(Amount) min, SUM(Amount)\n" +
          "FROM Opportunity\n" +
          "GROUP BY Name", "Opportunity"},
      {"SELECT LeadSource, COUNT(Name) cnt\n" +
          "FROM Lead\n" +
          "GROUP BY ROLLUP(LeadSource)", "Lead"},
      {"SELECT Status, LeadSource, COUNT(Name) cnt\n" +
          "FROM Lead\n" +
          "GROUP BY ROLLUP(Status, LeadSource)", "Lead"},
      {"SELECT LeadSource, Rating,\n" +
          "    GROUPING(LeadSource) grpLS, GROUPING(Rating) grpRating,\n" +
          "    COUNT(Name) cnt\n" +
          "FROM Lead\n" +
          "GROUP BY ROLLUP(LeadSource, Rating)", "Lead"},
      {"SELECT Type, BillingCountry,\n" +
          "    GROUPING(Type) grpType, GROUPING(BillingCountry) grpCty,\n" +
          "    COUNT(id) accts\n" +
          "FROM Account\n" +
          "GROUP BY CUBE(Type, BillingCountry)\n" +
          "ORDER BY GROUPING(Type), GROUPING(BillingCountry)", "Account"},
      {"SELECT LeadSource, COUNT(Name)\n" +
          "FROM Lead\n" +
          "GROUP BY LeadSource\n" +
          "HAVING COUNT(Name) > 100", "Lead"},
      {"SELECT Name, Count(Id)\n" +
          "FROM Account\n" +
          "GROUP BY Name\n" +
          "HAVING Count(Id) > 1", "Account"},
      {"SELECT LeadSource, COUNT(Name)\n" +
          "FROM Lead\n" +
          "GROUP BY LeadSource\n" +
          "HAVING COUNT(Name) > 100 and LeadSource > 'Phone'", "Lead"},
      {"SELECT \n" +
          "    TYPEOF What\n" +
          "        WHEN Account THEN Phone\n" +
          "        ELSE Name\n" +
          "    END\n" +
          "FROM Event\n" +
          "WHERE CreatedById IN\n" +
          "    (\n" +
          "    SELECT CreatedById\n" +
          "    FROM Case\n" +
          "    )", "Event"},
      {"SELECT \n" +
          "  TYPEOF What\n" +
          "    WHEN Account THEN Phone, NumberOfEmployees\n" +
          "    WHEN Opportunity THEN Amount, CloseDate\n" +
          "    ELSE Name, Email\n" +
          "  END\n" +
          "FROM Event", "Event"},
      {"SELECT FORMAT(amount) Amt, format(lastModifiedDate) editDate FROM Opportunity", "Opportunity"},
      {"SELECT Id, LastModifiedDate, FORMAT(LastModifiedDate) formattedDate FROM Account", "Account"},
      {"SELECT amount, FORMAT(amount) Amt, convertCurrency(amount) editDate, FORMAT(convertCurrency(amount)) convertedCurrency FROM Opportunity where id = '12345'", "Opportunity"},
      {"SELECT FORMAT(MIN(closedate)) Amt FROM opportunity", "opportunity"},
      {"SELECT Name, ID FROM Contact  LIMIT 1 FOR VIEW", "Contact"},
      {"SELECT Name, ID FROM Contact  LIMIT 1 FOR REFERENCE", "Contact"},
      {"SELECT Id FROM Account LIMIT 2 FOR UPDATE", "Account"},
      {"SELECT AVG(Amount)\n" +
          "FROM Opportunity", "Opportunity"},
      {"SELECT CampaignId, AVG(Amount)\n" +
          "FROM Opportunity\n" +
          "GROUP BY CampaignId", "Opportunity"},
      {"SELECT COUNT()\n" +
          "FROM Account\n" +
          "WHERE Name LIKE 'a%'", "Account"},
      {"SELECT COUNT(Id)\n" +
          "FROM Account\n" +
          "WHERE Name LIKE 'a%'", "Account"},
      {"SELECT COUNT_DISTINCT(Company)\n" +
          "FROM Lead\n", "Lead"},
      {"SELECT MIN(CreatedDate), FirstName, LastName\n" +
          "FROM Contact\n" +
          "GROUP BY FirstName, LastName", "Contact"},
      {"SELECT Name, MAX(BudgetedCost)\n" +
          "FROM Campaign\n" +
          "GROUP BY Name", "Campaign"},
      {"SELECT SUM(Amount)\n" +
          "FROM Opportunity\n" +
          "WHERE IsClosed = false AND Probability > 60", "Opportunity"},
      {"SELECT COUNT()\n" +
          "FROM Contact, Contact.Account\n" +
          "WHERE Account.Name = 'MyriadPubs'", "Contact,Contact.Account"},
      {"SELECT COUNT(Id), COUNT(CampaignId)\n" +
          "FROM Opportunity", "Opportunity"},
      {"SELECT LeadSource, COUNT(Name)\n" +
          "FROM Lead\n" +
          "GROUP BY LeadSource", "Lead"},
      {"SELECT CALENDAR_YEAR(CreatedDate), SUM(Amount)\n" +
          "FROM Opportunity\n" +
          "GROUP BY CALENDAR_YEAR(CreatedDate)", "Opportunity"},
      {"SELECT CreatedDate, Amount\n" +
          "FROM Opportunity\n" +
          "WHERE CALENDAR_YEAR(CreatedDate) = 2009", "Opportunity"},
      {"SELECT CALENDAR_YEAR(CloseDate)\n" +
          "FROM Opportunity\n" +
          "GROUP BY CALENDAR_YEAR(CloseDate)", "Opportunity"},
      {"SELECT HOUR_IN_DAY(convertTimezone(CreatedDate)), SUM(Amount)\n" +
          "FROM Opportunity\n" +
          "GROUP BY HOUR_IN_DAY(convertTimezone(CreatedDate))", "Opportunity"},
      {"SELECT Id, convertCurrency(AnnualRevenue)\n" +
          "FROM Account", "Account"},
      {"SELECT Amount, FORMAT(amount) Amt, convertCurrency(amount) convertedAmount,\n" +
          "FORMAT(convertCurrency(amount)) convertedCurrency FROM Opportunity where id = '006R00000024gDtIAI'", "Opportunity"},
      {"SELECT Id, Name\n" +
          "FROM Opportunity\n" +
          "WHERE Amount > USD5000", "Opportunity"},
      {"SELECT Name, MAX(Amount)\n" +
          "FROM Opportunity\n" +
          "GROUP BY Name\n" +
          "HAVING MAX(Amount) > 10000", "Opportunity"},
      // SDC allows SELECT *
      {"SELECT *\n" +
          "FROM Account\n" +
          "WHERE id = '0013600000EVlZVAA1'", "Account"},
      // SDC-7548 Salesforce Lookup Processor fails when record ID includes 'FROM'
      {"SELECT some, fields FROM Lead WHERE Id = '00QC000001VZfROMA1'", "Lead"},
      // SDC-7894 SOQL problem using DateTime as ${OFFSET} value
      {"SELECT * FROM Task WHERE LastModifiedDate > ${OFFSET}", "Task"},
      {"SELECT * FROM Task WHERE Id > '${OFFSET}'", "Task"},
  };

  @Parameterized.Parameters
  public static Collection<Object[]> data() throws Exception {
    return Arrays.asList(queries);
  }

  private final String soql;
  private final String expected;

  public TestForceUtils(String sql, String expected) {
    this.soql = sql;
    // getSobjectTypeFromQuery normalizes sobject names to lower case
    this.expected = expected.toLowerCase();
  }

  @Test
  public void testGetSobjectTypeFromQuery() {
    try {
      Assert.assertEquals(
          "Query is \"" + soql + "\"",
          expected,
          ForceUtils.getSobjectTypeFromQuery(soql)
      );
    } catch (Exception e) {
      Assert.fail(e.toString() + " parsing \"" + soql + "\"");
    }
  }
}
