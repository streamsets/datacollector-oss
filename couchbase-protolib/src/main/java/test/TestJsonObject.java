/*
 * Copyright 2018 StreamSets Inc.
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
package test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;

public class TestJsonObject {
    
    String jsonString = "{\n" +
"  \"medallion\": \"002B4CFC5B8920A87065FC131F9732D1\",\n" +
"  \"surcharge\": \"0.5\",\n" +
"  \"fare_amount\": \"13.5\",\n" +
"  \"pickup_longitude\": \"-73.969421\",\n" +
"  \"pickup_datetime\": \"2013-01-13 04:35asdf\",\n" +
"  \"passenger_count\": \"1\",\n" +
"  \"tolls_amount\": \"0\",\n" +
"  \"trip_time_in_secs\": \"660\",\n" +
"  \"dropoff_latitude\": \"40.769043\",\n" +
"  \"rate_code\": \"1\",\n" +
"  \"trip_distance\": \"3.9\",\n" +
"  \"payment_type\": \"CSH\",\n" +
"  \"dropoff_longitude\": \"-73.925873\",\n" +
"  \"credit_card\": \"\",\n" +
"  \"credit_card_type\": \"n/a\",\n" +
"  \"total_amount\": \"14.5\",\n" +
"  \"pickup_latitude\": \"40.763699\",\n" +
"  \"hack_license\": \"DD63AD6504C4A40A301AEE0C5CE41C28\",\n" +
"  \"vendor_id\": \"VTS\",\n" +
"  \"tip_amount\": \"0\",\n" +
"  \"mta_tax\": \"0.5\",\n" +
"  \"dropoff_datetime\": \"2013-01-13 04:46asdf\"\n" +
"}";
    
    public static void main(String[] argu) {
        
        new TestJsonObject();
        
    }
    
    public TestJsonObject() {
        test();
    }
    
    public void test() {
        JsonObject json = JsonObject.fromJson(jsonString);
        processJsonObject(json);
        
    }
    
    private void processJsonObject(JsonObject jsonObject) {
        /*
        //Get Field Names from JSON
        Set<String> fieldNames = jsonObject.getNames();
        Iterator<String> fieldNamesInt = fieldNames.iterator();
        
        //Loop throught and get names
        while (fieldNamesInt.hasNext()) {
            String fieldName = fieldNamesInt.next();
            String fieldValue = jsonObject.getString(fieldName);
            System.out.println(fieldName + " - " + fieldValue);
        }*/
        
                //Connect to Couchbase Cluster
        Cluster cluster = CouchbaseCluster.create();
        ClusterManager clusterManager = cluster.clusterManager("Administrator", "couchbase");
        
        //Now lets open the bucket
        Bucket bucket = cluster.openBucket("CreditCards");
        
         // Perform a N1QL Query
        N1qlQueryResult result = bucket.query(N1qlQuery.simple("SELECT * FROM CreditCards"));
        
        System.out.println(result.allRows().size());
        
        
        
        
        
        
    }
    
    
   
    
}
