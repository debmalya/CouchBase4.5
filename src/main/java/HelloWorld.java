import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

/**
 * Copyright 2015-2016 Debmalya Jash
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author debmalyajash
 *
 */
public class HelloWorld {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 3) {
			System.err.println(
					"Usage: HelloWorld <hostname or ip> <bucket name> <your name> <your hobby1> <your hobby2>");
		} else {
			Cluster cluster = null;
			Bucket bucket = null;
			try {
				// Initialize the Connection
				cluster = CouchbaseCluster.create(args[0]);
				bucket = cluster.openBucket(args[1]);

				// Create a JSON Document
				JsonObject johnDoe = JsonObject.create().put("name", args[2]).put("email", "johndoe@example.com")
						.put("interests", JsonArray.from(args[3], args[4]));

				// Store the Document
				bucket.upsert(JsonDocument.create("u:john_doe", johnDoe));

				// Load the Document and print it
				// Prints Content and Metadata of the stored Document
				System.out.println(bucket.get("u:john_doe"));

				// Create a N1QL Primary Index (but ignore if it exists)
				bucket.bucketManager().createN1qlPrimaryIndex(true, false);

				// Perform a N1QL Query
				N1qlQueryResult result = bucket.query(N1qlQuery
						.parameterized("SELECT name FROM `" + args[1] +"` WHERE $1 IN interests", JsonArray.from(args[3])));
				printQueryResult(result);

//				result = bucket.query(N1qlQuery.simple("SELECT DISTINCT(country) FROM `travel-sample` WHERE type = 'airline' LIMIT 10"));
//				printQueryResult(result);
			} catch (Throwable th) {
				th.printStackTrace();
			}finally {
				if (bucket != null) {
					boolean result = bucket.close();
					if (result) {
						System.out.println("Bucket closed successfully");
					}
				}
				if (cluster != null) {
					boolean result = cluster.disconnect();
					if (result){
						System.out.println("Cluster disconnected successfully");
					}
				}
			}
		}

	}

	/**
	 * Print each result row
	 * @param result
	 */
	public static void printQueryResult(N1qlQueryResult result) {
		if (result.allRows().isEmpty()) {
			System.out.println("Nothing retrieved");
		} else {
			for (N1qlQueryRow row : result) {
			    System.out.println(row.value());
			}
		}
	}

}
