package com.couchbase.sdktests;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static com.couchbase.client.java.kv.MutateInSpec.upsert;

import java.time.Duration;

import com.couchbase.client.core.Core;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.manager.search.AsyncSearchIndexManager;
import com.couchbase.client.java.manager.search.SearchIndex;
import com.couchbase.client.java.manager.search.SearchIndexManager;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchResult;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Arrays;

public class Run {

    public static void main(String... args) throws Exception {

        // Take flags for options:
        // connection string
        String connection = "";
        // username
        String username = "";
        // password
        String password = "";
        // bucket name
        String bucketName = "";
        // CA file (optional)
        String CAfile = "";

        for (String arg : args) {
            if (arg.startsWith("-connection=")) {
                connection = arg.replace("-connection=", "");
            }
            if (arg.startsWith("-username=")) {
                username = arg.replace("-username=", "");
            }
            if (arg.startsWith("-password=")) {
                password = arg.replace("-password=", "");
            }
            if (arg.startsWith("-bucket=")) {
                bucketName = arg.replace("-bucket=", "");
            }
            if (arg.startsWith("-cafile=")) {
                CAfile = arg.replace("-cafile=", "");
            }
        }

        if (connection == "") {
            System.err.println("-connection is a required argument");
            System.exit(1);
        }
        if (username == "") {
            System.err.println("-username is a required argument");
            System.exit(1);
        }
        if (password == "") {
            System.err.println("-password is a required argument");
            System.exit(1);
        }
        if (bucketName == "") {
            System.err.println("-bucket is a required argument");
            System.exit(1);
        }

        // set up cluster login with username/password
        ClusterOptions opt =  ClusterOptions.clusterOptions(username, password);
        // check if CA file is provided
        if (CAfile != "") {
            // read cert
            FileInputStream fis = new FileInputStream(CAfile);
            BufferedInputStream bis = new BufferedInputStream(fis);
           
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            while (bis.available() > 0) {
                Certificate cert = cf.generateCertificate(bis);
                System.out.println(cert.toString());
             }
             
            // add cert to cluster options
        }

        // try and connect to cluster
        Cluster cluster = Cluster.connect(connection, opt);
        cluster.waitUntilReady(Duration.ofSeconds(5));

        // try and connect to bucket
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(5));
        Collection collection = bucket.defaultCollection();

        // create index
        // cluster.query("CREATE PRIMARY INDEX ON ?", queryOptions().parameters(JsonArray.from(bucketName)));

        // upsert a doc inc. a subdoc
        JsonObject content = JsonObject.create().put("author", "mike").put("title", "My Blog Post 1");
        MutationResult mutationResult = collection.upsert("test-key", content);
        System.out.println("upsert done");

        // mutate a subdoc
        collection.mutateIn("test-key", Arrays.asList(upsert("author", "steve")));
        System.out.println("subdoc mutate done");

        // run a n1ql query
        cluster.query("SELECT *");
        System.out.println("n1ql query done");

        // run an analytics query
        AnalyticsResult analyticsResult = cluster.analyticsQuery("select \"hello\" as greeting");
        System.out.println("analytics query done");

        // run an fts search

        SearchResult searchResult = cluster.searchQuery("index", SearchQuery.queryString("test"));
        System.out.println("fts done");

        cluster.disconnect();

    }
}
