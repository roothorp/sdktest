package com.couchbase.sdktests;

import com.couchbase.client.core.env.SecurityConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.analytics.AnalyticsResult;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;

import java.io.FileInputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.couchbase.client.java.kv.MutateInSpec.upsert;

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

        if (connection.equals("")) {
            System.err.println("-connection is a required argument");
            System.exit(1);
        }
        if (username.equals("")) {
            System.err.println("-username is a required argument");
            System.exit(1);
        }
        if (password.equals("")) {
            System.err.println("-password is a required argument");
            System.exit(1);
        }
        if (bucketName.equals("")) {
            System.err.println("-bucket is a required argument");
            System.exit(1);
        }

        // set up cluster login with username/password
        ClusterEnvironment env;
        // check if CA file is provided
        if (!CAfile.equals("")) {
            // add cert to cluster options
            List<X509Certificate> certs = new ArrayList<>();
            FileInputStream fis = new FileInputStream(CAfile);
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            while (fis.available() > 0) {
                X509Certificate cert = (X509Certificate) cf.generateCertificate(fis);
                certs.add(cert);
            }
            fis.close();
            env = ClusterEnvironment.builder().securityConfig(SecurityConfig.enableTls(true).trustCertificates(certs)).build();
        } else {
            env = ClusterEnvironment.builder().build();
        }

        // try and connect to cluster

        Cluster cluster = Cluster.connect(connection, ClusterOptions.clusterOptions(username, password).environment(env));
        cluster.waitUntilReady(Duration.ofSeconds(5));

        // try and connect to bucket
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(5));
        Collection collection = bucket.defaultCollection();

        // upsert a doc
        JsonObject content = JsonObject.create().put("author", "mike").put("title", "My Blog Post 1");
        MutationResult mutationResult = collection.upsert("test-key", content);
        System.out.println("upsert done");

        // subdoc mutate
        collection.mutateIn("test-key", Arrays.asList(upsert("author", "steve")));
        System.out.println("subdoc mutate done");

        // run a n1ql query
        cluster.query("SELECT *");
        System.out.println("n1ql query done");

        // run an analytics query
        AnalyticsResult analyticsResult = cluster.analyticsQuery("select \"hello\" as greeting");
        System.out.println("analytics query done");

        // run an fts search (TODO: needs index)
        //SearchResult searchResult = cluster.searchQuery("index", SearchQuery.queryString("test"));
        //System.out.println("fts done");

        cluster.disconnect();
        env.shutdown();

    }
}
