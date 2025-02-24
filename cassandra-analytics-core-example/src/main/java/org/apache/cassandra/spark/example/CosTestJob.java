/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.example;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.yaml.snakeyaml.Yaml;

/**
 * A sample cassandra spark job that writes directly to Cassandra via Sidecar,
 * then reads from Cassandra
 */
public class CosTestJob
{
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public static void main(String[] args)
    {
        System.setProperty("SKIP_STARTUP_VALIDATIONS", "true");
        new CosTestJob().start(args);
    }

    public void start(String[] args)
    {
        logger.info("Starting CoS test Spark job with args={}", Arrays.toString(args));

        String fileName = "cassandra-analytics.yaml"
        if (args.length > 0)
        {
            fileName = args[0];
        }
        File file = new file(fileName);

        FileInputStream input = new FileInputStream(file);
        Yaml yaml = new Yaml();
        Map<String, Object> config = yaml.load(input);

        SparkConf sparkConf = new SparkConf().setAppName("Sample Spark Cassandra Bulk Reader Job");
        if (config.getOrDefault("local", "remote").equals("local"))
        {
            sparkConf.set("spark.master", "local[8]");
        }

        BulkSparkConf.setupSparkConf(sparkConf, true);
        KryoRegister.setup(sparkConf);

        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate();
        SparkContext sc = spark.sparkContext();
        SQLContext sql = spark.sqlContext();
        logger.info("Spark Conf: " + sparkConf.toDebugString());

        int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
        int numExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors",
                sparkConf.getInt("spark.executor.instances", 1));
        int numCores = coresPerExecutor * numExecutors;
        Map<String, String> readerOptions = new HashMap<>();
        readerOptions.put("sidecar_contact_points", "10.218.164.91,10.218.164.184,10.218.164.243");
        readerOptions.put("keyspace", config.getOrDefault("keyspace", "cos_primary_shard_va6_dev_01");
        readerOptions.put("table", config.getOrDefault("table", "components"));
        readerOptions.put("DC", "us-east-1");
        readerOptions.put("snapshotName", UUID.randomUUID().toString());
        readerOptions.put("createSnapshot", "true");
        readerOptions.put("defaultParallelism", String.valueOf(sc.defaultParallelism()));
        readerOptions.put("numCores", String.valueOf(numCores));
        readerOptions.put("sizing", "default");

        try
        {
            DataFrameReader reader = sql.read().format("org.apache.cassandra.spark.sparksql.CassandraDataSource");
            reader.options(readerOptions);
            Dataset<Row> df = reader.load();
            if (config.getOrDefault("operation", "count").equals("count"))
            {
                long count = df.count();
                logger.info("Found {} records", count);
                System.out.println("Found " + count + " records in " + readerOptions.get("table"));
            } else
            {
                logger.info("Export {} .....", readerOptions.get("table"));
                String csvLocation = config.getOrDefault("location", "/var/aws/") + readerOptions.get("table") + ".csv";
                df.write().option("compression", "gzip").csv(csvLocation);
//                 .save("s3a://dcx-cassandra-db-backup-va6c2-dev/export")
//                 https://spark.apache.org/docs/3.5.3/cloud-integration.html
            }
            logger.info("Finished Spark job, shutting down...");
            sc.stop();
        }
        catch (Throwable throwable)
        {
            logger.error("Unexpected exception executing Spark job", throwable);
            try
            {
                sc.stop();
            }
            catch (Throwable ignored)
            {
            }
        }

    }
}
