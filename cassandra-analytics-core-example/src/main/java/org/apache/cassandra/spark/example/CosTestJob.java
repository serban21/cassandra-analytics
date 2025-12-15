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
import java.util.List;
import java.util.stream.Collectors;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.Instant;
//import java.io.InputStream;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.TTLOption;
import org.apache.cassandra.spark.bulkwriter.TimestampOption;
import org.apache.cassandra.spark.bulkwriter.WriterOptions;
import org.apache.spark.sql.*;
//import org.apache.spark.sql.types.StructType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


import static org.apache.spark.sql.functions.*;


public class CosTestJob {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private Map<String, String> readerOptions = new HashMap<>();
    private Map<String, String> writerOptions = new HashMap<>();

//    private static String import_source = "s3a://aam-s2s-traits-stage-us-east-1/2025.12.11-00.00.02/profilemerge_spp_users_optout/region=7/";

    private static JobConfig config;

    public static void main(String[] args) {
        System.setProperty("SKIP_STARTUP_VALIDATIONS", "true");
        initConfig(args);
        new CosTestJob().start(args);
    }

    public static void initConfig(String[] args) {
        String fileName = "cassandra-analytics.yaml";
        if (args.length > 0) {
            fileName = args[0];
        }
        File file = new File(fileName);
        FileInputStream input;
        try {
            input = new FileInputStream(file);
            Yaml yaml = new Yaml();
            if (config == null) {
                config = yaml.loadAs(input, JobConfig.class);
            }
        } catch (FileNotFoundException e) {
//            logger.error("file not found: " + fileName, e);
        }
    }

    public void start(String[] args) {
        logger.info("Starting CoS test Spark job with args={}", Arrays.toString(args));

        SparkConf sparkConf = new SparkConf().setAppName("Cassandra-Spark export");
        if (config.getLocal()) {
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
        logger.info("Job config: " + config.toString());
        logger.info("Spark Conf: " + sparkConf.toDebugString());

        int coresPerExecutor = sparkConf.getInt("spark.executor.cores", 1);
        int numExecutors = sparkConf.getInt("spark.dynamicAllocation.maxExecutors",
                sparkConf.getInt("spark.executor.instances", 1));
        int numCores = coresPerExecutor * numExecutors;
        initOptions(config, sc, numCores);

        try {
            String location = "s3a://" + config.getBucket() + "/" + config.getPrefix();
            String dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmm"));
            for (Map<String, String> step : config.getSteps()) {
                step.putIfAbsent("keyspace", config.getKeyspace());
                for (Map<String, String> job : config.getJobs()) {
                    if (job.get("name").equals(step.get("job_name"))) {
                        step.putIfAbsent("table", job.get("table"));
                        step.putIfAbsent("format", job.get("format"));
                        step.putIfAbsent("operation", job.get("operation"));
                        step.putIfAbsent("columns", job.getOrDefault("columns", "*"));
                        break;
                    }
                }
                executeJob(sql, step, location, dateTime);
            }

            logger.info("Finished all Spark jobs, shutting down...");
            sc.stop();
        } catch (Throwable throwable) {
            logger.error("Unexpected exception executing Spark job", throwable);
            try {
                sc.stop();
            } catch (Throwable ignored) {
            }
        }

    }

    private void initOptions(JobConfig config, SparkContext sc, int numCores) {
        readerOptions.put("sidecar_contact_points", config.getSidecarContactPoints());
        writerOptions.put("sidecar_contact_points", config.getSidecarContactPoints());

        readerOptions.put("DC", config.getDc());
        writerOptions.put("DC", config.getDc());

        String snapshotName = UUID.randomUUID().toString();
        readerOptions.put("snapshotName", snapshotName);
        readerOptions.put("createSnapshot", "true");

        readerOptions.put("defaultParallelism", String.valueOf(sc.defaultParallelism()));
        writerOptions.put("defaultParallelism", String.valueOf(sc.defaultParallelism()));

        readerOptions.put("numCores", String.valueOf(numCores));
        writerOptions.put("numCores", String.valueOf(numCores));

        readerOptions.put("sizing", "default");
        writerOptions.put("sizing", "default");

        writerOptions.put("bulk_writer_cl", "ALL");
        writerOptions.put("number_splits", "-1"); // what is this?
    }

    private void executeWriteJob(SQLContext sql, String table, String location) {
        // TODO The timestamp is sent ONLY through the SQS messages. It's not present in S3
        // So, in production we could pass the timestamp as a parameter for the EMR run
        // Assuming the EMR is run only for one S3 path. Or maybe several if they have the same timestamp
        // (generated in the same run from Keystone)
        executeWriteJob(sql, table, location, Instant.now().toEpochMilli());
    }

    private String getS3Content() {
        String json = "{" + "\"rowkey\":\"01183700193606162565567718189896254252\",\"cols:[{\"key\":\"NOTARGET\",val:\"NDY3Mg==\",\"ttl\":\"0\"}]}";
        return json;
    }


    private void executeWriteJob(SQLContext sql, String table, String location, long timestamp)
    {
//        logger.info("Import data from S3 {} Sto table {}", import_source, job.get("table"));

        // should have option("fs.s3a.bucket.<bucket>.endpoint.region", "us-east-1")?
        Dataset<Row> df = sql.read().option("allowUnquotedFieldNames", "true").option("compression", "gzip")
                .json(location)
                .select(col("rowkey"), explode(col("cols")).as("exploded_element")).select(
                        col("rowkey").as("key"),
                        col("exploded_element.key").as("column1"),
                        col("exploded_element.ttl").as("ttl"),
                        col("exploded_element").getField("val").as("value") // Use getField("val") for the reserved keyword
          );

        DataFrameWriter<Row> writer = df.write().format("org.apache.cassandra.spark.sparksql.CassandraDataSink");
        writer.options(writerOptions);
        writer.option(WriterOptions.TTL.name(), TTLOption.perRow("ttl"));
        writer.option(WriterOptions.TIMESTAMP.name(), TimestampOption.constant(timestamp));
        writer.mode("append").save();

    }

    private void executeJob(SQLContext sql, Map<String, String> job, String location, String timestamp)
    {
        readerOptions.put("keyspace", job.get("keyspace"));
        writerOptions.put("keyspace", job.get("keyspace"));

        readerOptions.put("table", job.get("table"));
        writerOptions.put("table", job.get("table"));

        Dataset<Row> df = null;
        if (job.get("operation").equals("export") || job.get("operation").equals("count"))
        {
            DataFrameReader reader = sql.read().format("org.apache.cassandra.spark.sparksql.CassandraDataSource");
            reader.options(readerOptions);
            df = reader.load();
            if (!job.getOrDefault("columns", "*").equals("*"))
            {
                List<String> columns = Arrays.stream(job.get("columns").split(","))
                        .map(s -> s.trim()).filter(s -> !s.isEmpty()).collect(Collectors.toList());
                String firstColumn = columns.get(0);
                columns.remove(0);
                df = df.select(firstColumn, columns.toArray(new String[0]));
            }
        }

        logger.info("Starting Spark job " + job.get("operation") + " on " + job.get("table"));

        switch (job.get("operation"))
        {
            case "count":
                long count = df.count();
                logger.info("Found {} records", count);
                System.out.println("Found " + count + " records in " + job.get("table"));
                break;
            case "export":
                logger.info("Export to {} .....", job.get("table"));
                String tableLocation = location + job.get("table") + "." + job.get("format") + '/' + timestamp;
                DataFrameWriter<Row> dfw = df.write();
                if (location.startsWith("s3a://"))
                {
                    dfw.option("fs.s3a.committer.name", "directory");
                    dfw.option("fs.s3a.committer.conflict-mode", "replace");
                }
                dfw.mode("overwrite").option("compression", "gzip");
                switch (job.get("format"))
                {
                    case "parquet":
                        dfw.parquet(tableLocation);
                        break;
                    case "csv":
                        dfw.option("header", "true").csv(tableLocation);
                        break;
                    default:
                        logger.error("Unknown format " + job.get("format") + " for table " + job.get("table"));
                        break;
                }
                break;
            case "import":
//                String import_source = "s3a://aam-s2s-traits-stage-us-east-1/2025.12.11-00.00.02/profilemerge_spp_users_optout/region=7/";
                String import_source = "s3a://" + config.getBucket() + "/" + job.get("import_path");
                executeWriteJob(sql, job.get("table"), import_source);

                break;
            default:
                logger.error("Unknown operation " + job.get("operation") + " for table " + job.get("table"));
                return;
        }
        logger.info("Finished Spark job " + readerOptions.get("table") + " shutting down...");
    }

}
