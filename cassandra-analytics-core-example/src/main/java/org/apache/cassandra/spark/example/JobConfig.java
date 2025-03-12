package org.apache.cassandra.spark.example;

import java.util.List;
import java.util.Map;

public class JobConfig {
    private Boolean local;
    private String keyspace;
    private String table;
    private String sidecarContactPoints;
    private String operation;
    private String location;
    private String dc;
    private String columns;
    private List<Map<String, String>> jobs;

    public JobConfig() {
        this.local = true;
        this.keyspace = "cos_primary_shard_va6_dev_01";
        this.table = "components";
        this.sidecarContactPoints = "";
        this.operation = "count";
        this.location = "/var/aws/";
        this.dc = "us-east-1";
        this.columns = "*";
        this.jobs = new List<Map<String, String>>();
    }

    public Boolean getLocal() {
        return local;
    }

    public void setLocal(Boolean local) {
        this.local = local;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSidecarContactPoints() {
        return sidecarContactPoints;
    }

    public void setSidecarContactPoints(String sidecarContactPoints) {
        this.sidecarContactPoints = sidecarContactPoints;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public String getColumns() {
        return columns;
    }

    public void setColumns(String columns) {
        this.columns = columns;
    }

    public List<Map<String, String>> getJobs() {
        return jobs;
    }

    public void setJobs(List<Map<String, String>> jobs) {
        this.jobs = jobs;
    }

    public String toString() {
        return "JobConfig(local=" + this.getLocal() + ", keyspace=" + this.getKeyspace() + ", table=" + this.getTable() + ", sidecarContactPoints=" + this.getSidecarContactPoints() + ", operation=" + this.getOperation() + ", location=" + this.getLocation() + ")";
    }


}