/**
 MIT License
 Copyright (c) 2020 Grzegorz Nittner

 Based on https://github.com/aws-samples/amazon-cloudfront-access-logs-queries
 thanks to https://github.com/steffeng
 */

const { prepareAggregationQueries }  = require("../lambdas/aggregation");
const { config } = require("../lambdas/config");
const assert = require("assert");

const env = Object.assign({}, process.env);

after(() => {
    process.env = env;
});

function print(text, prefix) {
  const start = text.indexOf(prefix);
  if (start>=0) {
    return text.substr(start, 120);
  } else {
    return text;
  }
}

describe("analytics transform access logs function - prepareAggregationQueries tests", function() {
  const configMap = {};
  config.map(log => {
    configMap[log.name] = log;
  });
  process.env.DATABASE = 'test-database';
  process.env.ATHENA_RESULTS_LOCATION = 's3://test-bucket';
  
  it("check december day", function() {
    const year = '2019';
    const month = '12';
    const day = '21';
    const hour = '12';
    const output = prepareAggregationQueries(year, month, day, hour);
    assert.equal(output.length, 3);
    
    output.map(entry => {
      const log = configMap[entry.name];
      console.log(`Checking response for: ${entry.name}`);
      
      assert.equal(entry.s3Bucket, 'test-bucket');
      assert.equal(entry.s3DataPath, log.aggregationPath.substr(1)+'/year=2019/month=12/day=21/hour=12/');
      
      assert.equal(entry.intermediateTable, `ctas_agg_${log.name}_2019_12_21_12`);
      
      assert.ok(entry.aggregationStatement.indexOf(
        `CREATE TABLE test-database.${entry.intermediateTable}`)>=0,
        "CREATE TABLE table is incorrect: "+print(entry.aggregationStatement, 'CREATE TABLE'));
      assert.ok(entry.aggregationStatement.indexOf(
        `external_location='s3://test-bucket${log.aggregationPath}/year=2019/month=12/day=21/hour=12'`)>=0,
        "external_location is incorrect: "+print(entry.aggregationStatement, 'external_location'));
      
      switch (log.type) {
      case 'cloudfront':
        assert.ok(entry.aggregationStatement.indexOf(
          `SELECT host, uri, status, count(*) as count, SUM(bytes) as total_bytes,`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
        assert.ok(entry.aggregationStatement.indexOf(
          `CAST(MIN(timetaken) AS DOUBLE) as min_time, CAST(MAX(timetaken) AS DOUBLE) as max_time,`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
        assert.ok(entry.aggregationStatement.indexOf(
          `CAST(AVG(timetaken) AS DOUBLE) as avg_time, CAST(STDDEV(timetaken) AS DOUBLE) as stddev_time`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
      break;
      case 's3access':
        assert.ok(entry.aggregationStatement.indexOf(
          `SELECT bucket as bucket_name, httpstatus as status, count(*) as count`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
      break;
      case 'cloudtrail':
        assert.ok(entry.aggregationStatement.indexOf(
          `SELECT eventsource, awsregion, count(*) as count`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
      break;
      }
      
      
      console.log(`Glue table name: ${log.name}_aggregated`);
      assert.ok(entry.createPartitionStatement.indexOf(
        `ALTER TABLE test-database.${log.name}_aggregated`)>=0,
        "ALTER TABLE table is incorrect: "+print(entry.createPartitionStatement, 'ALTER TABLE'));
      assert.ok(entry.createPartitionStatement.indexOf(
        `year = '${year}'`)>=0,
        "PARTITION is incorrect: "+print(entry.createPartitionStatement, 'PARTITION'));
      assert.ok(entry.createPartitionStatement.indexOf(
        `month = '${month}'`)>=0,
        "PARTITION is incorrect: "+print(entry.createPartitionStatement, 'PARTITION'));
      assert.ok(entry.createPartitionStatement.indexOf(
        `day = '${day}'`)>=0,
        "PARTITION is incorrect: "+print(entry.createPartitionStatement, 'PARTITION'));
      assert.ok(entry.createPartitionStatement.indexOf(
        `hour = '${hour}'`)>=0,
        "PARTITION is incorrect: "+print(entry.createPartitionStatement, 'PARTITION'));
    });
  });

  it("check january day", function() {
    const year = '2020';
    const month = '01';
    const day = '01';
    const hour = '00';
    const output = prepareAggregationQueries(year, month, day, hour);
    output.map(entry => {
      const log = configMap[entry.name];
      console.log(`Checking response for: ${entry.name}`);
      
      assert.equal(entry.s3Bucket, 'test-bucket');
      assert.equal(entry.s3DataPath, log.aggregationPath.substr(1)+'/year=2020/month=01/day=01/hour=00/');
      
      assert.equal(entry.intermediateTable, `ctas_agg_${log.name}_2020_01_01_00`);
      
      assert.ok(entry.aggregationStatement.indexOf(
        `CREATE TABLE test-database.${entry.intermediateTable}`)>=0,
        "CREATE TABLE table is incorrect: "+print(entry.aggregationStatement, 'CREATE TABLE'));
      assert.ok(entry.aggregationStatement.indexOf(
        `external_location='s3://test-bucket${log.aggregationPath}/year=2020/month=01/day=01/hour=00'`)>=0,
        "external_location is incorrect: "+print(entry.aggregationStatement, 'external_location'));
      
      switch (log.type) {
      case 'cloudfront':
        assert.ok(entry.aggregationStatement.indexOf(
          `SELECT host, uri, status, count(*) as count, SUM(bytes) as total_bytes,`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
        assert.ok(entry.aggregationStatement.indexOf(
          `CAST(MIN(timetaken) AS DOUBLE) as min_time, CAST(MAX(timetaken) AS DOUBLE) as max_time,`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
        assert.ok(entry.aggregationStatement.indexOf(
          `CAST(AVG(timetaken) AS DOUBLE) as avg_time, CAST(STDDEV(timetaken) AS DOUBLE) as stddev_time`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
      break;
      case 's3access':
        assert.ok(entry.aggregationStatement.indexOf(
          `SELECT bucket as bucket_name, httpstatus as status, count(*) as count`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
      break;
      case 'cloudtrail':
        assert.ok(entry.aggregationStatement.indexOf(
          `SELECT eventsource, awsregion, count(*) as count`)>=0,
          "SELECT statement is incorrect: "+print(entry.aggregationStatement, 'SELECT'));
      break;
      }
      
      
      assert.ok(entry.createPartitionStatement.indexOf(
        `ALTER TABLE test-database.${log.name}_aggregated`)>=0,
        "ALTER TABLE table is incorrect: "+print(entry.createPartitionStatement, 'ALTER TABLE'));
      assert.ok(entry.createPartitionStatement.indexOf(
        `year = '${year}'`)>=0,
        "PARTITION is incorrect: "+print(entry.createPartitionStatement, 'PARTITION'));
      assert.ok(entry.createPartitionStatement.indexOf(
        `month = '${month}'`)>=0,
        "PARTITION is incorrect: "+print(entry.createPartitionStatement, 'PARTITION'));
      assert.ok(entry.createPartitionStatement.indexOf(
        `day = '${day}'`)>=0,
        "PARTITION is incorrect: "+print(entry.createPartitionStatement, 'PARTITION'));
      assert.ok(entry.createPartitionStatement.indexOf(
        `hour = '${hour}'`)>=0,
        "PARTITION is incorrect: "+print(entry.createPartitionStatement, 'PARTITION'));
    });
  });
});
