/**
 MIT License
 Copyright (c) 2020 Grzegorz Nittner

 Based on https://github.com/aws-samples/amazon-cloudfront-access-logs-queries
 thanks to https://github.com/steffeng
 */
const { createDataTransformations }  = require("../lambdas/transformPartition");
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

describe("analytics transform access logs function - createDataTransformations tests", function() {
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
    const output = createDataTransformations(year, month, day, hour);
    assert.equal(output.length, 3);
    
    output.map(entry => {
      const log = configMap[entry.name];
      
      assert.equal(entry.s3Bucket, 'test-bucket');
      assert.equal(entry.s3DataPath, log.path.substr(1)+'/year=2019/month=12/day=21/hour=12/');
      
      assert.equal(entry.intermediateTable, `ctas_${log.target}_2019_12_21_12`);
      assert.ok(entry.ctasStatement.indexOf(
        `CREATE TABLE test-database.${entry.intermediateTable}`)>=0,
        "CREATE TABLE table is incorrect: "+print(entry.ctasStatement, 'CREATE TABLE'));
      assert.ok(entry.ctasStatement.indexOf(
        `external_location='s3://test-bucket${log.path}/year=2019/month=12/day=21/hour=12'`)>=0,
        "external_location is incorrect: "+print(entry.ctasStatement, 'external_location'));
      
      console.log(`Glue table name: ${log.target}`);
      assert.ok(entry.createPartitionStatement.indexOf(
        `ALTER TABLE test-database.${log.target}`)>=0,
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
    const output = createDataTransformations(year, month, day, hour);
    assert.equal(output.length, 3);
    
    output.map(entry => {
      const log = configMap[entry.name];
      
      assert.equal(entry.s3Bucket, 'test-bucket');
      assert.equal(entry.s3DataPath, log.path.substr(1)+'/year=2020/month=01/day=01/hour=00/');
      
      assert.equal(entry.intermediateTable, `ctas_${log.target}_2020_01_01_00`);
      assert.ok(entry.ctasStatement.indexOf(
        `CREATE TABLE test-database.${entry.intermediateTable}`)>=0,
        "CREATE TABLE table is incorrect: "+print(entry.ctasStatement, 'CREATE TABLE'));
      assert.ok(entry.ctasStatement.indexOf(
        `external_location='s3://test-bucket${log.path}/year=2020/month=01/day=01/hour=00'`)>=0,
        "external_location is incorrect: "+print(entry.ctasStatement, 'external_location'));
      
      assert.ok(entry.createPartitionStatement.indexOf(
        `ALTER TABLE test-database.${log.target}`)>=0,
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
