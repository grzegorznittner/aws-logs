/**
 MIT License
 Copyright (c) 2020 Grzegorz Nittner

 Based on https://github.com/aws-samples/amazon-cloudfront-access-logs-queries
 thanks to https://github.com/steffeng
 */
const { createPartitionsPlan }  = require("../lambdas/createPartitions");
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

describe("analytics create partitions for access logs function - createPartitionsPlan tests", function() {
  const configMap = {};
  config.map(log => {
    configMap[log.name] = log;
  });
  process.env.DATABASE = 'test-database';
  process.env.ATHENA_RESULTS_LOCATION = 's3://test-bucket';
  
  it("check january day", function() {
    const year = '2020';
    const month = '01';
    const day = '20';
    const hour = '12';
    const database = process.env.DATABASE;
    const output = createPartitionsPlan(year, month, day, hour);
    assert.equal(output.length, 3);
    
    output.map(entry => {
      const log = configMap[entry.name];
      
      assert.ok(entry.createPartitionStatement.indexOf(
        `ALTER TABLE ${database}.${log.source}`)>=0,
        "ALTER TABLE is incorrect: "+print(entry.createPartitionStatement, 'ALTER TABLE'));
      
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
