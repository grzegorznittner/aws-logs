/**
 MIT License
 Copyright (c) 2020 Grzegorz Nittner

 Based on https://github.com/aws-samples/amazon-cloudfront-access-logs-queries
 thanks to https://github.com/steffeng
 */
const util = require('./util');
const { config } = require('./config');

// creates partitions for the hour following the current time
exports.handler = async (event, context, callback) => {
  await exports.createPartitions(new Date(Date.now() + 60 * 60 * 1000))
    .catch(err => util.log(err));
};

exports.createPartitions = async (time) => {
  return new Promise(async function(resolve, reject) {
    try {
      const partitionHour = time;
      var year = partitionHour.getUTCFullYear();
      var month = (partitionHour.getUTCMonth() + 1).toString().padStart(2, '0');
      var day = partitionHour.getUTCDate().toString().padStart(2, '0');
      var hour = partitionHour.getUTCHours().toString().padStart(2, '0');
      util.log('Creating Partitions', { year, month, day, hour });
    
      const partitionsPlan = exports.createPartitionsPlan(year, month, day, hour);
      await runPartitioning(partitionsPlan);
      resolve();
    } catch (Error) {
      reject(Error);
    }
  });
};

let runPartitioning = async (transformPlan) => {
  util.log('Start of runPartitioning');
  
  return new Promise(async function(resolve, reject) {
  
    const dropPartitions = transformPlan.map(async transform => {
        return util.runQueryAndWait(transform.dropPartitionStatement);
    });
    await Promise.all(dropPartitions)
      .catch(err => util.log(err));
    util.log('End of drop partitions');
    
    const createPartitions = transformPlan.map(async transform => {
        return util.runQueryAndWait(transform.createPartitionStatement);
    });
    await Promise.all(createPartitions)
      .catch(err => util.log(err));
    util.log('End of create partitions');
    
    util.log('End of runPartitioning');
  });
};


exports.createPartitionsPlan = (year, month, day, hour) => {
  // AWS Glue Data Catalog database
  const database = process.env.DATABASE;

  const transforms = config.map(log => {
    if (log.source === undefined) {
      return;
    }

    const dropPartitionStatement = `
    ALTER TABLE ${database}.${log.source}
    DROP PARTITION (
        year = '${year}',
        month = '${month}',
        day = '${day}',
        hour = '${hour}' );`;
    
    const createPartitionStatement = `
    ALTER TABLE ${database}.${log.source}
    ADD PARTITION (
        year = '${year}',
        month = '${month}',
        day = '${day}',
        hour = '${hour}' );`;
    
    const transform = {
      name: log.name,
      dropPartitionStatement: dropPartitionStatement,
      createPartitionStatement: createPartitionStatement
    };
    return transform;
  });
  // removing undefined
  return transforms.filter(Boolean);
};
