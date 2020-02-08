/**
 MIT License
 Copyright (c) 2020 Grzegorz Nittner

 Based on https://github.com/aws-samples/amazon-cloudfront-access-logs-queries
 thanks to https://github.com/steffeng
 */
const util = require('./util');
const {config} = require('./config');


// get the partition of an hour ago
exports.handler = async (event, context, callback) => {
    await exports.transformData(new Date(Date.now() - 60 * 60 * 1000))
        .catch(err => util.log(err));
};

exports.transformData = async (time) => {
    return new Promise(async function (resolve, reject) {
        try {
            const partitionHour = time;
            const year = partitionHour.getUTCFullYear();
            const month = (partitionHour.getUTCMonth() + 1).toString().padStart(2, '0');
            const day = partitionHour.getUTCDate().toString().padStart(2, '0');
            const hour = partitionHour.getUTCHours().toString().padStart(2, '0');

            // get definitions of all transformations
            const partitionLogSetsPlan = exports.createDataTransformations(year, month, day, hour);
            // execute transformations via AWS Athena
            await runDataTransformation(partitionLogSetsPlan);
            resolve();
        } catch (Error) {
            util.log(Error);
            reject(Error);
        }
    });
};


let runDataTransformation = async (transformPlan) => {
    util.log('Start of runDataTransformation');

    return new Promise(async function (resolve, reject) {

        const deleteS3Data = transformPlan.map(async transform => {
            return new Promise(resolve => {
                util.deleteS3Folder(transform.s3Bucket, transform.s3DataPath, resolve);
            }).then(err => {
                util.log(err !== undefined ?
                    `Error while deleting ${transform.s3Bucket}/${transform.s3DataPath} : ` + err :
                    `Files deleted from ${transform.s3Bucket}/${transform.s3DataPath}`);
            });
        });
        await Promise.all(deleteS3Data)
            .catch(err => util.log(err));
        util.log('End of deleting S3 data');

        const ctas = transformPlan.map(async transform => {
            return util.runQueryAndWait(transform.ctasStatement);
        });
        await Promise.all(ctas)
            .catch(err => util.log(err));
        util.log('End of ctas');

        const dropPartitions = transformPlan.map(async transform => {
            return util.runQueryAndWait(transform.dropPartitionStatement);
        });
        await Promise.all(dropPartitions)
            .catch(err => {
                util.log(err);
            });
        util.log('End of drop partitions');

        const createPartitions = transformPlan.map(async transform => {
            return util.runQueryAndWait(transform.createPartitionStatement);
        });
        await Promise.all(createPartitions)
            .catch(err => {
                util.log(err);
            });
        util.log('End of create partitions');

        const dropTable = transformPlan.map(async transform => {
            return util.deleteGlueTable(process.env.DATABASE, transform.intermediateTable);
        });
        await Promise.all(dropTable)
            .catch(err => {
                util.log(err);
            });
        util.log('End of drop tables');

        util.log('End of runDataTransformation');
    });
};


exports.createDataTransformations = (year, month, day, hour) => {
    // AWS Glue Data Catalog database and tables
    const database = process.env.DATABASE;

    // s3 URL to write CTAS results to (including trailing slash)
    const athenaCtasResultsLocation = process.env.ATHENA_RESULTS_LOCATION;

    const transforms = config.map(log => {
        if (log.target === undefined) {
            return;
        }
        util.log(`Transforming Data ${log.source} to ${log.target} `, {year, month, day, hour});

        const intermediateTable = `ctas_${log.target}_${year}_${month}_${day}_${hour}`;
        const path = `${log.path}/year=${year}/month=${month}/day=${day}/hour=${hour}`;
        const externalLocation = `${athenaCtasResultsLocation}${path}`;
        const s3DataPath = path.substr(1) + '/';
        const s3Bucket = athenaCtasResultsLocation.substr(5);

        const ctasStatement = `
      CREATE TABLE ${database}.${intermediateTable}
      WITH ( format='PARQUET',
          external_location='${externalLocation}',
          parquet_compression = 'SNAPPY')
      AS SELECT *
      FROM ${database}.${log.source}
      WHERE year = '${year}'
          AND month = '${month}'
          AND day = '${day}'
          AND hour = '${hour}';`;

        const dropPartitionStatement = `
      ALTER TABLE ${database}.${log.target}
      DROP PARTITION (
          year = '${year}',
          month = '${month}',
          day = '${day}',
          hour = '${hour}' );`;

        const createPartitionStatement = `
      ALTER TABLE ${database}.${log.target}
      ADD PARTITION (
          year = '${year}',
          month = '${month}',
          day = '${day}',
          hour = '${hour}' );`;

        return {
            name: log.name,
            s3Bucket: s3Bucket,
            s3DataPath: s3DataPath,
            intermediateTable: intermediateTable,
            ctasStatement: ctasStatement,
            dropPartitionStatement: dropPartitionStatement,
            createPartitionStatement: createPartitionStatement
        };
    });

    // removing undefined
    return transforms.filter(Boolean);
};
