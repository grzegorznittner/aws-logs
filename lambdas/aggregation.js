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
    await exports.aggregateData(new Date(Date.now() - 60 * 60 * 1000))
        .catch(err => util.log(err));
};

exports.aggregateData = async (time) => {
    return new Promise(async function (resolve, reject) {
        try {
            const [year, month, day, hour] = util.getPartitionDetails(time);
            const aggregationPlan = exports.prepareAggregationQueries(year, month, day, hour);
            await runAggregation(aggregationPlan);
            resolve();
        } catch (Error) {
            util.log(Error);
            reject(Error);
        }
    });
};

let runAggregation = async (aggregationPlan) => {
    util.log('Start of runAggregation');
    return new Promise(async function (resolve, reject) {
        await Promise.all(util.deleteS3Folders(aggregationPlan))
            .catch(err => util.log(err));
        util.log('End of deleting S3 data');

        const ctas = aggregationPlan.map(async transform => {
            return util.runQueryAndWait(transform.aggregationStatement);
        });
        await Promise.all(ctas)
            .catch(err => util.log(err));
        util.log('End of aggregation');

        await Promise.all(util.dropPartition(aggregationPlan))
            .catch(err => util.log(err));
        util.log('End of drop partitions');

        const createPartitions = aggregationPlan.map(async transform => {
            return util.runQueryAndWait(transform.createPartitionStatement);
        });
        await Promise.all(createPartitions)
            .catch(err => {
                util.log(err);
            });
        util.log('End of create partitions');

        const dropTable = aggregationPlan.map(async transform => {
            return util.deleteGlueTable(process.env.DATABASE, transform.intermediateTable);
        });
        await Promise.all(dropTable)
            .catch(err => {
                util.log(err);
            });
        util.log('End of drop tables');

        util.log('End of runAggregation');
    });
};

exports.prepareAggregationQueries = (year, month, day, hour) => {
    // AWS Glue Data Catalog database and tables
    const database = process.env.DATABASE;

    // s3 URL to write CTAS results to (including trailing slash)
    const athenaResultsLocation = process.env.ATHENA_RESULTS_LOCATION;

    const transforms = config.map(log => {
        if (log.aggregationPath === undefined) {
            return;
        }

        console.log('Preparing Aggregation Queries', {year, month, day, hour});
        const intermediateTable = `ctas_agg_${log.name}_${year}_${month}_${day}_${hour}`;

        const path = `${log.aggregationPath}/year=${year}/month=${month}/day=${day}/hour=${hour}`;
        const s3DataPath = path.substr(1) + '/';
        const s3Bucket = athenaResultsLocation.substr(5);

        const aggregationLocation = athenaResultsLocation + log.aggregationPath;
        const aggregationStatement = createCloudFrontAggregation(log.type, database, log.target, intermediateTable,
            aggregationLocation, year, month, day, hour);

        const dropPartitionStatement = `
      ALTER TABLE ${database}.${log.name}_aggregated
      DROP PARTITION (
          year = '${year}',
          month = '${month}',
          day = '${day}',
          hour = '${hour}' );`;

        const createPartitionStatement = `
      ALTER TABLE ${database}.${log.name}_aggregated
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
            aggregationStatement: aggregationStatement,
            dropPartitionStatement: dropPartitionStatement,
            createPartitionStatement: createPartitionStatement
        };
    });

    // removing undefined
    return transforms.filter(Boolean);
};

let createCloudFrontAggregation = (type, database, table, intermediateTable, aggregationLocation,
                                   year, month, day, hour) => {
    switch (type) {
        case 'cloudfront':
            return `CREATE TABLE ${database}.${intermediateTable}
        WITH ( format='PARQUET',
            external_location='${aggregationLocation}/year=${year}/month=${month}/day=${day}/hour=${hour}',
            parquet_compression = 'SNAPPY')
        AS SELECT host, uri, status, count(*) as count, SUM(bytes) as total_bytes,
           CAST(MIN(timetaken) AS DOUBLE) as min_time, CAST(MAX(timetaken) AS DOUBLE) as max_time,
           CAST(AVG(timetaken) AS DOUBLE) as avg_time, CAST(STDDEV(timetaken) AS DOUBLE) as stddev_time
        FROM ${database}.${table}
        WHERE year = '${year}'
            AND month = '${month}'
            AND day = '${day}'
            AND hour = '${hour}'
        GROUP BY host, uri, status;`;
        case 's3access':
            return `CREATE TABLE ${database}.${intermediateTable}
        WITH ( format='PARQUET',
            external_location='${aggregationLocation}/year=${year}/month=${month}/day=${day}/hour=${hour}',
            parquet_compression = 'SNAPPY')
        AS SELECT bucket as bucket_name, httpstatus as status, count(*) as count
        FROM ${database}.${table}
        WHERE year = '${year}'
            AND month = '${month}'
            AND day = '${day}'
            AND hour = '${hour}'
        GROUP BY bucket, httpstatus;`;
        case 'cloudtrail':
            return `CREATE TABLE ${database}.${intermediateTable}
        WITH ( format='PARQUET',
            external_location='${aggregationLocation}/year=${year}/month=${month}/day=${day}/hour=${hour}',
            parquet_compression = 'SNAPPY')
        AS SELECT eventsource, awsregion, count(*) as count
        FROM ${database}.${table}
        WHERE year = '${year}'
            AND month = '${month}'
            AND day = '${day}'
            AND hour = '${hour}'
        GROUP BY eventsource, awsregion;`;
    }
};
