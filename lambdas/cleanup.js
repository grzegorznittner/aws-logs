/**
 MIT License
 Copyright (c) 2020 Grzegorz Nittner
 */
const util = require('./util');

// creates partitions for the hour after the current hour
exports.handler = async (event, context, callback) => {
    await exports.cleanUp()
        .catch(err => util.log(err));
};

exports.cleanUp = async () => {
    return new Promise(async function (resolve, reject) {
        try {
            const database = process.env.DATABASE;
            const athenaQueryResultsLocation = process.env.ATHENA_QUERY_RESULTS_LOCATION;

            await deleteCTASTables(database);
            await deleteAthenaQueryDataLocation(athenaQueryResultsLocation);
            await exports.deleteRawData(90);
            resolve();
        } catch (Error) {
            reject(Error);
        }
    });
};

let deleteCTASTables = async (database) => {
    util.log('Attempting to delete remaining CTAS tables');
    await util.listGlueTables(database, '^ctas.*')
        .then(data => {
            data.TableList.map(async table => {
                await util.deleteGlueTable(database, table.Name)
                    .then(util.log('  deleted Glue table: ' + table.Name))
                    .catch(err => util.log(err));
            })
        })
        .catch(err => util.log(err));

    util.log('End deleting CTAS tables');
};

let deleteAthenaQueryDataLocation = (athenaQueryResultsLocation) => {

    const split = athenaQueryResultsLocation.split('/');
    const bucket = split[2];
    const s3Path = split[3];
    util.log(`Deleting contents of s3://${bucket}/${s3Path}`);

    return new Promise(resolve => {
        util.deleteS3Folder(bucket, s3Path, resolve);
    }).then(err => {
        util.log(err !== undefined ?
            `Error while deleting ${bucket}/${s3Path} : ` + err :
            `Files deleted from ${bucket}/${s3Path}`);
    });
};

exports.deleteRawData = (numOfDays) => {
    util.log("deleteRawData");

    const paritionedStorage = process.env.PARTITIONED_STORAGE;
    const optimizedStorage = process.env.OPTIMIZED_STORAGE;
    util.log("PARTITIONED_STORAGE=" + paritionedStorage);
    util.log("OPTIMIZED_STORAGE=" + optimizedStorage);

    const logLocations = [[paritionedStorage, "cloudfronts/raw"],
        [paritionedStorage, "s3buckets/raw"],
        [optimizedStorage, "cloudfronts/search"],
        [optimizedStorage, "s3buckets/search"]];

    const promises = logLocations.map((item) => {
        const bucket = item[0];
        const s3Path = item[1] + exports.dayAgoSuffix(new Date(), numOfDays);
        util.log("  - deleting s3://" + item[0] + "/" + s3Path);
        return new Promise(resolve => {
            util.deleteS3Folder(bucket, s3Path, resolve);
        }).then(err => {
            util.log(err !== undefined ?
                `Error while deleting ${bucket}/${s3Path} : ` + err :
                `Files deleted from ${bucket}/${s3Path}`);
        });
    });
    return Promise.all(promises);
};

exports.dayAgoSuffix = (now, daysAgo) => {
    const date = new Date(now - daysAgo * 24 * 60 * 60 * 1000);

    return "/year=" + date.getUTCFullYear()
        + "/month=" + ((date.getUTCMonth() + 1).toString().padStart(2, '0'))
        + "/day=" + date.getUTCDate().toString().padStart(2, '0');
};
