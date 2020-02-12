/**
 MIT License
 Copyright (c) 2020 Grzegorz Nittner

 Based on https://github.com/aws-samples/amazon-cloudfront-access-logs-queries
 thanks to https://github.com/steffeng
 */

const aws = require('aws-sdk');
const s3 = new aws.S3({apiVersion: '2006-03-01'});

const targetBucket = process.env.TARGET_BUCKET;
const DEBUG = process.env.DEBUG;

exports.handler = async (event, context, callback) => {
    const moves = event.Records.map(record => {
        const bucket = record.s3.bucket.name;
        const sourceKey = record.s3.object.key;
        console.log(`Handling s3://${bucket}/${sourceKey}.`);

        const copyParams = exports.createCopyParams(bucket, sourceKey, targetBucket);
        const deleteParams = {Bucket: bucket, Key: sourceKey};
        const copy = s3.copyObject(copyParams).promise();

        return copy.then(function () {
            console.log(`Copied s3://${bucket}/${sourceKey} to s3://${copyParams.Bucket}/${copyParams.Key}`);
            const del = s3.deleteObject(deleteParams).promise();
            console.log(`Deleted ${sourceKey}.`);
            return del;
        }, function (reason) {
            const error = new Error(`Error while copying s3://${bucket}/${sourceKey}: ${reason}`);
            callback(error);
        });
    });
    await Promise.all(moves);
};


exports.createCopyParams = (sourceBucket, sourceKey, targetBucket) => {
    const cloudfrontRegex = /^cloudfronts\/(?<prefix>[^.]*).(?<year>\d*)-(?<month>\d*)-(?<day>\d*)-(?<hour>\d*).(?<suffix>.*)/gm;
    const s3accessLogsRegex = /^s3buckets\/(?<year>\d*)-(?<month>\d*)-(?<day>\d*)-(?<hour>\d*).(?<suffix>.*)/gm;
    const cloudtrailDigestRegex = /^cloudtrail\/AWSLogs\/(?<account>[^\/]*)\/CloudTrail-Digest\/(?<region>[^\/]*)\/(?<year>\d*)\/(?<month>\d*)\/(?<day>\d*)\/(?<suffix>.*)\d{8}T(?<hour>\d{2}).*/gm;
    const cloudtrailRegex = /^cloudtrail\/AWSLogs\/(?<account>[^\/]*)\/CloudTrail\/(?<region>[^\/]*)\/(?<year>\d*)\/(?<month>\d*)\/(?<day>\d*)\/(?<suffix>.*)\d{8}T(?<hour>\d{2}).*/gm;

    console.log(`source key: ${sourceKey}`);
    let targetKey;
    let m;
    if ((m = cloudfrontRegex.exec(sourceKey)) !== null) {
        targetKey = handleCloudFrontLogs(m, cloudfrontRegex);
    } else if ((m = s3accessLogsRegex.exec(sourceKey)) !== null) {
        targetKey = handleS3AccessLogs(m, s3accessLogsRegex);
    } else if ((m = cloudtrailRegex.exec(sourceKey)) !== null) {
        targetKey = handleCloudTrailLogs(sourceKey, m, cloudtrailRegex);
    } else if ((m = cloudtrailDigestRegex.exec(sourceKey)) !== null) {
        targetKey = handleCloudTrailDigestLogs(sourceKey, m, cloudtrailDigestRegex);
    } else {
        targetKey = handleUnrecognized(sourceKey);
    }

    return {
        CopySource: sourceBucket + '/' + sourceKey,
        Bucket: targetBucket,
        Key: targetKey
    };
};

function handleCloudFrontLogs(m, regex) {
    console.log(`handleCloudFrontLogs`);
    printMatches(m, regex);
    return `cloudfronts/raw/year=${m[2]}/month=${m[3]}/day=${m[4]}/hour=${m[5]}/${m[1]}.${m[2]}-${m[3]}-${m[4]}-${m[5]}.${m[6]}`;
}

function handleS3AccessLogs(m, regex) {
    console.log(`handleS3AccessLogs`);
    printMatches(m, regex);
    return `s3buckets/raw/year=${m[1]}/month=${m[2]}/day=${m[3]}/hour=${m[4]}/${m[1]}-${m[2]}-${m[3]}-${m[4]}-${m[5]}`;
}

function handleCloudTrailLogs(sourceKey, m, regex) {
    console.log(`handleCloudTrailLogs`);
    printMatches(m, regex);
    const filename = sourceKey.split('/').slice(-1).pop();
    return `cloudtrail/raw/year=${m[3]}/month=${m[4]}/day=${m[5]}/hour=${m[7]}/${filename}`;
}

function handleCloudTrailDigestLogs(sourceKey, m, regex) {
    console.log(`handleCloudTrailDigestLogs`);
    printMatches(m, regex);
    const filename = sourceKey.split('/').slice(-1).pop();
    return `cloudtrail/digest/year=${m[3]}/month=${m[4]}/day=${m[5]}/hour=${m[7]}/${filename}`;
}

function handleUnrecognized(sourceKey) {
    console.log(`Found unrecognized: ${sourceKey}`);
    const [year, month, day, hour] = util.getPartitionDetails(Date.now());
    const filename = sourceKey.split('/').slice(-1).pop();
    return `unrecognized/${year}-${month}-${day}-${hour}_${filename}`;
}

function printMatches(m, regex) {
    if (typeof DEBUG !== 'undefined' && DEBUG) {
        // This is necessary to avoid infinite loops with zero-width matches
        if (m.index === regex.lastIndex) {
            regex.lastIndex++;
        }
        // The result can be accessed through the `m`-variable.
        m.forEach((match, groupIndex) => {
            console.log(`  - ${groupIndex}: ${match}`);
        });
    }
}
