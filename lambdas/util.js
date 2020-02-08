/**
 MIT License
 Copyright (c) 2020 Grzegorz Nittner

 Based on https://github.com/aws-samples/amazon-cloudfront-access-logs-queries
 thanks to https://github.com/steffeng
 */
const aws = require('aws-sdk');
const athena = new aws.Athena({apiVersion: '2017-05-18'});
const glue = new aws.Glue({apiVersion: '2017-03-31'});
var s3 = new aws.S3({apiVersion: '2006-03-01'});

// s3 URL of the query results (without trailing slash)
const athenaQueryResultsLocation = process.env.ATHENA_QUERY_RESULTS_LOCATION;


async function waitForQueryExecution(query, queryExecutionId) {
    exports.log(`Executing Athena statement: ${query} queryExecutionId:${queryExecutionId}`);
    while (true) {
        var data = await athena.getQueryExecution({
            QueryExecutionId: queryExecutionId
        }).promise();
        const state = data.QueryExecution.Status.State;
        if (state === 'SUCCEEDED') {
            exports.log(`End of Athena statement: ${queryExecutionId}`);
            return;
        } else if (state === 'FAILED' || state === 'CANCELLED') {
            throw Error(`Query ${queryExecutionId} failed: ${data.QueryExecution.Status.StateChangeReason}`);
        }
        await new Promise(resolve => setTimeout(resolve, 500));
    }
}

exports.runQueryAndWait = async (query) => {
    var params = {
        QueryString: query,
        ResultConfiguration: {
            OutputLocation: athenaQueryResultsLocation
        }
    };
    return athena.startQueryExecution(params).promise()
        .then(data => waitForQueryExecution(query, data.QueryExecutionId));
};

exports.deleteGlueTable = (database, tableName) => {
    const params = {
        DatabaseName: database,
        Name: tableName
    };
    exports.log(`Delete Glue table: ${database}.${tableName}`);
    return glue.deleteTable(params).promise();
};

exports.listGlueTables = (database, filterExpression) => {
    var params = {
        DatabaseName: database,
        Expression: filterExpression
    };
    return glue.getTables(params).promise();
};

exports.deleteS3Folder = (bucketName, folder, callback) => {
    var params = {
        Bucket: bucketName,
        Prefix: folder
    };

    s3.listObjects(params, function (err, data) {
        if (err) return callback(err);

        const totalFiles = data.Contents.length;
        if (totalFiles === 0) callback();

        exports.log(`Deleting ${totalFiles} files from s3://${bucketName}/${folder}`);
        params = {Bucket: bucketName};
        params.Delete = {Objects: []};

        data.Contents.forEach(function (content) {
            params.Delete.Objects.push({Key: content.Key});
        });

        s3.deleteObjects(params, function (err, data) {
            if (err) return callback(err);
            if (totalFiles >= 1000)
                exports.deleteS3Folder(bucketName, folder, callback);
            else callback();
        });
    });
};

exports.log = (message) => {
    console.log(new Date().toISOString() + " " + message);
};
