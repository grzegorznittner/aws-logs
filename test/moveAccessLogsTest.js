const {createCopyParams} = require("../lambdas/moveAccessLogs");
const assert = require("assert");

describe("analytics move access logs function - createCopyParams tests", function() {
  it("cloudfront logs 1", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "cloudfronts/E2RM06AWPLPG4A.2019-12-23-04.ee0a91d6.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "cloudfronts/raw/year=2019/month=12/day=23/hour=04/E2RM06AWPLPG4A.2019-12-23-04.ee0a91d6.gz");
  });

  it("cloudfront logs 2", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "cloudfronts/E2MB6G0UQ7QSIX.2019-11-25-05.1f5f36ff.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "cloudfronts/raw/year=2019/month=11/day=25/hour=05/E2MB6G0UQ7QSIX.2019-11-25-05.1f5f36ff.gz");
  });
  
  it("cloudfront logs 3", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "cloudfronts/EW0CYG3RXMS96.2019-12-11-09.cdcf4ab4.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "cloudfronts/raw/year=2019/month=12/day=11/hour=09/EW0CYG3RXMS96.2019-12-11-09.cdcf4ab4.gz");
  });
  
  it("cloudfront logs 4", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "cloudfronts/E2NJ3QYW5Z28DZ.2019-11-14-19.c1d8d31e.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "cloudfronts/raw/year=2019/month=11/day=14/hour=19/E2NJ3QYW5Z28DZ.2019-11-14-19.c1d8d31e.gz");
  });
  
  it("s3 bucket access logs 1", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "s3buckets/2019-10-07-07-34-53-96349443E23AFB45";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "s3buckets/raw/year=2019/month=10/day=07/hour=07/2019-10-07-07-34-53-96349443E23AFB45");
  });
  
  it("s3 bucket access logs 2", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "s3buckets/2019-12-18-14-18-13-5274284401078609";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "s3buckets/raw/year=2019/month=12/day=18/hour=14/2019-12-18-14-18-13-5274284401078609");
  });

  it("cloud trail logs", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "cloudtrail/AWSLogs/1234567890/CloudTrail/eu-central-1/2019/12/06/1234567890_CloudTrail_eu-central-1_20191206T0000Z_ogS3SY6UQgeVHcJc.json.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "cloudtrail/raw/year=2019/month=12/day=06/hour=00/1234567890_CloudTrail_eu-central-1_20191206T0000Z_ogS3SY6UQgeVHcJc.json.gz");
  });
  
  it("cloud trail digest logs", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "cloudtrail/AWSLogs/1234567890/CloudTrail-Digest/eu-central-1/2019/11/02/1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "cloudtrail/digest/year=2019/month=11/day=02/hour=00/1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz");
  });
  
  it("unrecognized logs 1", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "cloudtrail/1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "unrecognized/1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz");
  });
  
  it("unrecognized logs 2", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "abc/1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "unrecognized/1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz");
  });
  
  it("unrecognized logs 3", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "abc/def/1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "unrecognized/1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz");
  });
  
  it("unrecognized logs 4", function() {
    const sourceBucket = "source-bucket";
    const sourceKey = "1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz";
    const targetBucket = "target-bucket";
    const output = createCopyParams(sourceBucket, sourceKey, targetBucket);
    assert.equal(output.CopySource, sourceBucket + "/" + sourceKey);
    assert.equal(output.Bucket, targetBucket);
    assert.equal(output.Key, "unrecognized/1234567890_CloudTrail-Digest_eu-central-1_preprod-trail_eu-central-1_20191102T003813Z.json.gz");
  });
});
