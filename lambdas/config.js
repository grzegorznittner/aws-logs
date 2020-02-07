exports.config = [
    {
        name: "cloudfront",
        type: "cloudfront",
        source: "cloudfront_raw",
        target: "cloudfront_search",
        path: "/cloudfront/search",
        aggregationPath: "/cloudfront/aggregated"
    },
    {
        name: "s3buckets",
        type: "s3access",
        source: "s3buckets_raw",
        target: "s3buckets_search",
        path: "/s3buckets/search",
        aggregationPath: "/s3buckets/aggregated"
    },
    {
        name: "cloudtrail",
        type: "cloudtrail",
        source: "cloudtrail_raw",
        target: "cloudtrail_search",
        path: "/cloudtrail/trail",
        aggregationPath: "/cloudtrail/aggregated"
    },
    {
        name: "cloudtrail-digest",
        type: "cloudtrail-digest"
    }
];
