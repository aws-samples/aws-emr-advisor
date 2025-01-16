# IAM Permissions

When analyzing Spark Event Logs, the tool utilizes AWS services to retrieve information related to pricing, EMR releases, and, optionally, Amazon S3 buckets especially if the event logs are stored on S3 or if the report is being written to an S3 bucket.

Below is an example of the minimal IAM permissions required when running the Spark analysis:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::<your-bucket>",
                "arn:aws:s3:::<your-bucket>/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "elasticmapreduce:ListReleaseLabels",
                "elasticmapreduce:DescribeReleaseLabel"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": "pricing:GetProducts",
            "Resource": "*"
        }
    ]
}
```
