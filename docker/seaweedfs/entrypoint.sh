#!/bin/sh
# Render the S3 identity from env, then run master + volume + filer + S3 gateway in one process.
set -eu
mkdir -p /data /etc/seaweedfs
cat > /etc/seaweedfs/s3.json <<JSON
{
  "identities": [
    {
      "name": "playground",
      "credentials": [{"accessKey": "${S3_ACCESS_KEY}", "secretKey": "${S3_SECRET_KEY}"}],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    }
  ]
}
JSON
exec weed server -dir=/data -ip=seaweedfs -ip.bind=0.0.0.0 \
  -master.volumeSizeLimitMB=1024 -volume.max=0 \
  -filer -s3 -s3.port=8333 -s3.config=/etc/seaweedfs/s3.json
