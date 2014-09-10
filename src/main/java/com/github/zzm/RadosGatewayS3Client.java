package com.github.zzm;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

public class RadosGatewayS3Client {
    private String accessKey;
    private String secretKey;
    private String hostname;

    public RadosGatewayS3Client(String accessKey, String secretKey, String hostname) {
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.hostname = hostname;
    }

    public AmazonS3 createConnect() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 conn = new AmazonS3Client(credentials);
        conn.setEndpoint(hostname);
        return conn;
    }

    public List<Bucket> listBuckets() {
        AmazonS3 connect = createConnect();
        List<Bucket> buckets = connect.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getName() + "\t" +
                    StringUtils.fromDate(bucket.getCreationDate()));
        }
        return buckets;
    }

    public Bucket createBucket(String bucketName) {
        return createConnect().createBucket(bucketName);
    }

    public ObjectListing listObjects(Bucket bucket) {
        AmazonS3 connect = createConnect();
        ObjectListing objects = connect.listObjects(bucket.getName());
        do {
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                System.out.println(objectSummary.getKey() + "\t" +
                        objectSummary.getSize() + "\t" +
                        StringUtils.fromDate(objectSummary.getLastModified()));
            }
            objects = connect.listNextBatchOfObjects(objects);
        } while (objects.isTruncated());

        return objects;
    }

    public void putObject(File file, Bucket bucket) throws FileNotFoundException {
        createConnect().putObject(bucket.getName(), file.getName(), new FileInputStream(file), new ObjectMetadata());
    }


}
