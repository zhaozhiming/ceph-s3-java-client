package com.github.zzm;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
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
        return connect.listObjects(bucket.getName());
    }

    public PutObjectResult putObject(File file, Bucket bucket) throws FileNotFoundException {
        return createConnect().putObject(
                bucket.getName(), file.getName(), new FileInputStream(file), new ObjectMetadata());
    }

    public S3Object getObject(String bucketName, String objectName) {
        return createConnect().getObject(bucketName, objectName);
    }

    public Bucket getBucket(String bucketName) {
        List<Bucket> buckets = listBuckets();
        for (Bucket bucket : buckets) {
            if (bucketName.equals(bucket.getName())) {
                return bucket;
            }
        }
        throw new RuntimeException("Bucket not found");
    }

    private AmazonS3 createConnect() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 conn = new AmazonS3Client(credentials);
        conn.setEndpoint(hostname);
        return conn;
    }


}
