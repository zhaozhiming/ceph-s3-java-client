package com.github.zzm;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

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
        return createConnect().listBuckets();
    }

    public Bucket createBucket(String bucketName) {
        return createConnect().createBucket(bucketName);
    }

    public ObjectListing listObjects(Bucket bucket) {
        return createConnect().listObjects(bucket.getName());
    }

    public PutObjectResult putObject(File file, Bucket bucket) throws FileNotFoundException {
        return createConnect().putObject(
                bucket.getName(), file.getName(), new FileInputStream(file), new ObjectMetadata());
    }

    public S3Object getObject(String bucketName, String objectName) {
        return createConnect().getObject(bucketName, objectName);
    }

    public ObjectMetadata downloadObject(String bucketName, String objectName, File downloadFile) {
        return createConnect().getObject(new GetObjectRequest(bucketName, objectName), downloadFile);
    }

    public void deleteObject(String bucketName, String fileName) {
        createConnect().deleteObject(bucketName, fileName);
    }

    public Bucket getBucket(String bucketName) {
        List<Bucket> buckets = listBuckets();
        for (Bucket bucket : buckets) {
            if (bucketName.equals(bucket.getName())) {
                return bucket;
            }
        }
        throw new RuntimeException(String.format("Bucket: %s can't be found.", bucketName));
    }

    private AmazonS3 createConnect() {
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 conn = new AmazonS3Client(credentials);
        conn.setEndpoint(hostname);
        return conn;
    }


}
