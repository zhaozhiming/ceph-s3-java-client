package com.github.zzm;

import com.amazonaws.services.s3.model.*;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RadosGatewayS3ClientTest {

    private static final String ACCESS_KEY = "ZGV5G0OFFUNESNRBK55Q";
    private static final String SECRET_KEY = "8DSkRch3bnbZHtQqyES9lHH78t7UftLiSdFFzSdP";
    private static final String HOSTNAME = "http://192.168.42.2:80";
    private static final String BUCKET_NAME = "bucket1";
    private static final String FILE_NAME = "hello.txt";
    private RadosGatewayS3Client client;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        client = new RadosGatewayS3Client(
                ACCESS_KEY, SECRET_KEY, HOSTNAME);
    }

    @Test
    public void should_create_bucket_correct() throws Exception {
        Bucket bucket = client.createBucket("bucket1");
        assertThat(bucket.getName(), is("bucket1"));
    }

    @Test
    public void should_list_buckets_correct() throws Exception {
        List<Bucket> buckets = client.listBuckets();
        assertThat(buckets.size(), is(1));
        assertThat(buckets.get(0).getName(), is(BUCKET_NAME));
        assertThat(buckets.get(0).getOwner().getDisplayName(), is("zhaozhiming1"));
    }

    @Test
    public void should_put_object_correct() throws Exception {
        client.putObject(new File(String.format("upload%s%s", File.separator, FILE_NAME)), client.getBucket(BUCKET_NAME));

        assertThat(client.getObject(BUCKET_NAME, FILE_NAME).getKey(), is(FILE_NAME));
    }

    @Test
    public void should_list_objecs_correct() throws Exception {
        ObjectListing objectListing = client.listObjects(client.getBucket(BUCKET_NAME));
        List<S3ObjectSummary> objects = objectListing.getObjectSummaries();
        assertThat(objects.size(), is(1));
        assertThat(objects.get(0).getKey(), is(FILE_NAME));
        assertThat(objects.get(0).getSize(), is(12L));
    }

    @Test
    public void should_get_object_correct() throws Exception {
        S3Object object = client.getObject(BUCKET_NAME, FILE_NAME);
        assertThat(object.getKey(), is(FILE_NAME));
    }

    @Test
    public void should_download_object_correct() throws Exception {
        File downloadFile = tempFolder.newFile(FILE_NAME);
        client.downloadObject(BUCKET_NAME, FILE_NAME, downloadFile);

        assertThat(downloadFile.exists(), is(true));
        assertThat(downloadFile.length(), is(12L));
    }
}