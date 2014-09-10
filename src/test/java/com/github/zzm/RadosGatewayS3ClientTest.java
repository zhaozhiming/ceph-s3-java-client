package com.github.zzm;

import com.amazonaws.services.s3.model.Bucket;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RadosGatewayS3ClientTest {

    private RadosGatewayS3Client client;

    @Before
    public void setUp() throws Exception {
        client = new RadosGatewayS3Client("ZGV5G0OFFUNESNRBK55Q", "8DSkRch3bnbZHtQqyES9lHH78t7UftLiSdFFzSdP", "ceph-rgw");
    }

    @Test
    public void should_create_bucket_correct() throws Exception {
        Bucket bucket = client.createBucket("bucket1");
        assertThat(bucket.getName(), is("bucket1"));
    }

}