package com.github.zzm;

import com.amazonaws.services.s3.model.Bucket;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RadosGatewayS3ClientTest {

    private static final String ACCESS_KEY = "ZGV5G0OFFUNESNRBK55Q";
    private static final String SECRET_KEY = "8DSkRch3bnbZHtQqyES9lHH78t7UftLiSdFFzSdP";
    private static final String HOSTNAME = "http://192.168.42.2:80";
    private RadosGatewayS3Client client;

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

}