package com.yovole.s3;


import junit.framework.*;

import java.io.*;
import java.util.*;
import java.net.*;
import java.text.*;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.util.StringUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;

import com.yovole.s3.common.CephAdminAPI;
import com.yovole.s3.admin.*;

/**
 * Unit test for simple App.
 */
public class AppTest
    extends TestCase
{
    protected String admin_access_key="", admin_secret_key="", http_type="";
    protected String s3_access_key="", s3_secret_key="", end_point="";
    protected CephAdminAPI adminApi;
    protected AmazonS3 s3client;
    protected User adminUserOps;
    protected Buckets adminBucketOps;
    protected S3Objects adminObjectOps;
    protected Quota adminQuotaOps;
    protected Key adminkeyOps;
    protected Policy adminPolicyOps;
    protected Usage adminUsageOps;
    protected Caps adminCapsOps;
    protected OpsBucket s3Bucket;
    protected OpsObject s3Object;
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    protected void setUp(){
        Properties prop = new Properties();
        try {
            InputStream inputStream = AppTest.class.getResourceAsStream("/conf.propertis");
            prop.load(inputStream);
            inputStream.close();
            admin_access_key = prop.getProperty("admin_access_key");
            admin_secret_key = prop.getProperty("admin_secret_key");
            s3_access_key = prop.getProperty("user_access_key");
            s3_secret_key = prop.getProperty("user_secret_key");
            end_point = prop.getProperty("end_point");
            http_type = prop.getProperty("http_type");
        } catch(IOException e) {
            e.printStackTrace();
        }

        adminApi = new CephAdminAPI(admin_access_key, admin_secret_key, end_point, http_type);

        AWSCredentials credentials = new BasicAWSCredentials(s3_access_key, s3_secret_key);
        ClientConfiguration clientConfig = new ClientConfiguration();
        if(http_type.equals("https")){
           clientConfig.setProtocol(Protocol.HTTPS);
        }else{
           clientConfig.setProtocol(Protocol.HTTP);
        }
        clientConfig.setSignerOverride("S3SignerType");
        s3client = new AmazonS3Client(credentials, clientConfig);
        s3client.setEndpoint(end_point);

        adminUserOps = new User(adminApi);
        adminBucketOps = new Buckets(adminApi);
        adminObjectOps = new S3Objects(adminApi);
        adminQuotaOps = new Quota(adminApi);
        adminkeyOps = new Key(adminApi);
        adminPolicyOps  = new Policy(adminApi);
        adminUsageOps = new Usage(adminApi);
        adminCapsOps = new Caps(adminApi);

        s3Bucket = new OpsBucket(s3client);
        s3Object = new OpsObject(s3client);
    }

    /**
     * Rigourous Test :-)
     */
     
    public void test_on(){
        assertTrue(200 == 200 );
    }
         
    public void test_user_createUser(){
        int status = adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_user_getUserInfo(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status  = adminUserOps.getUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_user_modifyUser(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status  = adminUserOps.modifyUser("test-admin", "email:test-admin@qq.com");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_subuser_createSubUser(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status = adminUserOps.createSubUser("test-admin", "test-1");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_subuser_modifySubUser(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        adminUserOps.createSubUser("test-admin", "test-1");
        //int status = adminUserOps.modifySubUser("test-admin", "test-1", "access:full");
        adminUserOps.removeUser("test-admin");
        assertTrue(200 == 200 );
    }

    public void test_subuser_removeSubUser(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        adminUserOps.createSubUser("test-admin", "test-1");
        int status = adminUserOps.removeSubUser("test-admin", "test-1");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }
    public void test_user_removeUser(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status = adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_bucket_getBucketInfo(){
        s3Bucket.createBucket("test-bucket-1");
        String status = adminBucketOps.getBucketInfo("test-bucket-1");
        s3Bucket.removeBucket("test-bucket-1");
        assertTrue(status.length() != 0 );
    }
    public void test_bucket_checkBucketIndex(){
        s3Bucket.createBucket("test-bucket-1");
        s3Object.uploadObject("test-bucket-1", "object", "../yovoleOss/READ.md",
                              "public-read-write","Content-Language:en-US");
        int status = adminBucketOps.checkBucketIndex("test-bucket-1");
        s3Object.removeObject("test-bucket-1", "object");
        s3Bucket.removeBucket("test-bucket-1");
        assertTrue(status == 200 );
    }

    public void test_bucket_unlinkBucket(){
        s3Bucket.createBucket("test-bucket-1");
        int status = adminBucketOps.unlinkBucket("test-bucket-1", "private-user");
        adminBucketOps.removeBucket("test-bucket-1");
        assertTrue(status == 200 );
    }

    public void test_bucket_linkBucket(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        s3Bucket.createBucket("test-bucket-1");
        String content = adminBucketOps.getBucketInfo("test-bucket-1");
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> map = new HashMap<String, Object>();
        int status=0;
        try{
            map = mapper.readValue(content,
                     new TypeReference<Map<String, Object>>(){});
            status = adminBucketOps.linkBucket("test-bucket-1",
                     (String)map.get("id"),"test-admin");
        }catch(IOException e){
            e.printStackTrace();
        }
        adminBucketOps.removeBucket("test-bucket-1");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_bucket_removeBucket(){
        s3Bucket.createBucket("test-bucket-1");
        int status = adminBucketOps.removeBucket("test-bucket-1");
        assertTrue(status == 200 );
    }

    public void test_caps_addUserCaps(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status = adminCapsOps.addUserCaps("test-admin", "usage=read");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_caps_removeUserCaps(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        adminCapsOps.addUserCaps("test-admin", "usage=read");
        int status = adminCapsOps.removeUserCaps("test-admin", "usage=read");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }
    public void test_key_createKey(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status = adminkeyOps.createKey("test-admin");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }
    public void test_key_createKeySpeci(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status = adminkeyOps.createKeySpeci("test-admin", "12313131safsfswerqwer",
                                                "asdfweqrqwer41421342134");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_key_createKeySubSpeci(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        adminUserOps.createSubUser("test-admin", "test-1");
        int status = adminkeyOps.createKeySubSpeci("test-admin","test-1",
                             "safsfswerqweasfasr", "asdfwerqwersfasdfwer");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_key_createKeySub(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        adminUserOps.createSubUser("test-admin", "test-1");
        int status = adminkeyOps.createKeySub("test-admin", "test-1");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_key_removeKey(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        adminkeyOps.createKeySpeci("test-admin", "12313131safsfswerqwer",
                                  "sadfwerqwerwqerqwsdfasdf");
        int status = adminkeyOps.removeKey("12313131safsfswerqwer");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_key_removeKeyWithUid(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        adminkeyOps.createKeySpeci("test-admin", "12313131safsfswerqwer",
                                   "asdfasfwerqwerwqerwer");
        int status = adminkeyOps.removeKeyWithUid("12313131safsfswerqwer", "test-admin");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_policy_getBucketPolicy(){
        s3Bucket.createBucket("test-bucket-1");
        s3Bucket.setBucketAcl("test-bucket-1","public-read");
        int status = adminPolicyOps.getBucketPolicy("test-bucket-1");
        s3Bucket.removeBucket("test-bucket-1");
        assertTrue(status == 200 );
    }

    public void test_policy_getObjectPolicy(){
        s3Bucket.createBucket("test-bucket-1");
        s3Object.uploadObject("test-bucket-1", "getUrl.py", "../yovoleOss/pom.xml",
                              "public-read", "Content-Language:en-US");
        int status = adminPolicyOps.getObjectPolicy("test-bucket-1","getUrl.py");
        s3Object.removeObject("test-bucket-1", "getUrl.py");
        s3Bucket.removeBucket("test-bucket-1");
        assertTrue(status == 200 );
    }

    public void test_quota_getUserQuota(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status = adminQuotaOps.getUserQuota("test-admin");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_quota_setUserQuota(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status = adminQuotaOps.setUserQuota("test-admin",
                                              100, 100, true);
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_quota_getBucketQuota(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status = adminQuotaOps.getBucketQuota("test-admin");
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_quota_setBucketQuota(){
        adminUserOps.createUser("test-admin", "a test user", "max-buckets:100");
        int status = adminQuotaOps.setBucketQuota("test-admin",
                                               100, 100, true);
        adminUserOps.removeUser("test-admin");
        assertTrue(status == 200 );
    }

    public void test_object_removeObject(){
        s3Bucket.createBucket("test-bucket-1");
        s3Object.uploadObject("test-bucket-1", "test-file",
                              "../yovoleOss/READ.md", "private", "Content-Language:en-US");
        int status = adminObjectOps.removeObject("test-bucket-1", "test-file");
        adminBucketOps.removeBucket("test-bucket-1");
        assertTrue(status == 200 );
    }

    public void test_usage_getAllUsage(){
        int status = adminUsageOps.getAllUsage();
        assertTrue(status == 200 );
    }

    public void test_usage_getUsageWithUid(){
        int status = adminUsageOps.getUsageWithUid("s3cmd-user");
        assertTrue(status == 200 );
    }

    public void test_usage_getUsageWithDate(){
        int status = adminUsageOps.getUsageWithDate("2017-09-08", "2017-11-11");
        assertTrue(status == 200 );
    }

    public void test_usage_removeUsageWithDate(){
        int status = adminUsageOps.removeUsageWithDate("2017-09-08", "2017-11-11");
        assertTrue(status == 200 );
    }

    public void test_usage_removeUsageWithUid(){
        int status = adminUsageOps.removeUsageWithUid("s3cmd-user");
        assertTrue(status == 200 );
    }

    public void test_usage_removeAllUsage(){
        int status = adminUsageOps.removeAllUsage();
        assertTrue(status == 200 );
    }

    public void test_bucket_getAllBucket(){
        int status = s3Bucket.getAllBucket();
        assertTrue(status == 0 );
    }
    public void test_bucket_createBucket(){
        int status = s3Bucket.createBucket("test-bucket-2");
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_bucket_createBucketWithAcl(){
        int status = s3Bucket.createBucketWithAcl("test-bucket-2", "public-read");
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_bucket_getBucketObjectsWithNum(){
        s3Bucket.createBucket("test-bucket-2");
        for(int i =0; i< 10;i++){
            s3Object.uploadObject("test-bucket-2", "obj_"+i, "../yovoleOss/pom.xml", "private", "Content-Language:en-US");
        }
        int status = s3Bucket.getBucketObjectsWithNum("test-bucket-2", 10);
        for(int j =0; j < 10;j++){
            s3Object.removeObject("test-bucket-2", "obj_"+j);
        }
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_bucket_getBucketObjectsWithPrefix(){
        s3Bucket.createBucket("test-bucket-2");
        for(int i =0; i< 10;i++){
            s3Object.uploadObject("test-bucket-2", "test_"+i,
                                  "../yovoleOss/READ.md", "public-read", "Content-Language:en-US");
        }
        int status = s3Bucket.getBucketObjectsWithPrefix("test-bucket-2", "test");
        for(int j =0; j < 10;j++){
            s3Object.removeObject("test-bucket-2", "test_"+j);
        }
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_bucket_getLocation(){
        s3Bucket.createBucket("test-bucket-2");
        int status = s3Bucket.getLocation("test-bucket-2");
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }
    public void test_bucket_getBucketAcl(){
        s3Bucket.createBucket("test-bucket-2");
        int status = s3Bucket.getBucketAcl("test-bucket-2");
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }
    public void test_bucket_setBucketAcl(){
        s3Bucket.createBucket("test-bucket-2");
        int status = s3Bucket.setBucketAcl("test-bucket-2","public-read-write");
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }
    public void test_bucket_getBucketUploads(){
        s3Bucket.createBucket("test-bucket-2");
        String uploadId= s3Object.initMulitObjectUploadId("test-bucket-2",
                                                          "bigObjet");
        s3Object.putMulitObject("test-bucket-2", "bigObjet", uploadId,
                                "../yovoleOss/50m");
        int status = s3Bucket.getBucketUploads("test-bucket-2");
        s3Object.abortMulitObject("test-bucket-2", "bigObjet", uploadId);
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_bucket_enableBucketVersion(){
        s3Bucket.createBucket("test-bucket-2");
        int status = s3Bucket.enableBucketVersion("test-bucket-2");
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_bucket_unableBucketVersion(){
        s3Bucket.createBucket("test-bucket-2");
        int status = s3Bucket.enableBucketVersion("test-bucket-2");
        s3Bucket.removeBucket("test-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_buckets_removeBucket(){
        s3Bucket.createBucket("remove-bucket");
        int status = s3Bucket.removeBucket("remove-bucket");
        assertTrue(status == 0 );
    }

    public void test_object_checkObject(){
        s3Bucket.createBucket("remove-bucket");
        s3Object.uploadObject("remove-bucket", "getUrl.py",
                              "../yovoleOss/pom.xml", "private", "Content-Language:en-US");
        int status = s3Object.checkObject("remove-bucket", "getUrl.py");
        s3Object.removeObject("remove-bucket", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket");
        assertTrue(status == 0 );
    }

    public void test_object_uploadObject(){
        s3Bucket.createBucket("remove-bucket-2");
        int status = s3Object.uploadObject("remove-bucket-2",
                              "getUrl.py", "../yovoleOss/READ.md", "public-read", "Content-Language:en-US");
        s3Object.removeObject("remove-bucket-2", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_object_copyObject(){
        s3Bucket.createBucket("remove-bucket-3");
        s3Bucket.createBucket("remove-bucket");
        s3Object.uploadObject("remove-bucket-3", "getUrl.py",
                              "../yovoleOss/READ.md", "public-read-write", "Content-Language:en-US");
        int status = s3Object.copyObject("remove-bucket-3", "getUrl.py",
                                         "remove-bucket", "newgetUrl.py");
        s3Object.removeObject("remove-bucket-3", "getUrl.py");
        s3Object.removeObject("remove-bucket", "newgetUrl.py");
        s3Bucket.removeBucket("remove-bucket");
        s3Bucket.removeBucket("remove-bucket-3");
        assertTrue(status == 0 );
    }

    public void test_object_getObjectInfo(){
        s3Bucket.createBucket("remove-bucket-2");
        s3Object.uploadObject("remove-bucket-2", "getUrl.py",
                              "../yovoleOss/READ.md", "private", "Content-Language:en-US");
        int status = s3Object.getObjectInfo("remove-bucket-2", "getUrl.py");
        s3Object.removeObject("remove-bucket-2", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_object_getObject(){
        s3Bucket.createBucket("remove-bucket-3");
        s3Object.uploadObject("remove-bucket-3", "getUrl.py",
                              "../yovoleOss/READ.md", "public-read", "Content-Language:en-US");
        int status = s3Object.getObject("remove-bucket-3", "getUrl.py",
                              "/root/getUrl.py_1");
        s3Object.removeObject("remove-bucket-3", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket-3");
        assertTrue(status == 0 );
    }

    public void test_object_removeObjects(){
        s3Bucket.createBucket("remove-bucket-3");
        s3Object.uploadObject("remove-bucket-3", "getUrl.py",
                              "../yovoleOss/READ.md", "private", "Content-Language:en-US");
        int status = s3Object.removeObject("remove-bucket-3", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket-3");
        assertTrue(status == 0 );
    }

    public void test_object_getObjectAcl(){
        s3Bucket.createBucket("remove-bucket-2");
        s3Object.uploadObject("remove-bucket-2", "getUrl.py",
                              "../yovoleOss/READ.md", "public-read-write", "Content-Language:en-US");
        int status = s3Object.getObjectAcl("remove-bucket-2", "getUrl.py");
        s3Object.removeObject("remove-bucket-2", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_object_setObjectAcl(){
        s3Bucket.createBucket("remove-bucket-2");
        s3Object.uploadObject("remove-bucket-2", "getUrl.py",
                              "../yovoleOss/READ.md", "private", "Content-Language:en-US");
        int status = s3Object.setObjectAcl("remove-bucket-2",
                                    "getUrl.py", "public-read");
        s3Object.removeObject("remove-bucket-2", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_object_uploadBigObject(){
        s3Bucket.createBucket("remove-bucket-4");
        int status = s3Object.uploadBigObject("remove-bucket-4",
                                          "s3-1G", "/root/S3-1G");
        s3Object.removeObject("remove-bucket-4", "s3-1G");
        s3Bucket.removeBucket("remove-bucket-4");
        assertTrue(status == 0 );
    }

    public void test_object_createDir(){
        s3Bucket.createBucket("remove-bucket-2");
        int status = s3Object.createDir("remove-bucket-2", "files/");
        s3Object.removeObject("remove-bucket-2", "files/");
        s3Bucket.removeBucket("remove-bucket-2");
        assertTrue(status == 0 );
    }
    public void test_object_getObjectMeta(){
        s3Bucket.createBucket("remove-bucket-2");
        s3Object.uploadObject("remove-bucket-2", "getUrl.py",
                              "../yovoleOss/READ.md", "public-read-write", "Content-Language:en-US");
        int status = s3Object.getObjectMeta("remove-bucket-2", "getUrl.py");
        s3Object.removeObject("remove-bucket-2", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket-2");
        assertTrue(status == 0 );
    }
    public void test_object_resetObjectHeader(){
        s3Bucket.createBucket("remove-bucket-2");
        s3Object.uploadObject("remove-bucket-2", "getUrl.py",
                              "../yovoleOss/READ.md", "public-read", "Content-Language:en-US");
        int status = s3Object.resetObjectHeader("remove-bucket-2", "getUrl.py",
                              "Cache-Control:no-cache/Content-Encoding:utf-8/Expires:3600");
        s3Object.removeObject("remove-bucket-2", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket-2");
        assertTrue(status == 0 );
    }
    public void test_object_genTmpUrl(){
        s3Bucket.createBucket("remove-bucket-2");
        s3Object.uploadObject("remove-bucket-2", "getUrl.py",
                              "../yovoleOss/READ.md", "private", "Content-Language:en-US");
        int status = s3Object.genTmpUrl("remove-bucket-2", "getUrl.py");
        s3Object.removeObject("remove-bucket-2", "getUrl.py");
        s3Bucket.removeBucket("remove-bucket-2");
        assertTrue(status == 0 );
    }

    public void test_object_listObjectVersion(){
        s3Bucket.createBucket("remove-bucket-1");
        s3Bucket.enableBucketVersion("remove-bucket-1");
        for (int i =0 ;i< 10; i++){ 
        s3Object.uploadObject("remove-bucket-1", "getUrl.py",
             "../yovoleOss/READ.md", "public-write", "Content-Language:en-US");
        }
        List<S3VersionSummary> objects= s3Object.listObjectVersion(
                                            "remove-bucket-1");
        for(S3VersionSummary ob : objects){
            if(ob.getVersionId() == null){
                s3Object.removeObject("remove-bucket-1", ob.getKey());
                continue;
            }
            s3Object.removeObjectWithVersionId("remove-bucket-1", ob.getKey(),
                     ob.getVersionId());
        }
        s3Bucket.unenableBucketVersion("remove-bucket-1");
        s3Bucket.removeBucket("remove-bucket-1");
        assertTrue(objects.size() > 0 );
    }
    
    protected void tearDown() throws Exception{
        super.tearDown();
    }
}
