package com.yovole.s3;

import java.io.*;
import java.util.*;
import java.net.*;
import java.text.*;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONArray;

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


public class App
{
    static void usage(){
        System.out.println("java -jar target/yovoleOss-0.1.jar <cmd> [options]");
        System.out.println("admin commands:");
        System.out.println("  admin user createUser <uid> <display-name> <metadata>"
                           +"                   create a user. metadata:<key:value/key:value>");
        System.out.println("  admin user getUser <uid>  "
                           +"                                                 get user info");
        System.out.println("  admin user modifyUser <uid> <metadata>"
                           +"                                   mofify a user. metadata: <key:value/key:value>");
        System.out.println("  admin user removeUser <uid>                    "
                           +"                            delete a user");
        System.out.println("  admin subuser createSubUser <uid> <subuser>    "
                           +"                            create a sub user");
        System.out.println("  admin subuser modifySubUser <uid> <subuser> <metadata>  "
                           +"                    mofify a sub user. metadata: <key:value/key:value>");
        System.out.println("  admin subuser removeSubUser <uid> <subuser>    "
                           +"                            remove a sub user");
        System.out.println("  admin bucket getBucketInfo <bucket-name>       "
                           +"                            get a bucket info");
        System.out.println("  admin bucket checkBucketIndex <bucket-name>    "
                           +"                            check a bucket object index");
        System.out.println("  admin bucket unlinkBucket <bucket-name> <uid>  "
                           +"                            unlink a bucket from a user");
        System.out.println("  admin bucket removeBucket <bucket-name>        "
                           +"                            remove a bucket");
        System.out.println("  admin object removeObject <bucket-name> <object-name> "
                           +"                     remove a object");
        System.out.println("  admin quota getUserQuota <uid>                 "
                           +"                            get a user type quota with a user");
        System.out.println("  admin quota setUserQuota <uid> <kb> <objnums> <enabled>  "
                           +"                  set a user type quota with a user");
        System.out.println("  admin quota getBucketQuota <uid>                "
                           +"                           get a bucket type quota with a user");
        System.out.println("  admin quota setBucketQuota <uid> <kb> <objnums> <enabled>  "
                           +"                set a bucket type quota with a user");
        System.out.println("  admin policy getBucketPolicy <bucket-name>        "
                           +"                         get a bucket policy");
        System.out.println("  admin policy getObjectPolicy <bucket-name> <obj-name> "
                           +"                     get a object policy");
        System.out.println("  admin key createKey <uid>                        "
                           +"                          create a key with a user");
        System.out.println("  admin key createKeySpeci <uid> <access-key> <secret>  "
                           +"                     create a key for a user with a specifle access-key");
        System.out.println("  admin key createKeySubSpeci <uid> <subuser> <access-key> <secret>"
                           +"          create a key for a subuser with a specifle key");
        System.out.println("  admin key createKeySub <uid> <subuser>          "
                           +"                           create a key for a subuser");
        System.out.println("  admin key removeKey <access-key>                "
                           +"                           remove a key");
        System.out.println("  admin key removeKeyWithUid <access-key> <uid>   "
                           +"                           remove a key with a user");
        System.out.println("  admin usage getAllUsage                         "
                           +"                           get all usage");
        System.out.println("  admin usage getUsageWithUid <uid>               "
                           +"                           get  usage about a user");
        System.out.println("  admin usage getUsageWithDate <start_date>  <end_date>"
                           +"                   get  usage with start date");
        System.out.println("  admin usage removeUsageWithDate <start_date> <end_date>"
                           +"                   remove a usage with a start time");
        System.out.println("  admin usage removeUsageWithUid <uid>            "
                           +"                        remove a usage about a user");
        System.out.println("  admin usage removeAllUsage                      "
                           +"                           remove all usage");
        System.out.println("  admin caps addUserCaps <uid> <caps>             "
                           +"                           add caps with a user");
        System.out.println("  admin caps removeUserCaps <uid> <caps>          "
                           +"                           remove caps with a user");
        System.out.println("S3 commands:");
        System.out.println("  s3 bucket createBucket <bucket-name>            "
                           +"                           create a bucekt");
        System.out.println("  s3 bucket getAllBucket                          "
                           +"                           list a bucekt");
        System.out.println("  s3 bucket createBucketWithAcl <bucket-name> <acl>"
                           +"                          create a bucekt");
        System.out.println("  s3 bucket getBucketObjectsWithNum <bucket-name> <nums>"
                           +"                     get {nums} object in a bucekt");
        System.out.println("  s3 bucket getBucketObjectsWithPrefix <>bucket-name <prefix>"
                           +"                get object with prefix in a bucekt");
        System.out.println("  s3 bucket getLocation <bucket-name>             "
                           +"                           get a bucekt location");
        System.out.println("  s3 bucket getBucketAcl <bucket-name>            "
                           +"                           get a bucekt acl");
        System.out.println("  s3 bucket setBucketAcl <bucket-name> <acl>      "
                           +"                           set a bucekt acl");
        System.out.println("  s3 bucket getBucketUploads <bucket-name>        "
                           +"                           get upload mulitpart in a bucekt");
        System.out.println("  s3 bucket enableBucketVersion <bucket-name>     "
                           +"                           enable bucekt's version controll");
        System.out.println("  s3 bucket unenableBucketVersion <bucket-name>   "
                           +"                           unenable bucekt's version controll");
        System.out.println("  s3 bucket removeBucket <bucket-name>           "
                           +"                            remove a bucekt");
        System.out.println("  s3 object uploadObject <bucket> <object> <filepath> <acl> <metadata>"
                           +"        upload a object into bucket. metadata: <key:value/key:value>");
        System.out.println("  s3 object copyObject <sour-bucket> <object> <dest-bucket> <dest-object>"
                           +"    copy a object into other bucket or curent bucket");
        System.out.println("  s3 object getObjectInfo <bucket-name> <object>"
                           +"                             get a object info");
        System.out.println("  s3 object getObject <bucket-name> <object> <filepath>"
                           +"                      download a object");
        System.out.println("  s3 object getObjectMeta <bucket-name> <object>"
                           +"                      get object http header");
        System.out.println("  s3 object resetObjectHeader <bucket-name> <object> <metadata>"
                           +"           reset object http header. <metadata> = <key:value/key:value>");
        System.out.println("  s3 object removeObject <bucket-name> <object>"
                           +"                              remove a object");
        System.out.println("  s3 object getObjectAcl <bucket-name> <object>"
                           +"                              get a object acl");
        System.out.println("  s3 object setObjectAcl <bucket-name> <object> <acl>"
                           +"                        set a object acl");
        System.out.println("  s3 object uploadBigObject <bucket-name> <object> <filepath>"
                           +"                mulit part upload a object into bucket");
        System.out.println("  s3 object createDir <bucket-name> <dir>"
                           +"                                    create a dir");
        System.out.println("  s3 object genTmpUrl <bucket-anem> <object>"
                           +"                                 create a tmp shar url with a object");
        System.out.println("  s3 object listObjectVersion <bucket-name>"
                           +"                                  list all object's versionId");
        System.out.println("  s3 object removeObjectWithVersionId <bucket-name> <object> <versionId>"
                           +"     remove object with a specifile versionId");
    }

    @SuppressWarnings("deprecation")
    public static void main( String[] args )
    {
        String admin_access_key="", admin_secret_key="";
        String s3_access_key="", s3_secret_key="";
        String end_point="", http_type = "";
        InputStream inputStream = null;
        Properties prop = new Properties();
        try {
            inputStream = App.class.getResourceAsStream("/conf.propertis");
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
        if(admin_access_key==""||admin_secret_key==""
           ||s3_access_key==""||s3_secret_key==""||end_point==""){
           System.out.println("Can not parse conf");
        }

        CephAdminAPI adminApi = new CephAdminAPI(admin_access_key,
                                      admin_secret_key, end_point, http_type);

        AWSCredentials credentials = new BasicAWSCredentials(s3_access_key, s3_secret_key);
        ClientConfiguration clientConfig = new ClientConfiguration();
        if(http_type.equals("https")){
           clientConfig.setProtocol(Protocol.HTTPS);
        }else{
           clientConfig.setProtocol(Protocol.HTTP);
        }
        clientConfig.setSignerOverride("S3SignerType");
        AmazonS3 s3client = new AmazonS3Client(credentials, clientConfig);
        s3client.setEndpoint(end_point);

        User adminUserOps = new User(adminApi);
        Buckets adminBucketOps = new Buckets(adminApi);
        S3Objects adminObjectOps = new S3Objects(adminApi);
        Quota adminQuotaOps = new Quota(adminApi);
        Key adminkeyOps = new Key(adminApi);
        Policy adminPolicyOps  = new Policy(adminApi);
        Usage adminUsageOps = new Usage(adminApi);
        Caps adminCapsOps = new Caps(adminApi);

        OpsBucket s3Bucket = new OpsBucket(s3client);
        OpsObject s3Object = new OpsObject(s3client);
        String cmd = "";
        for(int i=0; i < args.length; i++){
           cmd = cmd +" "+args[i];
        }
        System.out.println(cmd);
        if(args[0].equals("admin") && args[1].equals("user")){
            if(args[2].equals("createUser")){
                String args_5 = null;
                if(args.length > 5){
                    args_5 = args[5];
                }
                adminUserOps.createUser(args[3], args[4], args_5);
            }else if (args[2].equals("getUser")){
        	adminUserOps.getUser(args[3]);
            }else if(args[2].equals("modifyUser")){
                adminUserOps.modifyUser(args[3], args[4]);
            }else if(args[2].equals("removeUser")){
                adminUserOps.removeUser(args[3]);
            }
        }else if(args[0].equals("admin") && args[1].equals("subuser")){
            if(args[2].equals("createSubUser")){
                adminUserOps.createSubUser(args[3], args[4]);
            }else if(args[2].equals("modifySubUser")){
                adminUserOps.modifySubUser(args[3], args[4], args[5]);
            }else if(args[2].equals("removeSubUser") ){
                adminUserOps.removeSubUser(args[3],args[4]);
            }
        }else if(args[0].equals("admin") && args[1].equals("bucket")){
            if(args[2].equals("getBucketInfo")){
                adminBucketOps.getBucketInfo(args[3]);
            }else if(args[2].equals("checkBucketIndex")){
                adminBucketOps.checkBucketIndex(args[3]);
            }else if(args[2].equals("unlinkBucket")){
                adminBucketOps.unlinkBucket(args[3],args[4]);
            }else if(args[2].equals("linkBucket")){
                adminBucketOps.linkBucket(args[3],args[4],args[5]);
            }else if(args[2].equals("removeBucket")){
                adminBucketOps.removeBucket(args[3]);
            }
        }else if(args[0].equals("admin") && args[1].equals("object")){
            if(args[2].equals("removeObject")){
                adminObjectOps.removeObject(args[3], args[4]);
            }
        }else if(args[0].equals("admin") && args[1].equals("policy")){
            if(args[2].equals("getBucketPolicy")){
                adminPolicyOps.getBucketPolicy(args[3]);
            }else if(args[2].equals( "getObjectPolicy")){
                adminPolicyOps.getObjectPolicy(args[3], args[4]);
            }
        }else if(args[0].equals("admin") && args[1].equals("quota")){
            if(args[2].equals("getUserQuota")){
                adminQuotaOps.getUserQuota(args[3]);
            }else if(args[2].equals("setUserQuota")){
                adminQuotaOps.setUserQuota(args[3], Integer.parseInt(args[4]),
                    Integer.parseInt(args[5]), Boolean.parseBoolean(args[6]));
            }else if(args[2].equals("getBucketQuota")){
                adminQuotaOps.getBucketQuota(args[3]);
            }else if(args[2].equals("setBucketQuota")){
                adminQuotaOps.setBucketQuota(args[3], Integer.parseInt(args[4]),
                    Integer.parseInt(args[5]), Boolean.parseBoolean(args[6]));
            }
        }else if(args[0].equals("admin") && args[1].equals("key")){
            if(args[2].equals("createKey")){
                adminkeyOps.createKey(args[3]);
            }else if(args[2].equals("createKeySpeci")){
                adminkeyOps.createKeySpeci(args[3], args[4], args[5]);
            }else if(args[2].equals("createKeySubSpeci")){
                adminkeyOps.createKeySubSpeci(args[3], args[4], args[5], args[6]);
            }else if(args[2].equals("createKeySub")){
                adminkeyOps.createKeySub(args[3], args[4]);
            }else if(args[2].equals("removeKey")){
                adminkeyOps.removeKey(args[3]);
            }else if(args[2].equals("removeKeyWithUid")){
                adminkeyOps.removeKeyWithUid(args[3], args[4]);
            }
        }else if(args[0].equals("admin") && args[1].equals("usage")){
            if(args[2].equals("getAllUsage")){
                adminUsageOps.getAllUsage();
            }else if(args[2].equals("getUsageWithUid")){
                adminUsageOps.getUsageWithUid(args[3]);
            }else if(args[2].equals("getUsageWithDate")){
                adminUsageOps.getUsageWithDate(args[3], args[4]);
            }else if(args[2].equals("removeUsageWithDate")){
                adminUsageOps.removeUsageWithDate(args[3], args[4]);
            }else if(args[2].equals("removeUsageWithUid")){
                adminUsageOps.removeUsageWithUid(args[3]);
            }else if(args[2].equals("removeAllUsage")){
                adminUsageOps.removeAllUsage();
            }
        }else if(args[0].equals("admin") && args[1].equals("caps")){
            if(args[2].equals("addUserCaps")){
                adminCapsOps.addUserCaps(args[3],args[4]);
            }else if(args[2].equals("removeUserCaps")){
                adminCapsOps.removeUserCaps(args[3],args[4]);
            }
        }else if (args[0].equals("s3") && args[1].equals("bucket")){
            if(args[2].equals("getAllBucket")){
                s3Bucket.getAllBucket();
            }else if(args[2].equals("createBucket")){
                s3Bucket.createBucket(args[3]);
            }else if(args[2].equals("createBucketWithAcl")){
                s3Bucket.createBucketWithAcl(args[3],args[4]);
            }else if(args[2].equals("getBucketObjectsWithNum")){
                s3Bucket.getBucketObjectsWithNum(args[3],Integer.parseInt(args[4]));
            }else if(args[2].equals("getBucketObjectsWithPrefix")){
                s3Bucket.getBucketObjectsWithPrefix(args[3], args[4]);
            }else if(args[2].equals("getLocation")){
                s3Bucket.getLocation(args[3]);
            }else if(args[2].equals("getBucketAcl")){
                s3Bucket.getBucketAcl(args[3]);
            }else if(args[2].equals("setBucketAcl")){
                s3Bucket.setBucketAcl(args[3], args[4]);
            }else if(args[2].equals("getBucketUploads")){
                s3Bucket.getBucketUploads(args[3]);
            }else if(args[2].equals("enableBucketVersion")){
                s3Bucket.enableBucketVersion(args[3]);
            }else if(args[2].equals("unenableBucketVersion")){
                s3Bucket.unenableBucketVersion(args[3]);
            }else if(args[2].equals("removeBucket")){
                s3Bucket.removeBucket(args[3]);
            }
        }else if(args[0].equals("s3") && args[1].equals("object")){
            if(args[2].equals("uploadObject")){
                String acl = "";
                String meta = null;
                if(args.length > 6 ){
                   acl = args[6];
                }
                if (args.length> 7){
                   meta = args[7];
                }
                s3Object.uploadObject(args[3], args[4], args[5], acl, meta);
            }else if(args[2].equals("getObjectMeta")){
                s3Object.getObjectMeta(args[3], args[4]);
            }else if(args[2].equals("resetObjectHeader")){
                s3Object.resetObjectHeader(args[3], args[4], args[5]);
            }else if(args[2].equals("checkObject")){
                s3Object.checkObject(args[3], args[4]);
            }else if(args[2].equals("copyObject")){
                s3Object.copyObject(args[3],args[4],args[5],args[6]);
            }else if(args[2].equals("getObjectInfo")){
                s3Object.getObjectInfo(args[3],args[4]);
            }else if(args[2].equals("getObject")){
                s3Object.getObject(args[3],args[4],args[5]);
            }else if(args[2].equals("removeObject")){
                s3Object.removeObject(args[3],args[4]);
            }else if(args[2].equals("getObjectAcl")){
                s3Object.getObjectAcl(args[3],args[4]);
            }else if(args[2].equals("setObjectAcl")){
                s3Object.setObjectAcl(args[3],args[4],args[5]);
            }else if(args[2].equals("uploadBigObject")){
                s3Object.uploadBigObject(args[3],args[4], args[5]);
            }else if(args[2].equals("createDir")){
                s3Object.createDir(args[3],args[4]);
            }else if(args[2].equals("genTmpUrl")){
                s3Object.genTmpUrl(args[3],args[4]);
            }else if(args[2].equals("listObjectVersion")){
                s3Object.listObjectVersion(args[3]);
            }else if(args[2].equals("removeObjectWithVersionId")){
                s3Object.removeObjectWithVersionId(args[3], args[4], args[5]);
            }
        }else if(args[0].equals("-h")){
            usage();
        }
    }
}
