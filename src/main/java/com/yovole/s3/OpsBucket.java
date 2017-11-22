package com.yovole.s3;

import java.util.List;
import java.lang.Exception;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.util.StringUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;

public class OpsBucket {
    AmazonS3 s3client;

    public OpsBucket(AmazonS3 s3client){
        this.s3client = s3client;
    }

    // GET /

    public int getAllBucket(){
        List<Bucket> buckets = s3client.listBuckets();
        try{
            for (Bucket buckete : buckets) {
                System.out.println(buckete.getName() + "\t" +
                     StringUtils.fromDate(buckete.getCreationDate()));
            }
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    // PUT /{bucket}
    public int createBucket(String bucket){
        try{
            System.out.println("PUT /{bucket} Input: "+bucket);
            Bucket new_bucket = s3client.createBucket(bucket);
            String info = new_bucket.toString();
            System.out.println(info);
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public int createBucketWithAcl(String bucket, String acl){
        try{
            CannedAccessControlList acls = CannedAccessControlList.Private;
            if(acl.equals("private")){
                acls = CannedAccessControlList.Private;
            }else if(acl.equals("public-read")){
                acls = CannedAccessControlList.PublicRead;
            }else if(acl.equals("public-read-write")){
                acls = CannedAccessControlList.PublicReadWrite;
            }else if(acl.equals("authenticated-read")){
                acls = CannedAccessControlList.AuthenticatedRead;
            }
            System.out.println("PUT /{bucket} Input: "+bucket+" "+acl);
            Bucket new_bucket = s3client.createBucket(new CreateBucketRequest(bucket)
                                 .withCannedAcl(acls));
            String info = new_bucket.toString();
            System.out.println(info+"\n");
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    // GET /{bucket}?max-keys=25
    public int getBucketObjectsWithNum(String bucket, int nums){
        try{
            System.out.println("GET /{bucket}?max-keys= Input: "+bucket+" "+nums);
            ListObjectsRequest  objRequest = new ListObjectsRequest(bucket, null,null,null,nums);
            ObjectListing objects = s3client.listObjects(objRequest);
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()){
                System.out.println("object: "+ objectSummary.getKey()+"\n");
            }
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public int getBucketObjectsWithPrefix(String bucket, String prefix){
        System.out.println("GET /{bucket}?prefix= Input: "+bucket+" "+prefix);
        try{
            ObjectListing objects = s3client.listObjects(bucket, prefix);
            for (S3ObjectSummary objectSummary : objects.getObjectSummaries()){
                System.out.println("object: "+ objectSummary.getKey());
            }
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        System.out.println("\n");
        return 0;
    }

    // GET /{bucket}?location
    public int getLocation(String bucket){
        System.out.println("GET /{bucket}?location Input: "+bucket);
        try{
            String location = s3client.getBucketLocation(bucket);
            System.out.println("location: "+location+"\n");
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    // GET /{bucket}?acl
    public int getBucketAcl(String bucket){
        System.out.println("GET /{bucket}?acl Input: "+bucket);
        try{
            AccessControlList ac = s3client.getBucketAcl(bucket);
            String acl = ac.toString();
            System.out.println(acl+"\n");
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    // PUT /{bucket}?acl
    public int setBucketAcl(String bucket, String acl){
        System.out.println("PUT /{bucket}?acl Input: "+bucket+" "+acl);
        try{
            CannedAccessControlList acls = CannedAccessControlList.Private;
            if(acl.equals("private")){
                acls = CannedAccessControlList.Private;
            }else if(acl.equals("public-read")){
                acls = CannedAccessControlList.PublicRead;
            }else if(acl.equals("public-read-write")){
                acls = CannedAccessControlList.PublicReadWrite;
            }else if(acl.equals("authenticated-read")){
                acls = CannedAccessControlList.AuthenticatedRead;
            }
            s3client.setBucketAcl(bucket,  acls);
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    // GET /{bucket}?uploads
    public int getBucketUploads(String bucket){
        System.out.println("GET /{bucket}?uploads Input: "+bucket);
        try{
            ListMultipartUploadsRequest listMultipartUploadsRequest = new ListMultipartUploadsRequest(bucket);
            MultipartUploadListing multipartUploadListing = s3client.listMultipartUploads(listMultipartUploadsRequest);
            for(MultipartUpload multipartUpload : multipartUploadListing.getMultipartUploads()) {
                System.out.println("uploadId: "+multipartUpload.getUploadId()+" key: "+
                    multipartUpload.getKey());
            }
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        System.out.println("\n");
        return 0;
    }

    // PUT  /{bucket}?versioning
    public int enableBucketVersion(String bucket){
        System.out.println("PUT  /{bucket}?versioning Input: "+bucket);
        try{
            BucketVersioningConfiguration versioningConfiguration = new BucketVersioningConfiguration();
            versioningConfiguration.setStatus(BucketVersioningConfiguration.ENABLED);
            SetBucketVersioningConfigurationRequest request = new SetBucketVersioningConfigurationRequest(
                   bucket,versioningConfiguration);
            s3client.setBucketVersioningConfiguration(request);
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    public int unenableBucketVersion(String bucket){
        System.out.println("PUT  /{bucket}?versioning Input: "+bucket);
        try{
            BucketVersioningConfiguration versioningConfiguration = new BucketVersioningConfiguration();
            versioningConfiguration.setStatus(BucketVersioningConfiguration.SUSPENDED);
            SetBucketVersioningConfigurationRequest request = new SetBucketVersioningConfigurationRequest(
                     bucket,versioningConfiguration);
            s3client.setBucketVersioningConfiguration(request);
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    //DELETE /{bucket}
    public int removeBucket(String bucket){
        System.out.println("DELETE /{bucket}  Input: "+bucket);
        try{
            s3client.deleteBucket(bucket);
            if (!s3client.doesBucketExist(bucket)){
                System.out.println("the bucket is delete");
            }
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }
}
