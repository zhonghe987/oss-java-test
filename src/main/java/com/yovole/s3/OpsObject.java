package com.yovole.s3;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import  java.lang.Exception;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.util.StringUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;

public class OpsObject {

    AmazonS3 s3client;

    public OpsObject(AmazonS3 s3client){
        this.s3client = s3client;
    }

    //Check object exist
    public int checkObject(String bucket, String object){
        System.out.println("check object exist Input: "+bucket+" "+object);
        try{
            boolean existObject = s3client.doesObjectExist(bucket, object);
            if (existObject){
                System.out.println("The object is exist.");
            }
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    //PUT /{bucket}/{object}
    public int  uploadObject(String bucket, String object, String path, String acl, String args){
        System.out.println("PUT /{bucket}/{object} Input: "+bucket+" "+object+" "+path+" "+acl);
        ObjectMetadata meta = new ObjectMetadata();
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
        if (args != null){
           String[] argsString = args.split("/");
           for(int i = 0; i< argsString.length; i++){
               String[] kv = argsString[i].split(":");
               meta.addUserMetadata(kv[0], kv[1]);
           }
        }
        try{
           File file = new File(path);
           //InputStream inputStream = new FileInputStream(path);
           if(file != null){
               PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, object, file);
               putObjectRequest.setMetadata(meta);
               s3client.putObject(putObjectRequest.withCannedAcl(acls));
               System.out.println("The object upload successful."+"\n");
           }
        }catch(Exception e){
           e.printStackTrace();
           return -1;
        }
        return 0;
    }
    
   // get object header
   public int getObjectMeta(String bucket,String object){
       System.out.println("get object header: " +bucket+"/"+object+"\n" );
       GetObjectMetadataRequest getMetadataRequest = new GetObjectMetadataRequest(bucket, object);
       try{
       ObjectMetadata objectMetadata =  s3client.getObjectMetadata(getMetadataRequest);
       System.out.println("object size " + objectMetadata.getContentLength());
       System.out.println("object size " + objectMetadata.getContentMD5());
       }catch(Exception e){
         e.printStackTrace();
         return -1;
       }
       return 0;
   }
    
    // reset object http header
    public int resetObjectHeader(String bucket, String object, String args){
        System.out.println("reset object http header:"+bucket+" "+object+" "+args+"\n");
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucket, object, bucket, object);
        ObjectMetadata meta = new ObjectMetadata();
        if (args != null){
           String[] argsString = args.split("/");
           for(int i = 0; i< argsString.length; i++){
               String[] kv = argsString[i].split(":");
               meta.addUserMetadata(kv[0], kv[1]);
           }
        }
        copyObjectRequest.setNewObjectMetadata(meta);
        try{
            s3client.copyObject(copyObjectRequest);
            System.out.println("copy successful"+"\n");
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;   
    }

    //PUT /{dest-bucket}/{dest-object}
    public int copyObject(String source_bucket, String object,
        String dest_bucket, String dest_obj){
        System.out.println("PUT /{dest-bucket}/{dest-object} Input: "+source_bucket+" "
           +object+" "+dest_bucket+" "+dest_obj);
        try{
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                 source_bucket, object, dest_bucket, dest_obj);
            s3client.copyObject(copyObjRequest);
            System.out.println("copy successful"+"\n");
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    // HEAD /{bucket}/{object}
    public int getObjectInfo(String bucket, String object){
        System.out.println("HEAD /{bucket}/{object} Input: "+bucket+" "+object);
        try{
            ObjectMetadata metadata = s3client.getObjectMetadata(bucket, object);
            System.out.println("The object length: "+metadata.getContentLength()
              +"  MD5: "+metadata.getContentMD5()+"\n");
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    //GET /{bucket}/{object}
    public int getObject(String bucket, String object, String path){
        System.out.println("GET /{bucket}/{object} Input: "+bucket+" "+object+" "+path);
        try{
            s3client.getObject(
                 new GetObjectRequest(bucket, object),
                 new File(path));
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    // DELETE /{bucket}/{object}
    public int removeObject(String bucket, String object){
        System.out.println("DELETE /{bucket}/{object} Input: "+bucket+" "+object);
        try{
            s3client.deleteObject(bucket, object);
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    //GET /{bucket}/{object}?acl
    public int getObjectAcl(String bucket, String object){
        System.out.println(" GET /{bucket}/{object}?acl Input: "+bucket+" "+object);
        try{
            AccessControlList acl = s3client.getObjectAcl(bucket, object);
            System.out.println(acl+"\n");
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    //PUT /{bucket}/{object}?acl
    public int setObjectAcl(String bucket, String object,
         String acl){
        System.out.println("PUT /{bucket}/{object}?acl Input: "+bucket+" "+object+" "+acl);
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
            s3client.setObjectAcl(bucket, object, acls);
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
    }


    // POST /{bucket}/{object}?uploads
    public String  initMulitObjectUploadId(String bucket, String object){
        System.out.println("POST /{bucket}/{object}?uploads Input: "+bucket+" "+object);
        InitiateMultipartUploadRequest initUploadRequest = new InitiateMultipartUploadRequest(
              bucket, object);
        InitiateMultipartUploadResult initResult = s3client.initiateMultipartUpload(
              initUploadRequest);
        return initResult.getUploadId();

    }

    // PUT /{bucket}/{object}?partNumber=&uploadId=
    public List<PartETag> putMulitObject(String bucket, String object,
        String uploadId, String path){
        System.out.println("PUT /{bucket}/{object}?partNumber=&uploadId= Input: "+bucket+" "+object+" "+uploadId);
        List<PartETag> partETags = new ArrayList<PartETag>();
        File file = new File(path);
        long contentLength = file.length();
        long partSize = 4 * 1024 * 1024;
        try{
            long compSize = 0;
            for(int i = 1; compSize < contentLength; i++){
            partSize = Math.min(partSize, (contentLength - compSize));
            UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(bucket)
                .withKey(object)
                .withUploadId(uploadId).withPartNumber(i)
                .withFileOffset(compSize).withFile(file).withPartSize(partSize);
            partETags.add(s3client.uploadPart(uploadRequest).getPartETag());
            compSize += partSize;
            }
        }catch(Exception e){
            e.printStackTrace();
            s3client.abortMultipartUpload(new AbortMultipartUploadRequest(
                    bucket, object, uploadId));
            return partETags;
        }
        return partETags;
    }

    // GET /{bucket}/{object}?uploadId=123
    public int listMulitObjectPart(String bucket, String object, String uploadId){
        System.out.println("GET /{bucket}/{object}?uploadId=123 Input: "+bucket+" "+object+" "+uploadId);
        try{
            ListPartsRequest upRequest = new ListPartsRequest(bucket,object,uploadId);
            PartListing listing = s3client.listParts(upRequest);
            for (PartSummary part : listing.getParts()) {
                System.out.println("partnumber:"+part.getPartNumber()+" size:"+
                part.getSize()+" etag:"+part.getETag()+" modify:"+
                part.getLastModified());
            }
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        System.out.println("\n");
        return 0;
    }

    // POST /{bucket}/{object}?uploadId=
    public int compleMulitObject(String bucket, String object,
        String uploadId,  List<PartETag> partETags){
        System.out.println("POST /{bucket}/{object}?uploadId= ");
        try{
            CompleteMultipartUploadRequest compRequest = new
                CompleteMultipartUploadRequest(
                bucket, object, uploadId, partETags);

            s3client.completeMultipartUpload(compRequest);
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        System.out.println("\n");
        return 0;
    }

    //DELETE /{bucket}/{object}?uploadId=
    public int abortMulitObject(String bucket, String object,
        String uploadId){
        System.out.println("DELETE /{bucket}/{object}?uploadId= ");
        try{
            s3client.abortMultipartUpload(new AbortMultipartUploadRequest(
                bucket, object, uploadId));
        }catch(Exception e){
            e.printStackTrace();
            return -1;
        }
        return 0;
   }

   // upload big object
   public int uploadBigObject(String bucket, String object, String path){
       String uploadId = initMulitObjectUploadId(bucket, object);
       try{
           List<PartETag> partETags = putMulitObject(bucket, object, uploadId, path);
           if(partETags.isEmpty()){
              return -1;
           }
           compleMulitObject(bucket, object, uploadId, partETags);
       }catch(Exception e){
           e.printStackTrace();
           abortMulitObject(bucket, object, uploadId);
           return -1;
       }
       return 0;
   }

   // create dir and upload object
   public int createDir(String bucket, String dir){
       System.out.println("create dir  Input: "+bucket+" "+dir);
       try{
           ObjectMetadata dirMetadata = new ObjectMetadata();
           dirMetadata.setContentLength(0);
           String directory = dir;
           s3client.putObject(bucket, directory,
                new ByteArrayInputStream(new byte[0]), dirMetadata);
       }catch(Exception e){
           e.printStackTrace();
           return -1;
       }
       System.out.println("\n");
       return 0;
   }

   //Gener tmp shar url
   public int genTmpUrl(String bucket, String object){
       System.out.println("Gener tmp shar url Input: "+bucket+" "+object);
       try{
           GeneratePresignedUrlRequest urlrequest = new GeneratePresignedUrlRequest(
     	        bucket, object);
           Date expiration = new java.util.Date();
           long msec = expiration.getTime();
           msec += 1000 * 60 * 60; // 1 hour.
           expiration.setTime(msec);
           urlrequest.setExpiration(expiration);
           URL url = s3client.generatePresignedUrl(urlrequest);
           System.out.println(url.toString()+"\n");
       }catch(Exception e){
           e.printStackTrace();
           return -1;
       }
       return 0;
   }

   //List  versionID of any object  in the bucket
   public List<S3VersionSummary> listObjectVersion(String bucket){
       System.out.println("List versionID of any object in the bucket Input: "+bucket);
       List<S3VersionSummary> s3vs = new ArrayList<S3VersionSummary>();
       try{
           String nextMarker = null;
           String nextVersionIdMarker = null;
           ObjectListing objectListing = s3client.listObjects(bucket);
           do {
               ListVersionsRequest req = new ListVersionsRequest();
               req.withBucketName(bucket).withKeyMarker(nextMarker)
                  .withVersionIdMarker(nextVersionIdMarker);
               VersionListing objects = s3client.listVersions(req);
               for (S3VersionSummary s3VersionSummary: objects.getVersionSummaries()) {
                   s3vs.add(s3VersionSummary);
                   System.out.println(s3VersionSummary.getKey()
                      +" versiodId: "+ s3VersionSummary.getVersionId());
               }
               nextMarker = objects.getNextKeyMarker();
               nextVersionIdMarker = objects.getNextVersionIdMarker();
           } while (objectListing.isTruncated());
           return s3vs;
       }catch(Exception e){
           e.printStackTrace();

       }
       System.out.println("\n");
       return s3vs;
   }

   // delete  object with a versionId.
   public int removeObjectWithVersionId(String bucket,
                   String object, String versionId){
       System.out.println("delete  object with a versionId Input: "+bucket+" "+object+" "+versionId);
       try{
           s3client.deleteVersion(bucket, object, versionId);
       }catch(Exception e){
           e.printStackTrace();
           return -1;
       }
       System.out.println("\n");
       return 0;
   }
}
