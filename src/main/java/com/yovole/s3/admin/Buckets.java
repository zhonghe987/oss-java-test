package com.yovole.s3.admin;

import java.io.*;
import java.util.*;
import java.net.*;
import java.text.*;
//import java.util.Map.Entry;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONArray;

import com.yovole.s3.common.CephAdminAPI;
public class Buckets {
    CephAdminAPI adminApi;

    public Buckets(CephAdminAPI api){
        this.adminApi = api;
    }

    //GET /{admin}/bucket?format=json
    public String getBucketInfo(String bucket){
        System.out.println("GET /{admin}/bucket Input: "+bucket);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("bucket", bucket);
        response = adminApi.execute("GET", "/admin/bucket", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            String content = EntityUtils.toString(entity, "UTF-8");
            System.out.println("\nResponse Content is: "
                        + content + "\n");
            response.close();
            return content;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return "";
       }
    }

    //GET /{admin}/bucket?index&format=json
    public int checkBucketIndex(String bucket){
        System.out.println("GET /{admin}/bucket?index Input: "+bucket);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("bucket", bucket);
        requestArgs.put("check-objects", "True");
        response = adminApi.execute("GET", "/admin/bucket", "index", requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            String content  = EntityUtils.toString(entity, "UTF-8");
            System.out.println("\nResponse Content is: "
                        + content + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }
    }

    //POST /{admin}/bucket?format=json
    public int unlinkBucket(String bucket, String uid){
        System.out.println("POST /{admin}/bucket Input: "+bucket+" "+uid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("bucket", bucket);
        requestArgs.put("uid", uid);
        response = adminApi.execute("POST", "/admin/bucket", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("\nResponse Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }
    }

    //PUT /{admin}/bucket?format=json
    public int linkBucket(String bucket,String bucket_id, String user){
        System.out.println("PUT /{admin}/bucket Input: "+bucket+" "+bucket_id+" "+user);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("bucket", bucket);
        requestArgs.put("bucket-id", bucket_id);
        requestArgs.put("uid", user);
        response = adminApi.execute("PUT", "/admin/bucket", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("\nResponse Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }
    }

    //DELETE /{admin}/bucket?format=json
    public int removeBucket(String bucket){
        System.out.println("DELETE /{admin}/bucket Input: "+bucket);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("bucket", bucket);
        requestArgs.put("purge-objects", true);
        response = adminApi.execute("DELETE", "/admin/bucket", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("\nResponse Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }
    }
}
