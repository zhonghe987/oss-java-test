package com.yovole.s3.admin;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

import com.yovole.s3.common.CephAdminAPI;

public class Policy {

    CephAdminAPI adminApi;

    public Policy(CephAdminAPI api){
        this.adminApi = api;
    }

    //GET /{admin}/bucket?policy&format=json
    public int getBucketPolicy(String bucket){
        System.out.println("GET /{admin}/bucket?policy Input: "+bucket);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("bucket", bucket);
        response = adminApi.execute("GET", "/admin/bucket", "policy", requestArgs, null);
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

    //GET /{admin}/bucket?policy&format=json
    public int getObjectPolicy(String bucket, String object){
        System.out.println("GET /{admin}/bucket?policy Input: "+bucket+" "+object);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("bucket", bucket);
        requestArgs.put("object", object);
        response = adminApi.execute("GET", "/admin/bucket", "policy", requestArgs, null);
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
