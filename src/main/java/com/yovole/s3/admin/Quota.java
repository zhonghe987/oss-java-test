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
public class Quota {
    CephAdminAPI adminApi;

    public Quota(CephAdminAPI api){
        this.adminApi = api;
    }

    //GET /admin/user?quota&uid=<uid>&quota-type=user
    public int getUserQuota(String uid){
        System.out.println("GET /admin/user?quota&uid=<uid>&quota-type=user Input: "+uid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("quota-type", "user");
        response = adminApi.execute("GET", "/admin/user", "quota", requestArgs, null);
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

    //PUT /admin/user?quota&uid=<uid>&quota-type=user
    public int setUserQuota(String uid, int kb, int obs, boolean enable){
        System.out.println("PUT /admin/user?quota&uid=<uid>&quota-type=user Input: "+uid
           +" "+kb+" "+obs+" "+enable);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("quota-type", "user");
        String body = "{\"enabled\":"+enable+",\"max_size_kb\":"+kb+",\"max_objects\":"+obs+"}";
        response = adminApi.execute("PUT", "/admin/user", "quota", requestArgs, body);
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

    //GET /admin/user?quota&uid=<uid>&quota-type=bucket
    public int getBucketQuota(String uid){
        System.out.println("GET /admin/user?quota&uid=<uid>&quota-type=bucket Input: "+uid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("quota-type", "bucket");
        response = adminApi.execute("GET", "/admin/user", "quota", requestArgs, null);
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

    //PUT /admin/user?quota&uid=<uid>&quota-type=bucket
    public int setBucketQuota(String uid,int kb, int obs, boolean enable){
        System.out.println("PUT /admin/user?quota&uid=<uid>&quota-type=bucket Input: "+uid
           +" "+kb+" "+obs+" "+enable);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("quota-type", "bucket");
        String body = "{\"enabled\":"+enable+",\"max_size_kb\":"+kb+",\"max_objects\":"+obs+"}";
        response = adminApi.execute("PUT", "/admin/user", "quota", requestArgs, body);
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
