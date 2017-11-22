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

public class Key {
    CephAdminAPI adminApi;

    public Key(CephAdminAPI api){
        this.adminApi = api;
    }

    //PUT /{admin}/user?key&format=json
    public int createKey(String uid){
        System.out.println("PUT /{admin}/user?key Input: "+uid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("generate-key", true);
        response = adminApi.execute("PUT", "/admin/user", "key", requestArgs, null);
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

    public int createKeySpeci(String uid, String key, String secret){
        System.out.println("PUT /{admin}/user?key Input: "+uid+" "+key+" "+secret);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("access-key", key);
        requestArgs.put("secret-key", secret);
        response = adminApi.execute("PUT", "/admin/user", "key", requestArgs, null);
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

    public int createKeySubSpeci(String uid, String subuid, String key, String secret){
        System.out.println("PUT /{admin}/user?key Input: "+uid+" "+subuid+" "+key+" "+secret);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("subuser", subuid);
        requestArgs.put("access-key", key);
        requestArgs.put("secret-key", secret);
        requestArgs.put("key-type", "s3");
        response = adminApi.execute("PUT", "/admin/user", "key", requestArgs, null);
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

    public int createKeySub(String uid, String subuid){
        System.out.println("PUT /{admin}/user?key Input: "+uid+" "+subuid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("subuser", subuid);
        requestArgs.put("generate-key", true);
        requestArgs.put("key-type", "s3");
        response = adminApi.execute("PUT", "/admin/user", "key", requestArgs, null);
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

    //DELETE /{admin}/user?key&format=json
    public int removeKey(String key){
        System.out.println("DELETE /{admin}/user?key Input: "+key);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("access-key", key);
        requestArgs.put("key-type", "s3");
        response = adminApi.execute("DELETE", "/admin/user", "key", requestArgs, null);
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

    public int removeKeyWithUid(String key, String uid){
        System.out.println("DELETE /{admin}/user?key Input: "+key+" "+uid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("access-key", key);
        requestArgs.put("key-type", "s3");
        response = adminApi.execute("DELETE", "/admin/user", "key", requestArgs, null);
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
