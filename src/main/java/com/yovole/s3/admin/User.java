package com.yovole.s3.admin;

import java.io.*;
import java.lang.Object;
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

public class User {

    CephAdminAPI adminApi;

    public User(CephAdminAPI api){
        this.adminApi = api;
    }

    // PUT /{admin}/user?format=json HTTP/1.1
    public int createUser(String uid, String display, String args){
        System.out.println("PUT /{admin}/user Input: "+uid+" "+display+" "+args);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("uid", uid);
        requestArgs.put("display-name", display);
        requestArgs.put("format", "json");
        if(args != null){
            String[] argsString = args.split("/");
            for(int i = 0; i< argsString.length; i++){
                String[] kv = argsString[i].split(":");
                if(kv[0].equals("max-buckets")){
                    requestArgs.put(kv[0], Integer.parseInt(kv[1]));
                    continue;
                }else if(kv[0].equals("suspended")){
                    requestArgs.put(kv[0], Boolean.parseBoolean(kv[1]));
                    continue;
                }else if(kv[0].equals("generate-key")){
                    requestArgs.put(kv[0], Boolean.parseBoolean(kv[1]));
                    continue;
                }
                requestArgs.put(kv[0], kv[1]);
            }
        }
        response = adminApi.execute("PUT", "/admin/user", null, requestArgs, null);
        int status = response.getStatusLine().getStatusCode();
        System.out.println(status);
        entity = response.getEntity();
        try {
            System.out.println("Response Content is: "
                        + EntityUtils.toString(entity, "UTF-8") + "\n");
            response.close();
            return status;
        } catch (IOException e){
            System.err.println ("Encountered an I/O exception.");
            e.printStackTrace();
            return -1;
        }
    }

    // GET /{admin}/user?format=json
    public int getUser(String uid){
        System.out.println("GET /{admin}/user Input: "+uid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        response = adminApi.execute("GET", "/admin/user", null, requestArgs, null);
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

    // POST /{admin}/user?format=json
    public int modifyUser(String uid, String args){
        System.out.println("POST /{admin}/user Input: "+uid+" "+args);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        String[] argsString = args.split("/");
        for(int i = 0; i< argsString.length; i++){
           String[] kv = argsString[i].split(":");
           if(kv[0].equals("max-buckets")){
               requestArgs.put(kv[0], Integer.parseInt(kv[1]));
               continue;
           }else if(kv[0].equals("suspended")){
               requestArgs.put(kv[0], Boolean.parseBoolean(kv[1]));
               continue;
           }else if(kv[0].equals("generate-key")){
               requestArgs.put(kv[0], Boolean.parseBoolean(kv[1]));
               continue;
           }
           requestArgs.put(kv[0], kv[1]);
        }
        response = adminApi.execute("POST", "/admin/user", null, requestArgs, null);
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

    // DELETE /{admin}/user?format=json
    public int removeUser(String uid){
        System.out.println("DELETE /{admin}/user Input: "+uid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("purge-data","True");
        response = adminApi.execute("DELETE", "/admin/user", null, requestArgs, null);
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

    //PUT /{admin}/user?subuser&format=json
    public int createSubUser(String uid, String subuid){
        System.err.println("PUT /{admin}/user?subuser Input: "+uid+" "+subuid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("subuser",subuid);
        requestArgs.put("key-type","s3");
        requestArgs.put("access","full");
        response = adminApi.execute("PUT", "/admin/user", "subuser", requestArgs, null);
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

    //POST /{admin}/user?subuser&format=json
    public int modifySubUser(String uid, String subuid, String args){
        System.out.println("POST /{admin}/user?subuser Input: "+uid+" "+subuid+" "+args);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("key-type","s3");
        requestArgs.put("uid", uid);
        requestArgs.put("subuser",subuid);
        String[] argsString = args.split("/");
        for(int i = 0; i< argsString.length; i++){
           String[] kv = argsString[i].split(":");
           requestArgs.put(kv[0], kv[1]);
        }
        response = adminApi.execute("POST", "/admin/user", "subuser", requestArgs, null);
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

    //DELETE /{admin}/user?format=json
    public int removeSubUser(String uid, String subuid){
        System.out.println("DELETE /{admin}/user Input: "+uid+" "+subuid);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("key-type","s3");
        requestArgs.put("uid", uid);
        requestArgs.put("subuser",subuid);
        requestArgs.put("purge-keys", true);
        response = adminApi.execute("DELETE", "/admin/user", "subuser", requestArgs, null);
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
