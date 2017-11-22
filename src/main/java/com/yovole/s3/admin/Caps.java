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
public class Caps {

    CephAdminAPI adminApi;

    public Caps(CephAdminAPI api){
        this.adminApi = api;
    }

    //PUT /{admin}/user?caps&format=json
    public int addUserCaps(String uid , String caps){
        System.out.println("PUT /{admin}/user?caps Input: "+uid+" "+caps);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("user-caps", caps);
        response = adminApi.execute("PUT", "/admin/user", "caps", requestArgs, null);
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

    //DELETE /{admin}/user?caps&format=json
    public int removeUserCaps(String uid, String caps){
        System.out.println("DELETE /{admin}/user?caps Input: "+uid+" "+caps);
        Map requestArgs;
        CloseableHttpResponse response;
        HttpEntity entity;
        requestArgs = new HashMap();
        requestArgs.put("format", "json");
        requestArgs.put("uid", uid);
        requestArgs.put("user-caps", caps);
        response = adminApi.execute("DELETE", "/admin/user", "caps", requestArgs, null);
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
