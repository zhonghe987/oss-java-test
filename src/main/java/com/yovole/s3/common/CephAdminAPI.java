package com.yovole.s3.common;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneId;

import org.apache.http.params.*;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.NameValuePair;
import org.apache.http.Header;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.http.client.utils.URIBuilder;

import java.util.Base64;
import java.util.Base64.Encoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.Mac;

import java.util.Map;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;


public class CephAdminAPI {

    /*
    * Each call must specify an access key, secret key, endpoint and format.
    */
    String accessKey;
    String secretKey;
    String endpoint;
    String scheme; //http only.
    int port;

    /*
    constructor that takes an access key, secret key, endpoint and format.
    */
    public CephAdminAPI(String accessKey, String secretKey, String endpoint, String http_type){
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.endpoint = endpoint;
        this.scheme = http_type;
        port  = 80;
        if (http_type.equals("https")){
           port = 443;
        }
    }

    /*
    Accessor methods for access key, secret key, endpoint and format.
    */
    public String getHttp(){
        return this.scheme;
    }

    public void setHttp(String http_type){
        this.scheme = http_type;
    }

    public String getEndpoint(){
        return this.endpoint;
    }

    public void setEndpoint(String endpoint){
        this.endpoint = endpoint;
    }

    public String getAccessKey(){
        return this.accessKey;
    }

    public void setAccessKey(String accessKey){
        this.accessKey = accessKey;
    }

    public String getSecretKey(){
        return this.secretKey;
    }

    public void setSecretKey(String secretKey){
        this.secretKey = secretKey;
    }

    /*
    Takes an HTTP Method, a resource and a map of arguments and
    * returns a CloseableHTTPResponse.
    */
    public CloseableHttpResponse execute(String HTTPMethod, String resource,
                                    String subresource, Map arguments, String body) {

        String httpMethod = HTTPMethod;
        String requestPath = resource;
        StringBuffer request = new StringBuffer();
        StringBuffer headerString = new StringBuffer();
        HttpRequestBase httpRequest;
        CloseableHttpClient httpclient;
        URI uri;
        CloseableHttpResponse httpResponse = null;
        try {
            uri = new URIBuilder()
                .setScheme(this.scheme)
                .setHost(this.getEndpoint())
                .setPath(requestPath)
                .setPort(this.port)
                .build();
            if (subresource != null){
                uri = new URIBuilder(uri)
                    .setCustomQuery(subresource)
                    .build();
                }

            for (Iterator iter = arguments.entrySet().iterator();
                iter.hasNext();) {
                Entry entry = (Entry)iter.next();
                uri = new URIBuilder(uri)
                    .setParameter(entry.getKey().toString(),
                entry.getValue().toString()).build();

            }

            request.append(uri);
            headerString.append(HTTPMethod.toUpperCase().trim() + "\n\n\n");
            OffsetDateTime dateTime = OffsetDateTime.now(ZoneId.of("GMT"));
            DateTimeFormatter formatter = DateTimeFormatter.RFC_1123_DATE_TIME;
            String date = dateTime.format(formatter);

            headerString.append(date + "\n");
            if(subresource == "policy"){
               requestPath = requestPath + "?" + subresource;
            }
            headerString.append(requestPath);
            HttpEntity entity = null;
            if(body != null){
               entity = new ByteArrayEntity(body.getBytes("UTF-8"));

            }


            if(HTTPMethod.equalsIgnoreCase("PUT")){
               httpRequest = new HttpPut(uri);
               ((HttpPut)httpRequest).setEntity(entity);
            } else if (HTTPMethod.equalsIgnoreCase("POST")){
               httpRequest = new HttpPost(uri);
            } else if (HTTPMethod.equalsIgnoreCase("GET")){
               httpRequest = new HttpGet(uri);
            } else if (HTTPMethod.equalsIgnoreCase("DELETE")){
               httpRequest = new HttpDelete(uri);
            } else {
               System.err.println("The HTTP Method must be" +
                  " GET POST, GET or DELETE.");
               throw new IOException();
            }

            httpRequest.addHeader("Date", date);
            httpRequest.addHeader("Authorization", "AWS " + this.getAccessKey()
               + ":" + base64Sha1Hmac(headerString.toString(),
               this.getSecretKey()));

            httpclient = HttpClients.createDefault();
            httpResponse = httpclient.execute(httpRequest);

            }catch(URISyntaxException e){
               System.err.println("The URI is not formatted properly.");
               e.printStackTrace();
        }catch (IOException e){
            System.err.println("There was an error making the request.");
            e.printStackTrace();
        }
       return httpResponse;
       }

       /*
       * Takes a uri and a secret key and returns a base64-encoded
       * SHA-1 HMAC.
       */
    public String base64Sha1Hmac(String uri, String secretKey) {
        try {

            byte[] keyBytes = secretKey.getBytes("UTF-8");
            SecretKeySpec signingKey = new SecretKeySpec(keyBytes, "HmacSHA1");

            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(signingKey);

            byte[] rawHmac = mac.doFinal(uri.getBytes("UTF-8"));

            Encoder base64 = Base64.getEncoder();
            return base64.encodeToString(rawHmac);

        }catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
