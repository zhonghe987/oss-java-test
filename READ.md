The project is a restful api test project  about ceph object store.
It includes two items:
1, Admin ops API
 
2, S3 API

REFERENCE:
Admin ops API used http mode to test,we can reference:
https://access.redhat.com/documentation/en-us/red_hat_ceph_storage/1.3/html/object_gateway_guide_for_ubuntu/object_gateway_admin_api

S3 API used java-sdk-s3, we can reference:
http://docs.aws.amazon.com/AmazonS3/latest/dev/Welcome.html
http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/s3/AmazonS3Client.html
https://github.com/aws/aws-sdk-java/tree/master/aws-java-sdk-s3

Install require:
java-sdk-8
maven 3.5

Compile:
mvn clean package

Run:
1,excute create user
java -jar target/yovoleOss-0.1.jar admin user createUser user-1

2,excute create bucket
java -jar target/yovoleOss-0.1.jar s3 bucket createBucket test-1-bucket

3, execute upload object
java -jar target/yovoleOss-0.1.jar s3 object uploadObject test-1-bucket object-1 /root/getUrl.py

