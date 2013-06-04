package com.mulesoft.demo.messaging.sqs;/*
 * Copyright 2010-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.codehaus.jackson.map.TreeMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.springframework.util.StringUtils;

import java.io.File;

/**
 * This sample demonstrates how to make basic requests to Amazon SQSClient using the
 * AWS SDK for Java.
 * <p>
 * <b>Prerequisites:</b> You must have a valid Amazon Web
 * Services developer account, and be signed up to use Amazon SQSClient. For more
 * information on Amazon SQSClient, see http://aws.amazon.com/sqs.
 * <p>
 * <b>Important:</b> Be sure to fill in your AWS access credentials in the
 *                   AwsCredentials.properties file before you try to run this
 *                   sample.
 * http://aws.amazon.com/security-credentials
 */
public class SQSClient {

    public static String LEADS_Q = "https://sqs.us-east-1.amazonaws.com/628374222115/leads";

    public static AmazonSQS getClient() {

        AmazonSQS sqs = new AmazonSQSClient(new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return new AWSCredentials() {
                    @Override
                    public String getAWSAccessKeyId() {
                        return "AKIAIIWYWCHEIEHDD4SQ";
                    }

                    @Override
                    public String getAWSSecretKey() {
                        return "iC8BxPZ2N4k9/W38InatHzp9y4Qn+256nvkwoE6F";
                    }
                };
            }

            @Override
            public void refresh() {
                System.out.println("REFRESH CALLED");
            }
        });
        Region usWest2 = Region.getRegion(Regions.US_EAST_1);
        sqs.setRegion(usWest2);

        System.out.println("===========================================");
        System.out.println("Sending new Messages to " + LEADS_Q);
        System.out.println("===========================================\n");

        return sqs;
    }

    public static String convertToJSON(String src) {
        TreeMapper treeMapper = new TreeMapper();
        ObjectNode rootObj = treeMapper.objectNode() ;
        String[] nextLead = StringUtils.commaDelimitedListToStringArray(src);
        rootObj.put("FirstName", nextLead[0]);
        rootObj.put("LastName", nextLead[1]);
        rootObj.put("Company", nextLead[2]);
        rootObj.put("Address", nextLead[3]);
        rootObj.put("City", nextLead[4]);
        rootObj.put("County", nextLead[5]);
        rootObj.put("State", nextLead[6]);
        rootObj.put("ZIP", nextLead[7]);
        rootObj.put("Phone", nextLead[8]);
        rootObj.put("Fax", nextLead[9]);
        rootObj.put("Email", nextLead[10]);
        rootObj.put("Web", nextLead[11]);

        return rootObj.toString();

    }

    public static void main(String[] args) throws Exception {

        try {

            final AmazonSQS sqs = getClient();

            System.out.println();

            CSVReader.readCSV(new File(args[0]), new CSVReader.LineHandler() {
                int counter = 0;
                public void handle(String line) {
                    String json = convertToJSON(line);
                    System.out.println("Sending a message to Queue: " + json + "\n");
                    sqs.sendMessage(new SendMessageRequest(LEADS_Q, json));
                    System.out.println("Messages sent: " + counter++);
                }
            });

        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQSClient, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQSClient, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}
