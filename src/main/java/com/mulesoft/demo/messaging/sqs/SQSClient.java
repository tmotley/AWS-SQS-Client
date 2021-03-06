package com.mulesoft.demo.messaging.sqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
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
 * Uses the AWS SDK to post messages to an SQS queue.
 *
 */
public class SQSClient {
    public static String LEADS_Q = "https://sqs.us-east-1.amazonaws.com/628374222115/leads";

    public static AmazonSQS getClient() {
        AmazonSQS sqs = new AmazonSQSClient(new ClasspathPropertiesFileCredentialsProvider());

        Region usWest2 = Region.getRegion(Regions.US_EAST_1);
        sqs.setRegion(usWest2);

        System.out.println("===========================================");
        System.out.println("Sending new Messages to " + LEADS_Q);
        System.out.println("===========================================\n");

        return sqs;
    }

    public static String convertToJSON(String src) {
        String[] nextLead = StringUtils.commaDelimitedListToStringArray(src);

        ObjectNode rootObj = new TreeMapper().objectNode() ;
        rootObj.put("FirstName", nextLead[0]);
        rootObj.put("LastName", nextLead[1]);
        rootObj.put("Company", nextLead[2]);
        rootObj.put("Street", nextLead[3]);
        rootObj.put("City", nextLead[4]);
        rootObj.put("County", nextLead[5]);
        rootObj.put("State", nextLead[6]);
        rootObj.put("Zip", nextLead[7]);
        rootObj.put("Phone", nextLead[8]);
        rootObj.put("Fax", nextLead[9]);
        rootObj.put("Email", nextLead[10]);
        rootObj.put("Website", nextLead[11]);

        return rootObj.toString();
    }

    public static void main(String[] args) throws Exception {

        try {

            final AmazonSQS sqs = getClient();
            System.out.println();

            CSVReader.readCSV(new File(args[0]), new CSVReader.LineHandler() {
                int counter = 1;
                public void handle(String line) {
                    System.out.println("Message #: " + counter++);
                    String json = convertToJSON(line);
                    System.out.println("Sending a message to Queue: " + json + "\n");
                    sqs.sendMessage(new SendMessageRequest(LEADS_Q, json));

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
