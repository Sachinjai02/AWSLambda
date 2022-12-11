package com.study;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;


import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.HashMap;
import java.util.Map;

public class SendMessageSQS implements RequestHandler<Map<String,String>, String> {
    private Gson gson = new GsonBuilder().setPrettyPrinting().create();

    @Override
    public String handleRequest(Map<String, String> input, Context context) {
        context.getLogger().log("Input: " + input);
        Map<String, String> messageMap = new HashMap<>();
        messageMap.put("Name", "sachinjainnsit");
        messageMap.put("Email", "sachin.jain.nsit@gmail.com");


        AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .build();

        CreateQueueRequest createQueueRequest = new CreateQueueRequest("sachinjainnsit");
        String queueUrl = sqsClient.createQueue(createQueueRequest).getQueueUrl();

        for(int i=1;i<=500;++i) {

            messageMap.put("MessageBody", "message" + i);
            messageMap.put("MessageNumber", "" + i);
            try {
                context.getLogger().log("Sending request: " + i);
                SendMessageRequest sendRequest = new SendMessageRequest()
                        .withQueueUrl(queueUrl)
                        .withMessageBody(gson.toJson(messageMap));

                sqsClient.sendMessage(sendRequest);
            }catch(Exception e) {
                context.getLogger().log("Exception occurred while sending message  " + i + e);
            }
        }
        context.getLogger().log("Done with sending all messages");
        return "Hello World - " + input;
    }
}
