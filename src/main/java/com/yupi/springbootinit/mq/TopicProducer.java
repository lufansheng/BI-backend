package com.yupi.springbootinit.mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class TopicProducer {

    private static final String EXCHANGE_NAME = "topic-exchange";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            Scanner sc = new Scanner(System.in);
            while (sc.hasNext()) {
                String userInput = sc.nextLine();
                String[] s = userInput.split(" ");
                if (s.length < 1) {
                    continue;
                }
                String message = s[0];
                String routingKey = s[1];

                channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                System.out.println(" [x] Sent '" + message + "'with routingKey'" + routingKey + "'");


            }
        }
        //..
    }
}