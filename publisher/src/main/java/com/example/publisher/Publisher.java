package com.example.publisher;

import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.UUID;

public class Publisher {

    static String username;

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: java -jar publisher.jar <username> <broker_ip> <broker_port>");
            return;
        }

        username = args[0];
        String brokerIp = args[1];
        int brokerPort = Integer.parseInt(args[2]);

        // 创建连接到 Broker 的 Socket
        Socket socket = new Socket(brokerIp, brokerPort);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        BufferedReader brokerReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // 发送身份标识
        out.println("PUB");


        // 创建一个线程来处理接收来自 Broker 的消息
        new Thread(() -> {
            try {
                String response;

                while ((response = brokerReader.readLine()) != null) {
                    System.out.println("[来自Broker的响应]: " + response);
                    if (response.equals("close")){
                        socket.close();
                    }
                }
                System.out.println("res:"+brokerReader.readLine());
            } catch (IOException e) {
                if (e.getMessage().equals("socket closed")){
                    System.out.println("发布者数量不能超过5个");
                    System.exit(0);
                }

                //e.printStackTrace();
            }
        }).start();

        System.out.println("Connected to broker as " + username);
        while (true) {
            System.out.println("请选择命令：创建、发布、显示、删除。");
            String command = reader.readLine();
            String[] parts = command.split(" ");

            switch (parts[0].toUpperCase()) {
                case "创建":
                    createTopic(parts, out);
                    break;
                case "发布":
                    publishMessage(parts, out);
                    break;
                case "显示":
                    showSubscribers(parts, out);
                    break;
                case "删除":
                    deleteTopic(parts, out);
                    break;
                default:
                    System.out.println("[ERROR] 非法指令.");
            }
        }
    }

    private static void createTopic(String[] parts, PrintWriter out) {
        if (parts.length != 3) {
            System.out.println("[ERROR] 创建参数错误.");
            return;
        }

        String topicId = parts[1];
        String topicName = parts[2];
        String message = "CREATE " + topicId + " " + topicName + " " + username;

        out.println(message);
    }

    private static void publishMessage(String[] parts, PrintWriter out) {
        if (parts.length < 3) {
            System.out.println("[ERROR] 发布参数错误.");
            return;
        }

        String topicId = parts[1];
        String content = String.join(" ", Arrays.copyOfRange(parts, 2, parts.length));

        if(content.length()>100){
            System.out.println("[ERROR] 不能超过100个字母.");
            return;
        }

        String message = "PUBLISH " + topicId + " " + content+ " " +username;

        out.println(message);
    }

    private static void showSubscribers(String[] parts, PrintWriter out) {

        String message = "SHOW "+username;
        out.println(message);
    }

    private static void deleteTopic(String[] parts, PrintWriter out) {
        if (parts.length != 2) {
            System.out.println("[ERROR] 删除参数错误.");
            return;
        }

        String topicId = parts[1];
        String message = "DELETE " + topicId;

        out.println(message);
    }
}
