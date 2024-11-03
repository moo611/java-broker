package com.example.subscriber;

import java.io.*;
import java.net.*;
import java.util.UUID;

public class Subscriber {
    static String username;

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.out.println("Usage: java -jar subscriber.jar <username> <broker_ip> <broker_port>");
            return;
        }


        username = args[0];
        String brokerIp = args[1];
        int brokerPort = Integer.parseInt(args[2]);


        Socket socket = new Socket(brokerIp, brokerPort);
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        BufferedReader brokerReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // 发送身份标识
        out.println("SUB");

        // 使用多线程来接收Broker的消息
        new Thread(() -> {
            try {
                String message;
                while ((message = brokerReader.readLine()) != null) {
                    System.out.println("[来自Broker的消息]: " + message);
                    if (message.equals("close")){
                        socket.close();
                    }
                }
            } catch (IOException e) {
                if (e.getMessage().equals("socket closed")){
                    System.out.println("订阅者数量不能超过10个");
                    System.exit(0);
                }
            }
        }).start();


        System.out.println("Connected to broker as " + username);
        while (true) {
            System.out.println("请选择命令：显示、订阅、当前、取消订阅。");
            String command = reader.readLine();
            String[] parts = command.split(" ");

            switch (parts[0].toUpperCase()) {
                case "显示":
                    displayTopics(parts, out);
                    break;
                case "订阅":
                    subscribe(parts, out);
                    break;
                case "当前":
                    currentSubscriptions(out);
                    break;
                case "取消订阅":
                    unsubscribe(parts, out);
                    break;
                default:
                    System.out.println("[ERROR] 非法指令.");
            }
        }
    }

    private static void displayTopics(String[] parts, PrintWriter out) {
        if (parts.length != 1 || !parts[0].equals("显示")) {
            System.out.println("[ERROR] 显示参数错误.");
            return;
        }
        String message = "DISPLAY";

        out.println(message);
    }

    private static void subscribe(String[] parts, PrintWriter out) {
        if (parts.length != 2) {
            System.out.println("[ERROR] 订阅参数错误.");
            return;
        }

        String topicId = parts[1];

        String message = "SUBSCRIBE " + topicId + " " + username;


        out.println(message);
    }

    private static void currentSubscriptions(PrintWriter out) {
        String message = "CURRENT "+username;

        out.println(message);
    }

    private static void unsubscribe(String[] parts, PrintWriter out) {
        if (parts.length != 2) {
            System.out.println("[ERROR] 取消订阅参数错误.");
            return;
        }

        String topicId = parts[1];

        String message = "UNSUBSCRIBE " + topicId + " " + username;

        out.println(message);
    }
}
