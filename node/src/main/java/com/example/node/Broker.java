package com.example.node;

import java.io.*;
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Broker {

    private static final int MAX_PUB = 5;
    private static final int MAX_SUB = 10;

    // 当前连接的 Publisher 和 Subscriber 数量
    private static int publisherCount = 0;
    private static int subscriberCount = 0;

    private static final Map<String, Topic> topics = new HashMap<>();
    //记录各自节点下的socketClient，用于通知
    private static final Map<String, List<Socket>> socketClients = new HashMap<>();
    private static final List<Socket> brokerConnections = new ArrayList<>(); // 其他 Broker 的连接
    private static final List<String> failedBrokers = new ArrayList<>(); // 存储失败的 Broker

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        ServerSocket serverSocket = new ServerSocket(port);
        System.out.println("Broker started on port: " + port);

        // 连接到其他 Broker
        String brokersArg = "";

        // 解析命令行参数
        for (int i = 1; i < args.length; i++) {
            if ("-b".equals(args[i])) {
                // 找到 -b 后，拼接所有后面的 broker 地址
                StringBuilder brokers = new StringBuilder();
                for (int j = i + 1; j < args.length; j++) {
                    brokers.append(args[j]).append(" "); // 使用空格拼接多个 broker 地址
                }
                brokersArg = brokers.toString().trim(); // 去掉末尾空格
                break;
            }
        }

        if (!brokersArg.isEmpty()) {
            // 调用 connectToOtherBrokers 方法，并传递所有 brokers 地址
            connectToOtherBrokers(brokersArg);
            new Thread(new BrokerConnectionListener(args[2])).start();
        }


        // 接受客户端和其他 Broker 连接
        while (true) {
            Socket socket = serverSocket.accept();
            new Thread(() -> {
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                    // 读取初始消息以确定连接类型
                    String type = in.readLine();
                    if ("BROKER".equalsIgnoreCase(type)) {
                        // 这是来自其他 Broker 的连接
                        brokerConnections.add(socket);
                        new Thread(new BrokerHandler(socket)).start();
                        System.out.println("Accepted connection from another broker.");
                    } else if ("SUB".equalsIgnoreCase(type) || "PUB".equalsIgnoreCase(type)) {
                        // 这是来自客户端的连接
//                        clientConnections.add(socket);
                        new Thread(new ClientHandler(socket)).start();
                        System.out.println("Accepted connection from a client.");


                        if ("SUB".equalsIgnoreCase(type)){
                            subscriberCount++;
                            if (subscriberCount>MAX_SUB){
                                //socket.close();
                                sendResponse(socket,"close");
                                subscriberCount--;
                            }

                        }else{
                            publisherCount++;
                            if (publisherCount>MAX_PUB){
                                //socket.close();
                                sendResponse(socket,"close");
                                publisherCount--;
                            }

                        }

                    }
                } catch (IOException e) {
                    //e.printStackTrace();
                    System.out.println(e.getMessage());
                }
            }).start();
        }

    }

    // 连接到其他 Broker
    private static void connectToOtherBrokers(String brokersArg) {
        String[] brokers = brokersArg.split(" ");
        for (String broker : brokers) {
            String[] brokerInfo = broker.split(":");
            String brokerIp = brokerInfo[0];
            int brokerPort = Integer.parseInt(brokerInfo[1]);
            try {
                Socket brokerSocket = new Socket(brokerIp, brokerPort);
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println("BROKER"); // 发送身份标识
                brokerConnections.add(brokerSocket); // 添加到 Broker 连接列表
                new Thread(new BrokerHandler(brokerSocket)).start();
                System.out.println("Connected to broker: " + brokerIp + ":" + brokerPort);
            } catch (IOException e) {
                System.out.println("Failed to connect to broker: " + brokerIp + ":" + brokerPort);
                if (!failedBrokers.contains(broker)){
                    failedBrokers.add(broker); // 将失败的 Broker 加入列表
                }

            }
        }
    }

    // 用于处理其他 Broker 的消息
    private static class BrokerHandler implements Runnable {
        private final Socket brokerSocket;

        public BrokerHandler(Socket brokerSocket) {
            this.brokerSocket = brokerSocket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(brokerSocket.getInputStream()))) {
                String message;
                while ((message = in.readLine()) != null) {
                    handleBrokerMessage(message);
                }
            } catch (IOException e) {
                //e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }


        // 处理来自其他 Broker 的消息
        private static void handleBrokerMessage(String message) {
            String[] parts = message.split(" ");
            String command = parts[0];


            switch (command) {
                case "CREATE":
                    createTopic(parts);
                    break;
                case "PUBLISH":
                    publishMessage(parts);
                    break;
                case "SUBSCRIBE":
                    subscribe(parts);
                    break;
                case "DELETE":
                    deleteTopic(parts);
                    break;
                case "UNSUBSCRIBE":
                    unsubscribe(parts);
                default:
                    break;
            }


        }


        // 创建主题
        private static void createTopic(String[] parts) {
            if (parts.length != 4) {
                return;
            }

            String topicId = parts[1];
            String topicName = parts[2];
            String publisher = parts[3];

            if (!topics.containsKey(topicId)) {
                topics.put(topicId, new Topic(topicId, topicName, publisher));
            }
        }

        // 发布消息
        private static void publishMessage(String[] parts) {
            if (parts.length < 4) {
                return;
            }

            String topicId = parts[1];
            String message = String.join(" ", Arrays.copyOfRange(parts, 2, parts.length));
            Topic topic = topics.get(topicId);

            if (topic == null) {
                return;
            }

            String username = parts[3];

            if (!topic.getPublisher().equals(username)){
                //不能往别人的主题里发消息
                return;
            }


            String timestamp = new SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());
            String formattedMessage = String.format("[%s] [主题 ID:%s:%s] [%s]", timestamp, topicId, topic.getName(), message);
            topic.publishMessage(formattedMessage);

            if (socketClients.get(topicId) != null) {
                for (Socket subscriberSocket : socketClients.get(topicId)) {
                    try {
                        PrintWriter out = new PrintWriter(subscriberSocket.getOutputStream(), true);
                        out.println(formattedMessage);
                    } catch (IOException e) {
                        //e.printStackTrace();
                        System.out.println(e.getMessage());
                    }
                }
            }


        }

        // 订阅主题
        private static void subscribe(String[] parts) {
            if (parts.length != 3) {

                return;
            }

            String topicId = parts[1];
            String subscriber = parts[2];
            Topic topic = topics.get(topicId);
            if (topic == null) {
                //sendResponse(socket, "[ERROR] 主题没找到: " + topicId);
                return;
            }

            topic.addSubscriber(subscriber);

        }

        // 删除主题
        private static void deleteTopic(String[] parts) {
            if (parts.length != 2) {

                return;
            }

            String topicId = parts[1];
            topics.remove(topicId);

            //删掉订阅者socket
            if (socketClients.get(topicId) != null) {
                socketClients.remove(topicId);
            }
        }

        // 取消订阅
        private static void unsubscribe(String[] parts) {
            if (parts.length != 3) {
                return;
            }

            String topicId = parts[1];
            String subscriber = parts[2];
            Topic topic = topics.get(topicId);
            if (topic == null) {

                return;
            }

            topic.removeSubscriber(subscriber);

        }
    }


    // 监听器类：定期重试连接失败的 Broker
    private static class BrokerConnectionListener implements Runnable {
        private final String brokersArg;
        private static final int RETRY_INTERVAL = 5000; // 每隔 5 秒重试一次

        public BrokerConnectionListener(String brokersArg) {
            this.brokersArg = brokersArg;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(RETRY_INTERVAL);
                    retryFailedConnections();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // 重试连接失败的 Broker
        private void retryFailedConnections() {
            Iterator<String> iterator = failedBrokers.iterator();
            while (iterator.hasNext()) {
                String broker = iterator.next();
                String[] brokerInfo = broker.split(":");
                String brokerIp = brokerInfo[0];
                int brokerPort = Integer.parseInt(brokerInfo[1]);
                try {
                    Socket brokerSocket = new Socket(brokerIp, brokerPort);
                    brokerConnections.add(brokerSocket);
                    new Thread(new BrokerHandler(brokerSocket)).start();
                    System.out.println("Reconnected to broker: " + brokerIp + ":" + brokerPort);
                    iterator.remove(); // 连接成功后从失败列表中移除
                } catch (IOException e) {
                    System.out.println("Retrying failed broker: " + brokerIp + ":" + brokerPort);
                }
            }
        }
    }

    // 处理客户端消息的类
    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                String message;
                while ((message = in.readLine()) != null) {
                    handleClientMessage(message, clientSocket);
                }
            } catch (IOException e) {
                //e.printStackTrace();
                System.out.println(e.getMessage());
            }
        }

        // 处理来自客户端的消息
        private void handleClientMessage(String message, Socket socket) {
            String[] parts = message.split(" ");
            String command = parts[0];

            switch (command) {
                case "CREATE":
                    createTopic(parts, socket);
                    broadcastToBrokers(message);  // 广播消息到其他 Broker
                    break;
                case "PUBLISH":
                    publishMessage(parts, socket);
                    broadcastToBrokers(message);  // 广播消息到其他 Broker
                    break;
                case "SHOW":
                    showSubscribers(parts, socket);
                    break;
                case "DELETE":
                    deleteTopic(parts, socket);
                    broadcastToBrokers(message);  // 广播消息到其他 Broker
                    break;
                case "SUBSCRIBE":
                    subscribe(parts, socket);
                    broadcastToBrokers(message);  // 广播消息到其他 Broker
                    break;
                case "DISPLAY":
                    displayTopics(socket);
                    break;
                case "CURRENT":
                    showCurrentSubscriptions(parts,socket);
                    break;
                case "UNSUBSCRIBE":
                    unsubscribe(parts, socket);
                    broadcastToBrokers(message);  // 广播消息到其他 Broker
                    break;
                default:
                    System.out.println("[ERROR] 客户端非法指令.");
            }
        }


        // 创建主题
        private static void createTopic(String[] parts, Socket socket) {
            if (parts.length != 4) {
                sendResponse(socket, "[ERROR] 创建主题参数错误");
                return;
            }

            String topicId = parts[1];
            String topicName = parts[2];
            String publisher = parts[3];

            if (topics.containsKey(topicId)) {
                sendResponse(socket, "[ERROR] 主题已存在.");
            } else {
                topics.put(topicId, new Topic(topicId, topicName, publisher));
                sendResponse(socket, "[SUCCESS] 主题创建: " + topicId + " - " + topicName);
            }
        }

        // 发布消息
        private static void publishMessage(String[] parts, Socket socket) {
            if (parts.length < 4) {
                sendResponse(socket, "[ERROR] 发布消息参数错误");
                return;
            }

            String topicId = parts[1];
            String message = String.join(" ", Arrays.copyOfRange(parts, 2, parts.length));
            Topic topic = topics.get(topicId);

            if (topic == null) {
                sendResponse(socket, "[ERROR] 主题没找到: " + topicId);
                return;
            }
            String username = parts[3];
            if (!topic.getPublisher().equals(username)){
                sendResponse(socket, "[ERROR] 该主题不是自己创建的 ");
                return;
            }


            String timestamp = new SimpleDateFormat("dd/MM HH:mm:ss").format(new Date());
            String formattedMessage = String.format("[%s] [主题 ID:%s:%s] [%s]", timestamp, topicId, topic.getName(), message);
            topic.publishMessage(formattedMessage);

            if (socketClients.get(topicId) != null) {
                for (Socket subscriberSocket : socketClients.get(topicId)) {
                    try {
                        PrintWriter out = new PrintWriter(subscriberSocket.getOutputStream(), true);
                        out.println(formattedMessage);
                    } catch (IOException e) {
                        //e.printStackTrace();
                        System.out.println(e.getMessage());
                    }
                }
            }

            sendResponse(socket, "[SUCCESS] 成功发布消息: " + topicId);
        }

        // 显示某个发布者的订阅者数量
        private static void showSubscribers(String[] parts, Socket socket) {
            if (parts.length != 2) {
                sendResponse(socket, "[ERROR] 显示主题及订阅者参数错误.");
                return;
            }

            String publisher = parts[1];
            List<Topic> publisherTopics = getTopicsByPublisher(publisher);

            for (Topic tp : publisherTopics) {
                int subscriberCount = tp.getSubscribers().size();
                sendResponse(socket, String.format("[主题 ID:%s] [主题名称:%s] [订阅者数量:%d]", tp.getId(), tp.getName(), subscriberCount));
            }
        }

        // 删除主题
        private static void deleteTopic(String[] parts, Socket socket) {
            if (parts.length != 2) {
                sendResponse(socket, "[ERROR] 删除主题参数错误.");
                return;
            }

            String topicId = parts[1];
            if (topics.remove(topicId) != null) {
                sendResponse(socket, "[SUCCESS] 主题删除成功: " + topicId);
            } else {
                sendResponse(socket, "[ERROR] 主题没找到: " + topicId);
            }

            //删掉订阅者socket
            if (socketClients.get(topicId) != null) {
                socketClients.remove(topicId);
            }
        }

        // 订阅主题
        private static void subscribe(String[] parts, Socket socket) {
            if (parts.length != 3) {
                sendResponse(socket, "[ERROR] 订阅主题参数错误.");
                return;
            }

            String topicId = parts[1];
            String subscriber = parts[2];
            Topic topic = topics.get(topicId);
            if (topic == null) {
                sendResponse(socket, "[ERROR] 主题没找到: " + topicId);
                return;
            }

            topic.addSubscriber(subscriber);
            if (socketClients.get(topicId) == null) {
                List<Socket> sockets = new ArrayList<>();
                sockets.add(socket);
                socketClients.put(topicId, sockets);
            } else {
                List<Socket> sockets = socketClients.get(topicId);
                if (!sockets.contains(socket)) {
                    sockets.add(socket);
                }
            }

            sendResponse(socket, "[SUCCESS] 成功订阅主题: " + topicId);
        }

        // 显示所有主题
        private static void displayTopics(Socket socket) {
            if (topics.isEmpty()) {
                sendResponse(socket, "[ERROR] 主题为空.");
                return;
            }

            for (Topic topic : topics.values()) {
                sendResponse(socket, String.format("[主题 ID:%s] [主题名称:%s] [发布者:%s]", topic.getId(), topic.getName(), topic.getPublisher()));
            }
        }

        // 显示当前订阅（暂未实现）
        private static void showCurrentSubscriptions(String[]parts, Socket socket) {


            if (parts.length != 2) {
                sendResponse(socket, "[ERROR] 显示订阅者订阅的主题参数错误.");
                return;
            }

            String subscriber = parts[1];
            List<Topic>result = getTopicsBySubscriber(subscriber);
            for (Topic topic : result) {
                sendResponse(socket, String.format("[主题 ID:%s] [主题名称:%s] [发布者:%s]", topic.getId(), topic.getName(), topic.getPublisher()));
            }
        }

        // 取消订阅
        private static void unsubscribe(String[] parts, Socket socket) {
            if (parts.length != 3) {
                sendResponse(socket, "[ERROR] 取消订阅参数错误.");
                return;
            }

            String topicId = parts[1];
            String subscriber = parts[2];
            Topic topic = topics.get(topicId);
            if (topic == null) {
                sendResponse(socket, "[ERROR] 主题没找到: " + topicId);
                return;
            }

            topic.removeSubscriber(subscriber);

            List<Socket> sockets = socketClients.get(topicId);
            if (sockets != null) {
                sockets.remove(socket);
            }

            sendResponse(socket, "[SUCCESS] 取消订阅成功: " + topicId);
        }



        // 根据发布者获取主题列表
        private static List<Topic> getTopicsByPublisher(String publisher) {
            return topics.values().stream()
                    .filter(topic -> topic.getPublisher().equals(publisher))
                    .collect(Collectors.toList());
        }


        // 获取含有当前订阅者的 Topic 列表
        private static List<Topic> getTopicsBySubscriber(String subscriber) {
            List<Topic> result = new ArrayList<>();

            for (Topic topic : topics.values()) {
                if (topic.getSubscribers().contains(subscriber)) {
                    result.add(topic);
                }
            }

            return result;
        }


    }

    // 发送响应给客户端
    private static void sendResponse(Socket socket, String message) {

        try {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            out.println(message);
        } catch (IOException e) {
            //e.printStackTrace();
            System.out.println(e.getMessage());
        }
    }

    // 向其他 Broker 广播消息
    private static void broadcastToBrokers(String message) {
        for (Socket brokerSocket : brokerConnections) {
            try {
                PrintWriter out = new PrintWriter(brokerSocket.getOutputStream(), true);
                out.println(message);
            } catch (IOException e) {
                System.out.println("Failed to broadcast message to broker: " + e.getMessage());
            }
        }
    }


    // Topic 类
    static class Topic {
        private final String id;
        private final String name;
        private String publisher;
        //记录所有的订阅者
        private final List<String> subscribers = new ArrayList<>();

        public Topic(String id, String name) {
            this.id = id;
            this.name = name;
        }

        public Topic(String id, String name, String publisher) {
            this.id = id;
            this.name = name;
            this.publisher = publisher;
        }

        public String getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public String getPublisher() {
            return publisher;
        }

        public void setPublisher(String publisher) {
            this.publisher = publisher;
        }

        public List<String> getSubscribers() {
            return subscribers;
        }

        public void publishMessage(String message) {
            // 可选：存储消息以供检索
        }

        public void addSubscriber(String subscriber) {
            subscribers.add(subscriber);
        }

        public void removeSubscriber(String subscriber) {
            subscribers.remove(subscriber);
        }
    }
}
