package src;

import org.zeromq.*;
import java.io.*;
import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter a file path:");
        String filePath = sc.nextLine();

        try (ZContext context = new ZContext(); FileInputStream fileInput = new FileInputStream(new File(filePath))) {
            ZMQ.Socket socket = context.createSocket(SocketType.PUB);
            socket.bind("tcp://127.0.0.1:5555");
            socket.bind("tcp://127.0.0.1:5556");
            socket.bind("tcp://127.0.0.1:5557");
            socket.bind("tcp://127.0.0.1:5558");
            socket.bind("tcp://127.0.0.1:5559");
            System.out.println("Connecting to subs...");
            Thread.sleep(1000);

            Random rand = new Random();
            byte[] buffer = new byte[10];
            int bytesRead;
            int counter = 0;
            long lamportClock = 0;
            
            String topic = "";
            List<Message> messageList = new ArrayList<>();
            String[] topics = {"file-p1", "file-p2", "file-p3", "file-p4", "file-p5"};

            while ((bytesRead = fileInput.read(buffer)) != -1) {
                topic = topics[rand.nextInt(topics.length)];  
                byte[] chunk = Arrays.copyOf(buffer, bytesRead);
                Message message = new Message(chunk, lamportClock, lamportClock + 1, false);
                messageList.add(message);
                byte[] serializedMessage = serializeMessage(message);
                socket.sendMore(topic);
                socket.send(serializedMessage, 0);
                System.out.println("Sent chunk " + counter + " to: " + topic + " (" + new String(chunk) + ")");
                lamportClock++;
                Thread.sleep(1);
                counter++;
            }
            Message allChunksMessage = new Message("END");
            byte[] allChunksMessageBytes = serializeMessage(allChunksMessage);            
            for (String Endtopic : topics) {
                socket.sendMore(Endtopic); 
                socket.send(allChunksMessageBytes, 0); 
                System.out.println("Sent allChunksMessage to " + Endtopic + ": " + allChunksMessage);
            }
            lamportClock++;
            ZMQ.Socket ackToP = context.createSocket(SocketType.PUB);
            ackToP.connect("tcp://127.0.0.1:8001");
            ackToP.connect("tcp://127.0.0.1:8002");
            ackToP.connect("tcp://127.0.0.1:8003");
            ackToP.connect("tcp://127.0.0.1:8004");
            ackToP.connect("tcp://127.0.0.1:8005");
            Thread.sleep(1000);
            String request = "ackToP ";
            System.out.println("Sending " + request);
            ackToP.sendMore("ackToP");
            ackToP.send(request.getBytes(ZMQ.CHARSET), 1);
            System.out.println("Waiting 15 seconds...");
            Thread.sleep(15000);
            System.out.println("Waiting Completed");
            lamportClock++;
            
            
            ZMQ.Socket receiveSocket = context.createSocket(SocketType.SUB);
            receiveSocket.bind("tcp://*:7001");
            receiveSocket.bind("tcp://*:7002");
            receiveSocket.bind("tcp://*:7003");
            receiveSocket.bind("tcp://*:7004");
            receiveSocket.bind("tcp://*:7005");
            receiveSocket.subscribe(""); 
            Message receivedMessage = null;
            boolean allChunksReceived = false;
            lamportClock++;
            ArrayList<Message> receivedMessages = new ArrayList<>();
            int EndCounter = 0;
            System.out.println("Receiving messages back from Processes ... ");
            while (!allChunksReceived) {
                Thread.sleep(1); 
                byte[] messageBytes = receiveSocket.recv(0);                    
                try {
                    receivedMessage = deserializeMessage(messageBytes);
                } catch (Exception e) {
                    System.out.println("Error deserializing message: " + e.getMessage());
                    continue;
                }
                long receivedLamportClock = receivedMessage.getNewLamportClock();
                lamportClock = Math.max(lamportClock, receivedLamportClock) + 1;
                receivedMessage.setNewLamportClock(lamportClock);
                receivedMessages.add(receivedMessage);
                String messageContent = receivedMessage.getMessageType();
                if ("END".equals(messageContent)) {
                    System.out.println("Received 'END' message, skipping..."+" Count: "+EndCounter);
                    EndCounter++;
                    if (EndCounter == topics.length) {
                        allChunksReceived = true;
                        System.out.println("Received all messages and exiting loop...");
                    } else {
                        continue;
                    }
                }
            }
            bubbleSort(receivedMessages);
            int lastDot = filePath.lastIndexOf('.');
            String fileExtension = filePath.substring(lastDot);
            String desktopPath = System.getProperty("user.home") + "/Desktop/reconstructedFile" + fileExtension;
            File file = new File(desktopPath);
            try (FileOutputStream fos = new FileOutputStream(file)) {
                for (Message msgFile : receivedMessages) {
                    fos.write(msgFile.getFileContent());
                }   
            }
        
            System.out.println("Reconstructed message saved to: " + desktopPath);
        }
    }

    public static byte[] serializeMessage(Message message) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(message);
        objectOutputStream.flush();
        return byteArrayOutputStream.toByteArray();
    }

    public static Message deserializeMessage(byte[] messageBytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(messageBytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        return (Message) objectInputStream.readObject();
    }

    public static void bubbleSort(List<Message> messages) {
        int n = messages.size();
        boolean swapped;    
        for (int i = 0; i < n - 1; i++) {
            swapped = false;    
            for (int j = 0; j < n - i - 1; j++) {
                if (messages.get(j).getOldLamportClock() > messages.get(j + 1).getOldLamportClock()) {
                    Message temp = messages.get(j);
                    messages.set(j, messages.get(j + 1));
                    messages.set(j + 1, temp);
                    swapped = true;
                }
            }    
            if (!swapped) break;
        }
    }
}
