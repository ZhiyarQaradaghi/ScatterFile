package src;

import org.zeromq.*;
import java.io.*;
import java.util.*;

public class Process4 {
    public static void main(String[] args) {
        List<Message> savedMessages = new ArrayList<>();
        boolean allChunksReceived = false;

        try (ZContext context = new ZContext()) {
            ZMQ.Socket receiveSocket = context.createSocket(SocketType.PULL);
            receiveSocket.bind("tcp://localhost:5559");

            ZMQ.Socket sendSocket = context.createSocket(SocketType.PUSH);
            sendSocket.connect("tcp://localhost:5560");

            Message receivedMessage = null;
            long lamportClock = 0;
            ArrayList<Message> receivedMessages = new ArrayList<>();
            ArrayList<byte[]> messageBytesList = new ArrayList<>();


            while (!allChunksReceived) {
                Thread.sleep(1);
                byte[] messageBytes = receiveSocket.recv(0);

                receivedMessage = deserializeMessage(messageBytes);
                String content = new String(receivedMessage.getFileContent());

                long receivedLamportClock = receivedMessage.getNewLamportClock();
                receivedMessage.setOldLamportClock(receivedLamportClock);
                lamportClock = Math.max(lamportClock, receivedLamportClock) + 1;  
                
                receivedMessage.setNewLamportClock(lamportClock);

                messageBytesList.add(messageBytes);

                if ("END".equals(receivedMessage.getMessageType())) {
                    System.out.println("Received end-of-chunks receivedMessage.");
                    allChunksReceived = true;
                } else {
                    if ((receivedMessage.getProcess()).equals("file-p4")) {
                        savedMessages.add(receivedMessage);
                        System.out.println("Process4 saved receivedMessage: " + content);
                    } else {
                        sendSocket.send(messageBytes, 0);
                        System.out.println("Process4 forwarded message: " + content);
                    }
                }
            }
            for (Message savedMessage : savedMessages) {
                Thread.sleep(1);
                byte[] serializedMessage = serializeMessage(savedMessage);
                sendSocket.send(serializedMessage, 0);
                System.out.println("Process4 sent saved message to Process5: " + new String(savedMessage.getFileContent()));
            }
            Message endMessage = new Message("END");
            sendSocket.send(serializeMessage(endMessage), 0);
            System.out.println("Process4 sent 'END' to Process5.");
        } catch (Exception e) {
            System.err.println("Error in Process4: " + e.getMessage());
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
}
