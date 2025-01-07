package src;

import org.zeromq.*;
import java.io.*;
import java.util.*;
public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        System.out.println("Enter a file path:");
        String filePath = sc.nextLine();
        
        try (ZContext context = new ZContext(); FileInputStream fileInput = new FileInputStream(new File(filePath))) {
            ZMQ.Socket sendSocket = context.createSocket(SocketType.PUSH);
            sendSocket.bind("tcp://*:5555");

            ZMQ.Socket receieveSocket = context.createSocket(SocketType.PULL);
            receieveSocket.bind("tcp://*:5556");

            Random rand = new Random();
            byte[] buffer = new byte[10];
            int bytesRead;
            int counter = 0;
            long lamportClock = 0;
            
            String process = "";
            List<Message> messageList = new ArrayList<>();

            while ((bytesRead = fileInput.read(buffer)) != -1) {
                String[] randomProcesses = {"file-p1", "file-p2", "file-p3", "file-p4", "file-p5"};
                process = randomProcesses[rand.nextInt(randomProcesses.length)];  
                byte[] chunk = Arrays.copyOf(buffer, bytesRead);
                Message message = new Message(chunk, lamportClock, lamportClock + 1, false, process, "NOT_END");
                messageList.add(message);
                byte[] serializedMessage = serializeMessage(message);
                sendSocket.send(serializedMessage, 0);
                System.out.println("Sent chunk " + counter + " to: " + process + " (" + new String(chunk) + ")");
                lamportClock++;
                Thread.sleep(1);
                counter++;
            }   
            System.out.println("Waiting 15 seconds ...");
            Thread.sleep(15000);
            System.out.println("Completed wait, Sending END message to p1...");
            Message allChunksMessage = new Message("END");
            byte[] allChunksMessageBytes = serializeMessage(allChunksMessage);            
            sendSocket.send(allChunksMessageBytes, 0); 
            lamportClock++;


                Message receivedMessage = null;
                boolean allChunksReceived = false;
                
                ArrayList<Message> receivedMessages = new ArrayList<>();

                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(1); 
                    byte[] messageBytes = receieveSocket.recv(0);               
                    try {
                        receivedMessage = deserializeMessage(messageBytes);
                    } catch (Exception e) {
                        System.out.println("Error deserializing message: " + e.getMessage());
                        continue;
                    }
                    if ("END".equals(receivedMessage.getMessageType())) {
                        System.out.println("Received end-of-chunks message.");
                        break; 
                    }                
                    long receivedLamportClock = receivedMessage.getNewLamportClock();
                    lamportClock = Math.max(lamportClock, receivedLamportClock) + 1;
                    receivedMessage.setNewLamportClock(lamportClock);
                
                    receivedMessages.add(receivedMessage);
                    System.out.println("List: "+receivedMessages.size());
                }
                System.out.println("Original total: " + messageList.size());
                System.out.println("All chunks received. Total messages: " + receivedMessages.size());
                
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


        } catch (Exception ex) {
            System.err.println("Error in Main: "+ex.getMessage());
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
