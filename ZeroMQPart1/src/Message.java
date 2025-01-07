package src;

import java.io.Serializable;

public class Message implements Serializable {
    private byte[] fileContent = new byte[0]; 
    private long newLamportClock;
    private long oldLamportClock;
    private boolean allChunksSent;
    private String messageType;

    public Message(byte[] fileContent, long oldLamportClock, long newLamportClock, boolean allChunksSent) {
        this.fileContent = fileContent;
        this.oldLamportClock = oldLamportClock;
        this.newLamportClock = newLamportClock;
        this.allChunksSent = allChunksSent;
    }

    public Message(String messageType) {
        this.allChunksSent = true;
        this.messageType = messageType;
    }

    public String getMessageType() {
        return messageType;
    }

    public void setMessageType(String messageType) {
        this.messageType = messageType;
    }

    public byte[] getFileContent() {
        return fileContent;
    }

    public void setFileContent(byte[] fileContent) {
        this.fileContent = fileContent;
    }

    public long getNewLamportClock() {
        return newLamportClock;
    }

    public void setNewLamportClock(long newLamportClock) {
        this.newLamportClock = newLamportClock;
    }

    public long getOldLamportClock() {
        return oldLamportClock;
    }

    public void setOldLamportClock(long oldLamportClock) {
        this.oldLamportClock = oldLamportClock;
    }

    public boolean isAllChunksSent() {
        return allChunksSent;
    }

    public void setAllChunksSent(boolean allChunksSent) {
        this.allChunksSent = allChunksSent;
    }

    @Override
    public String toString() {
        return "Message{" +
               "fileContent=" + (fileContent != null ? new String(fileContent) : "") + 
               ", newLamportClock=" + newLamportClock + 
               ", oldLamportClock=" + oldLamportClock +
               ", allChunksSent=" + allChunksSent + '}';
    }
}
