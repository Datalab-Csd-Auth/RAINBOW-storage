package org.auth.csd.datalab.common.helpers;

public class Message {

    private final String result;
    private final String message;

    public Message(String result, String message) {
        this.result  = result;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public String getResult() {
        return result;
    }

}