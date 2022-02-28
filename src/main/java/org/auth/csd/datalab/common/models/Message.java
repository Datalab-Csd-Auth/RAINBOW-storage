package org.auth.csd.datalab.common.models;

public class Message {

    private final String result;
    private final String msg;

    public Message(String result, String message) {
        this.result  = result;
        this.msg = message;
    }

    public String getMessage() {
        return msg;
    }

    public String getResult() {
        return result;
    }

}