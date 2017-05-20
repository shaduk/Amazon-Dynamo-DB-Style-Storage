package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by shadkhan on 25/04/17.
 */

public class MessageObj implements Serializable {
    public String key;
    public String val;
    public String comingFrom;
    public String fwdPort;
    public reqType reqT;
    public boolean status;
    public ConcurrentHashMap<String, String> store;

    public MessageObj()
    {
        val = null;
        comingFrom = null;
        fwdPort = null;
        store = null;
        status = true;
    }

    enum reqType {INSERT, QUERY_ALL,QUERY_ONE, DELETE, RECOVERY, RECOVER_DONE, QUERY_DONE, QUERY_ONE_DONE}
}