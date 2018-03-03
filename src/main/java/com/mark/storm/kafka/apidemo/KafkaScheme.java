package com.mark.storm.kafka.apidemo;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

public class KafkaScheme implements Scheme {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaScheme.class);
    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        //从kafka中读取的值直接序列化为UTF-8的str
        String mString= null;
        try {
            mString = new String(byteBuffer.array(), "UTF-8");
            return new Values(mString);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error("Cannot parse the provided message");
        }
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("message");
    }
}
