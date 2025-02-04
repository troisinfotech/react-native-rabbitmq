package nl.kega.reactnativerabbitmq;

import android.net.Uri;
import android.util.Log;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ExecutorService;

import com.facebook.react.bridge.Arguments;
import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReadableMap;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.bridge.ReadableType;
import com.facebook.react.modules.core.DeviceEventManagerModule;
import com.facebook.react.bridge.ReadableMapKeySetIterator;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;

public class RabbitMqExchange {

    public String name;
    public String type;
    public Boolean durable;
    public Boolean autodelete;
    public Boolean internal;
    public Integer rpcTimeout;

    private ReactApplicationContext context;

    public Channel channel;
    public Channel rpcChannel;

    private ExecutorService executorService;

    private ConcurrentNavigableMap<Long, ReadableMap> outstandingConfirms;

    public RabbitMqExchange (ReactApplicationContext context, Channel channel, Channel rpcChannel, ExecutorService executorService, ConcurrentNavigableMap<Long, ReadableMap> outstandingConfirms, ReadableMap config) throws IOException {
        
        this.context = context;
        this.channel = channel;
        this.rpcChannel = rpcChannel;
        this.executorService = executorService;

        this.name = config.getString("name");
        this.type = (config.hasKey("type") ? config.getString("type") : "fanout");
        this.durable = (config.hasKey("durable") ? config.getBoolean("durable") : true);
        this.autodelete = (config.hasKey("autoDelete") ? config.getBoolean("autoDelete") : false);
        this.internal = (config.hasKey("internal") ? config.getBoolean("internal") : false);
        this.rpcTimeout = (config.hasKey("rpcTimeout") ? config.getInt("rpcTimeout") : 10);

        this.outstandingConfirms = outstandingConfirms;
        
        Map<String, Object> args = new HashMap<String, Object>();

        this.channel.exchangeDeclare(this.name, this.type, this.durable, this.autodelete, this.internal, args);

    }

    private Uri getFileUri(String filepath, boolean isDirectoryAllowed) {
        Uri uri = Uri.parse(filepath);
        if (uri.getScheme() == null) {
        // No prefix, assuming that provided path is absolute path to file
        File file = new File(filepath);
        if (!isDirectoryAllowed && file.isDirectory()) {
            throw new IllegalStateException("illegal operation on a directory, read '" + filepath + "'");
        }
        uri = Uri.parse("file://" + filepath);
        }
        return uri;
    }

    private InputStream getInputStream(String filepath) {
        Uri uri = getFileUri(filepath, false);
        InputStream stream;
        try {
        stream = this.context.getContentResolver().openInputStream(uri);
        } catch (FileNotFoundException ex) {
            throw new IllegalStateException(ex.getMessage() + ", open '" + filepath + "'");
        }
        if (stream == null) {
            throw new IllegalStateException("Could not open an input stream for '" + filepath + "'");
        }
        return stream;
    }

    private static byte[] getInputStreamBytes(InputStream inputStream) throws IOException {
        byte[] bytesResult;
        ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
        int bufferSize = 1024;
        byte[] buffer = new byte[bufferSize];
        try {
            int len;
            while ((len = inputStream.read(buffer)) != -1) {
                byteBuffer.write(buffer, 0, len);
            }
            bytesResult = byteBuffer.toByteArray();
        } finally {
            try {
                byteBuffer.close();
            } catch (IOException ignored) { }
        }
        return bytesResult;
    }

    public void publish(String messageOrFilePath, boolean isBlob, String routing_key, ReadableMap message_properties, ReadableMap message_headers){ 
        Long sequenceNumber = null;
        try {
            byte[] message_body_bytes = null;
            if(isBlob) {
                InputStream inputStream = getInputStream(messageOrFilePath);
                message_body_bytes = getInputStreamBytes(inputStream);
            } else {
                message_body_bytes = messageOrFilePath.getBytes();
            }

            AMQP.BasicProperties.Builder properties = new AMQP.BasicProperties.Builder();

            // if (message_properties != null){
                //  try {
                     
                    if (message_properties.hasKey("content_type") && message_properties.getType("content_type") == ReadableType.String){
                        properties.contentType(message_properties.getString("content_type"));
                    }
                    if (message_properties.hasKey("content_encoding") && message_properties.getType("content_encoding") == ReadableType.String){
                        properties.contentEncoding(message_properties.getString("content_encoding"));
                    }
                    if (message_properties.hasKey("delivery_mode") && message_properties.getType("delivery_mode") == ReadableType.Number){
                        properties.deliveryMode(message_properties.getInt("delivery_mode"));
                    }
                    if (message_properties.hasKey("priority") && message_properties.getType("priority") == ReadableType.Number){
                        properties.priority(message_properties.getInt("priority"));
                    }
                    if (message_properties.hasKey("correlation_id") && message_properties.getType("correlation_id") == ReadableType.String){
                        properties.correlationId(message_properties.getString("correlation_id"));
                    }
                    if (message_properties.hasKey("expiration") && message_properties.getType("expiration") == ReadableType.String){ 
                        properties.expiration(message_properties.getString("expiration"));
                    }
                    if (message_properties.hasKey("message_id") && message_properties.getType("message_id") == ReadableType.String){
                        properties.messageId(message_properties.getString("message_id"));
                    }
                    if (message_properties.hasKey("type") && message_properties.getType("type") == ReadableType.String){
                        properties.type(message_properties.getString("type"));
                    }
                    if (message_properties.hasKey("user_id") && message_properties.getType("user_id") == ReadableType.String){
                        properties.userId(message_properties.getString("user_id"));
                    }
                    if (message_properties.hasKey("app_id") && message_properties.getType("app_id") == ReadableType.String){
                        properties.appId(message_properties.getString("app_id"));
                    }
                    if (message_properties.hasKey("reply_to") && message_properties.getType("reply_to") == ReadableType.String){
                        properties.replyTo(message_properties.getString("reply_to"));
                    }

                    //if (message_properties.hasKey("timestamp")){properties.timestamp(message_properties.getBoolean("timestamp"));}
                    //if (message_properties.hasKey("headers")){properties.expiration(message_properties.getBoolean("headers"))}

                //  } catch (Exception e){
                //     Log.e("RabbitMqExchange", "Exchange publish properties error " + e);
                //     e.printStackTrace();
                // }
            // }
            
            // if (message_headers != null){
                //  try {
                 
                    Map<String, Object> headers = new HashMap<String, Object>();
                    ReadableMapKeySetIterator iterator = message_headers.keySetIterator();
                    while (iterator.hasNextKey()) {
                        String key = iterator.nextKey();
                        headers.put(key, message_headers.getString(key));
                    }
                    properties.headers(headers);

                    sequenceNumber = this.channel.getNextPublishSeqNo();
                    this.outstandingConfirms.put(sequenceNumber, message_headers);
                    
                //  } catch (Exception e){
                //     Log.e("RabbitMqExchange", "Exchange publish headers error " + e);
                //     e.printStackTrace();
                // }
            // }
            
            this.channel.basicPublish(this.name, routing_key, properties.build(), message_body_bytes);
        } catch (Exception e){
            Log.e("RabbitMqExchange", "Exchange publish error " + e);
            this.outstandingConfirms.remove(sequenceNumber);
            WritableMap event = Arguments.createMap();
            event.putString("name", "publish-error");
            event.putString("description", e.getMessage());
            if (message_headers != null){
                event.merge(message_headers);
            }
            this.context.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class).emit("RabbitMqExchangeEvent", event);
        }

    }

    public void rpc(String routingKey, ReadableMap headers, ReadableMap properties, String message, Promise promise) {
        executorService.execute(new RPCWorker(this, routingKey, headers, properties, message, promise));
    }

    public void delete(Boolean ifUnused){ 
        try {
            this.channel.exchangeDelete(this.name, ifUnused);
        } catch (Exception e){
            Log.e("RabbitMqExchange", "Exchange delete error " + e);
            e.printStackTrace();
        }
    }

}

       
