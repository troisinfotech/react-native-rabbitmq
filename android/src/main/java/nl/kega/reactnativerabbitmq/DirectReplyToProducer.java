package nl.kega.reactnativerabbitmq;

import android.os.Bundle;
import android.util.Log;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.facebook.react.bridge.Arguments;
import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReadableMap;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.bridge.ReadableMapKeySetIterator;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.AMQP;

import org.json.JSONObject;

public class DirectReplyToProducer {

    private ReactApplicationContext context;
    private Channel channel;

    private static final String replyQueue = "amq.rabbitmq.reply-to";

    public DirectReplyToProducer (ReactApplicationContext context, Channel channel) {
        this.context = context;
        this.channel = channel;
    } 

    public void publish(ReadableMap headers, String message, String routingKey, Promise promise) {
        try {
            Map<String, Object> transformed = new HashMap<String, Object>();
            ReadableMapKeySetIterator iterator = headers.keySetIterator();
            while (iterator.hasNextKey()) {
                String key = iterator.nextKey();
                transformed.put(key, headers.getString(key));
            }
            AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .headers(transformed)
                .replyTo(replyQueue)
                .build();
            final CompletableFuture<String> completableFuture = new CompletableFuture<>();
            String ctag = this.channel.basicConsume(replyQueue, true, (consumerTag, delivery) -> {
                completableFuture.complete(new String(delivery.getBody(), "UTF-8"));
            }, consumerTag -> {
            });
            this.channel.basicPublish("", routingKey, props, message.getBytes("UTF-8"));
            String response = completableFuture.get();
            this.channel.basicCancel(ctag);
            JSONObject jsonObject = new JSONObject(response);
            BundleJSONConverter bundleJSONConverter = new BundleJSONConverter();
            Bundle bundle = bundleJSONConverter.convertToBundle(jsonObject);
            WritableMap map = Arguments.fromBundle(bundle);
            promise.resolve(map);
        } catch (Exception e){
            Log.e("DirectReplyToProducer", "DirectReplyToProducer publish error " + e);
            promise.reject("DirectReplyToProducer", "DirectReplyToProducer publish error", e);
        }
    }

}
