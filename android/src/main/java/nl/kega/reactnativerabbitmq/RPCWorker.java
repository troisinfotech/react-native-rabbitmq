package nl.kega.reactnativerabbitmq;

import static java.util.Objects.isNull;

import android.util.Log;

import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReadableMap;
import com.facebook.react.bridge.ReadableType;
import com.facebook.react.bridge.WritableMap;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

public class RPCWorker implements Runnable {

    private static final String rpcQueue = "amq.rabbitmq.reply-to";

    private RabbitMqExchange exchange;
    private String routingKey; 
    private ReadableMap headers; 
    private ReadableMap properties; 
    private String message; 
    private Promise promise;
    private String consumerTag;

    public RPCWorker() {}

    public RPCWorker(RabbitMqExchange exchange, String routingKey, ReadableMap headers, ReadableMap properties, String message, Promise promise) {
        this.exchange = exchange;
        this.routingKey = routingKey;
        this.headers = headers;
        this.properties = properties;
        this.message = message;
        this.promise = promise;
        this.consumerTag = null;
    }

    @Override
    public void run() {
        try {
            AMQP.BasicProperties.Builder amqpPropertiesBuilder = new AMQP.BasicProperties.Builder();
            amqpPropertiesBuilder.headers(MapUtils.toHashMap(headers));
            amqpPropertiesBuilder.replyTo(rpcQueue);
            if (properties.hasKey("delivery_mode") && properties.getType("delivery_mode") == ReadableType.Number) {
                amqpPropertiesBuilder.deliveryMode(properties.getInt("delivery_mode"));
            }
            final CompletableFuture<String> response = new CompletableFuture<>();
            consumerTag = exchange.rpcChannel.basicConsume(rpcQueue, true, new DefaultConsumer(exchange.rpcChannel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties amqpProperties, byte[] body) throws IOException {
                    response.complete(new String(body, "UTF-8"));
                }
            });
            exchange.rpcChannel.basicPublish(exchange.name, routingKey, amqpPropertiesBuilder.build(), message.getBytes("UTF-8"));
            String data = response.get(exchange.rpcTimeout, TimeUnit.SECONDS);
            JSONObject jsonObject = new JSONObject(data);
            WritableMap result = MapUtils.convertJsonToMap(jsonObject);
            promise.resolve(result);
        } catch (TimeoutException e) {
            Log.e("RPCWorker", "Error: RPC timeout");
            promise.reject("RPCWorker", "Error: RPC timeout");
        } catch (Exception e) {
            Log.e("RPCWorker", "Error: " + e.getMessage());
            promise.reject("RPCWorker", "Error: " + e.getMessage());
        } finally {
            if (!isNull(consumerTag)) { 
                try {
                    exchange.rpcChannel.basicCancel(consumerTag);
                } catch (Exception e) {
                    Log.e("RPCWorker", "Error: " + e.getMessage());
                }
            }
        }
    }

}