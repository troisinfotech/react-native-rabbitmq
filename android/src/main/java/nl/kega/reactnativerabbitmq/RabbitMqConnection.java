package nl.kega.reactnativerabbitmq;

import android.util.Log;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.facebook.react.modules.core.DeviceEventManagerModule;

import com.facebook.react.bridge.Arguments;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContextBaseJavaModule;
import com.facebook.react.bridge.ReactMethod;
import com.facebook.react.bridge.Callback;
import com.facebook.react.bridge.Promise;
import com.facebook.react.bridge.ReadableMap;
import com.facebook.react.bridge.WritableArray;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.bridge.GuardedAsyncTask;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.RecoveryListener;
import com.rabbitmq.client.Recoverable;
import com.rabbitmq.client.RecoverableConnection;
import com.rabbitmq.client.ConfirmListener;

class RabbitMqConnection extends ReactContextBaseJavaModule {

    private ReactApplicationContext context;

    public ReadableMap config;

    private ConnectionFactory factory = null;
    private RecoverableConnection connection;
    private Channel channel;
    private Channel rpcChannel;

    private Callback status;
    
    private ExecutorService executorService;

    private ArrayList < RabbitMqQueue > queues = new ArrayList < RabbitMqQueue > ();
    private ArrayList < RabbitMqExchange > exchanges = new ArrayList < RabbitMqExchange > ();

    private ConcurrentNavigableMap < Long, ReadableMap > outstandingConfirms = new ConcurrentSkipListMap < > ();

    public RabbitMqConnection(ReactApplicationContext reactContext) {
        super(reactContext);

        this.context = reactContext;

        this.executorService = Executors.newSingleThreadExecutor();

    }

    @Override
    public String getName() {
        return "RabbitMqConnection";
    }

    @ReactMethod
    public void initialize(ReadableMap config) {
        this.config = config;

        this.factory = new ConnectionFactory();
        this.factory.setUsername(this.config.getString("username"));
        this.factory.setPassword(this.config.getString("password"));
        this.factory.setHost(this.config.getString("host"));
        this.factory.setPort(this.config.getInt("port"));
        if (this.config.hasKey("virtualhost")) {
            this.factory.setVirtualHost(this.config.getString("virtualhost"));
        } else {
            this.factory.setVirtualHost("/");
        }
        this.factory.setAutomaticRecoveryEnabled(true);
        this.factory.setRequestedHeartbeat(10);

        try {
            if (this.config.hasKey("ssl") && this.config.getBoolean("ssl")) {
                this.factory.useSslProtocol();
            }
        } catch (Exception e) {
            Log.e("RabbitMqConnection", e.toString());
        }

    }

    @ReactMethod
    public void status(Callback onStatus) {
        this.status = onStatus;
    }

    @ReactMethod
    public void connect() {

        if (this.connection != null && this.connection.isOpen()) {
            WritableMap event = Arguments.createMap();
            event.putString("name", "connected");

            this.context.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class).emit("RabbitMqConnectionEvent", event);
        } else {

            try {
                this.connection = (RecoverableConnection) this.factory.newConnection();
            } catch (Exception e) {

                Log.e("RabbitMqConnection", "Connect error " + e);
                WritableMap event = Arguments.createMap();
                event.putString("name", "error");
                event.putString("description", e.getMessage());

                this.context.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class).emit("RabbitMqConnectionEvent", event);

                this.connection = null;

            }

            if (this.connection != null) {

                try {

                    this.connection.addShutdownListener(new ShutdownListener() {
                        @Override
                        public void shutdownCompleted(ShutdownSignalException cause) {
                            Log.e("RabbitMqConnection", "Shutdown signal received " + cause);
                            onClose(cause);
                        }
                    });

                    this.connection.addRecoveryListener(new RecoveryListener() {

                        @Override
                        public void handleRecoveryStarted(Recoverable recoverable) {
                            Log.e("RabbitMqConnection", "RecoveryStarted " + recoverable);
                        }

                        @Override
                        public void handleRecovery(Recoverable recoverable) {
                            Log.e("RabbitMqConnection", "Recoverable " + recoverable);
                            onRecovered();
                        }

                    });

                    this.channel = connection.createChannel();
                    this.channel.confirmSelect();
                    this.channel.basicQos(1);

                    this.channel.addConfirmListener(new ConfirmListener() {

                        @Override
                        public void handleAck(long deliveryTag, boolean multiple) throws IOException {

                            Log.i("RabbitMqConnection", "Ack received");
                            processConfirmation(deliveryTag, multiple, "ack");

                        }

                        @Override
                        public void handleNack(long deliveryTag, boolean multiple) throws IOException {

                            Log.i("RabbitMqConnection", "Not ack received");
                            processConfirmation(deliveryTag, multiple, "nack");

                        }

                    });

                    this.rpcChannel = connection.createChannel();

                    WritableMap event = Arguments.createMap();
                    event.putString("name", "connected");

                    this.context.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class).emit("RabbitMqConnectionEvent", event);

                } catch (Exception e) {

                    Log.e("RabbitMqConnectionChannel", "Create channel error " + e);
                    e.printStackTrace();

                }
            }
        }
    }

    public void processConfirmation(Long deliveryTag, boolean multiple, String eventName) {
        if (multiple) {
            ConcurrentNavigableMap < Long, ReadableMap > confirmed = this.outstandingConfirms.headMap(
                deliveryTag, true
            );
            Iterator < Long > iterator = confirmed.keySet().iterator();
            while (iterator.hasNext()) {
                Long key = iterator.next();
                WritableMap event = Arguments.createMap();
                event.putString("name", eventName);
                event.merge(confirmed.get(key));
                this.context.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class).emit("RabbitMqConnectionEvent", event);
            }
            confirmed.clear();
        } else {
            WritableMap event = Arguments.createMap();
            event.putString("name", eventName);
            event.merge(this.outstandingConfirms.get(deliveryTag));
            this.context.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class).emit("RabbitMqConnectionEvent", event);
            this.outstandingConfirms.remove(deliveryTag);
        }
    }

    @ReactMethod
    public void addQueue(ReadableMap queue_config, ReadableMap arguments) throws IOException {
        RabbitMqQueue queue = new RabbitMqQueue(this.context, this.channel, queue_config, arguments);
        this.queues.add(queue);
    }

    @ReactMethod
    public void bindQueue(String exchange_name, String queue_name, String routing_key) {

        RabbitMqQueue found_queue = null;
        for (RabbitMqQueue queue: queues) {
            if (Objects.equals(queue_name, queue.name)) {
                found_queue = queue;
            }
        }

        RabbitMqExchange found_exchange = null;
        for (RabbitMqExchange exchange: exchanges) {
            if (Objects.equals(exchange_name, exchange.name)) {
                found_exchange = exchange;
            }
        }

        if (!found_queue.equals(null) && !found_exchange.equals(null)) {
            found_queue.bind(found_exchange, routing_key);
        }
    }

    @ReactMethod
    public void unbindQueue(String exchange_name, String queue_name, String routing_key) {

        RabbitMqQueue found_queue = null;
        for (RabbitMqQueue queue: queues) {
            if (Objects.equals(queue_name, queue.name)) {
                found_queue = queue;
            }
        }

        RabbitMqExchange found_exchange = null;
        for (RabbitMqExchange exchange: exchanges) {
            if (Objects.equals(exchange_name, exchange.name)) {
                found_exchange = exchange;
            }
        }

        if (!found_queue.equals(null) && !found_exchange.equals(null)) {
            found_queue.unbind(routing_key);
        }
    }

    @ReactMethod
    public void removeQueue(String queue_name) {
        RabbitMqQueue found_queue = null;
        for (RabbitMqQueue queue: queues) {
            if (Objects.equals(queue_name, queue.name)) {
                found_queue = queue;
            }
        }

        if (!found_queue.equals(null)) {
            found_queue.delete();
        }
    }

    @ReactMethod
    public void basicAck(String queue_name, Double delivery_tag) {
        RabbitMqQueue found_queue = null;
        for (RabbitMqQueue queue: queues) {
            if (Objects.equals(queue_name, queue.name)) {
                found_queue = queue;
            }
        }

        if (!found_queue.equals(null)) {
            long long_delivery_tag = Double.valueOf(delivery_tag).longValue();
            found_queue.basicAck(long_delivery_tag);
        }


    }

    /*
    @ReactMethod
    public void publishToQueue(String message, String exchange_name, String routing_key) {

        for (RabbitMqQueue queue : queues) {
		    if (Objects.equals(exchange_name, queue.exchange_name)){
                Log.e("RabbitMqConnection", "publish " + message);
                queue.publish(message, exchange_name);
                return;
            }
		}

    }
    */

    @ReactMethod
    public void addExchange(ReadableMap exchange_config) throws IOException {

        RabbitMqExchange exchange = new RabbitMqExchange(this.context, this.channel, this.rpcChannel, this.executorService, this.outstandingConfirms, exchange_config);
        this.exchanges.add(exchange);

    }

    @ReactMethod
    public void publishExchange(String messageOrFilePath, boolean isBlob, String exchange_name, String routing_key, ReadableMap message_properties, ReadableMap message_headers) {

        for (RabbitMqExchange exchange: exchanges) {
            if (Objects.equals(exchange_name, exchange.name)) {
                exchange.publish(messageOrFilePath, isBlob, routing_key, message_properties, message_headers);
                return;
            }
        }

    }

    @ReactMethod
    public void directReplyToExchange(String exchangeName, String routingKey, ReadableMap headers, ReadableMap properties, String message, Promise promise) {

        for (RabbitMqExchange exchange: exchanges) {
            if (Objects.equals(exchangeName, exchange.name)) {
                exchange.rpc(routingKey, headers, properties, message, promise);
                return;
            }
        }

    }

    @ReactMethod
    public void deleteExchange(String exchange_name, Boolean if_unused) {

        for (RabbitMqExchange exchange: exchanges) {
            if (Objects.equals(exchange_name, exchange.name)) {
                exchange.delete(if_unused);
                return;
            }
        }

    }

    @ReactMethod
    public void close() {
        try {

            this.queues = new ArrayList < RabbitMqQueue > ();
            this.exchanges = new ArrayList < RabbitMqExchange > ();

            this.channel.close();

            this.connection.close();
        } catch (Exception e) {
            Log.e("RabbitMqConnection", "Connection closing error " + e);
            e.printStackTrace();
        } finally {
            this.connection = null;
            this.factory = null;
            this.channel = null;
        }
    }

    private void onClose(ShutdownSignalException cause) {
        Log.e("RabbitMqConnection", "Closed");

        WritableMap event = Arguments.createMap();
        event.putString("name", "closed");
        event.putString("description", cause.getMessage());

        this.context.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class).emit("RabbitMqConnectionEvent", event);
    }

    private void onRecovered() {
        Log.i("RabbitMqConnection", "Recovered");

        WritableMap event = Arguments.createMap();
        event.putString("name", "reconnected");

        this.context.getJSModule(DeviceEventManagerModule.RCTDeviceEventEmitter.class).emit("RabbitMqConnectionEvent", event);
    }

    @Override
    public void onCatalystInstanceDestroy() {

        // serialize on the AsyncTask thread, and block
        try {
            new GuardedAsyncTask < Void, Void > (getReactApplicationContext()) {
                @Override
                protected void doInBackgroundGuarded(Void...params) {
                    close();
                }
            }.execute().get();
        } catch (InterruptedException ioe) {
            Log.e("RabbitMqConnection", "onCatalystInstanceDestroy", ioe);
        } catch (ExecutionException ee) {
            Log.e("RabbitMqConnection", "onCatalystInstanceDestroy", ee);
        }
    }

}
