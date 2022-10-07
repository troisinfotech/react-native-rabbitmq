import { NativeModules, NativeEventEmitter } from 'react-native';

const { EventEmitter } = NativeModules;

export class Exchange {

    constructor(connection, exchange_config) {

        this.callbacks = {};
        this.rabbitmqconnection = connection.rabbitmqconnection;

        this.name = exchange_config.name;
        this.exchange_config = exchange_config;

        const RabbitMqEmitter = new NativeEventEmitter(EventEmitter);

        this.subscription = RabbitMqEmitter.addListener('RabbitMqExchangeEvent', this.handleEvent);

        this.rabbitmqconnection.addExchange(exchange_config);
    }

    handleEvent = (event) => {
        if (this.callbacks.hasOwnProperty(event.name)) {
            this.callbacks[event.name](event);
        }
    }

    on(event, callback) {
        this.callbacks[event] = callback;
    }

    removeOn(event) {
        delete this.callbacks[event];
    }

    publish(messageOrFilePath, isBlob, routing_key = '', properties = {}, headers = {}) {
        this.rabbitmqconnection.publishToExchange(messageOrFilePath, isBlob, this.name, routing_key, properties, headers);
    }

    delete() {
        this.rabbitmqconnection.deleteExchange(this.name);
    }

}

export default Exchange;
