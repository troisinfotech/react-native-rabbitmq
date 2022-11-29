export class DirectReplyToProducer {

    constructor(connection) {
        this.rabbitmqconnection = connection.rabbitmqconnection;
    }

    async publish(headers, message, routingKey) {
        headers = headers || {};
        message = message || '{}';
        routingKey = routingKey || '';
        return this.rabbitmqconnection.publishDirectReplyToProducer(headers, message, routingKey);
    }

}

export default DirectReplyToProducer;
