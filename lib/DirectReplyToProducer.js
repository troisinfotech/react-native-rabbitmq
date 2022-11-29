export class DirectReplyToProducer {

    constructor(connection) {
        this.rabbitmqconnection = connection.rabbitmqconnection;
    }

    async publish(headers, message, routingKey) {
        headers = headers === null ? {} : headers;
        message = message === null ? '{}' : message;
        routingKey = routingKey === null ? '' : routingKey;
        return await this.rabbitmqconnection.publishDirectReplyToProducer(headers, message, routingKey);
    }

}

export default DirectReplyToProducer;
