const { Kafka } = require('kafkajs');

class MessageProducer {
    constructor() {
        this.kafka = new Kafka({
            clientId: 'benchmark-producer',
            brokers: [
                process.env.KAFKA_BOOTSTRAP_SERVERS_1 || '10.42.53.125:19092',
                process.env.KAFKA_BOOTSTRAP_SERVERS_2 || '10.42.53.126:19092',
                process.env.KAFKA_BOOTSTRAP_SERVERS_3 || '10.42.53.127:19092'
            ]
        });
        
        this.producer = this.kafka.producer({
            maxInFlightRequests: 1,
            idempotent: false,
            transactionTimeout: 30000,
        });
        
        this.messagesPerSecond = parseInt(process.env.MESSAGES_PER_SECOND) || 1000;
        this.topicName = process.env.TOPIC_NAME || 'disburse-commands';
        this.messageCount = 0;
        this.startTime = null;
    }

    async initialize() {
        console.log('Connecting to Kafka...');
        await this.producer.connect();
        console.log('Connected to Kafka');
        
        // Create topic if it doesn't exist
        const admin = this.kafka.admin();
        await admin.connect();
        
        try {
            await admin.createTopics({
                topics: [{
                    topic: this.topicName,
                    numPartitions: 3,
                    replicationFactor: 1
                }]
            });
            console.log(`Topic ${this.topicName} created successfully`);
        } catch (error) {
            console.log(`Topic creation result: ${error.message}`);
        }
        
        await admin.disconnect();
    }

    generateMessage() {
        const messageId = `msg-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        
        // Generate message in format expected by consumers
        const disburseCommand = {
            amount: 1000 + Math.random() * 9000, // Random amount between 1000-10000
            loanId: this.generateUUID(),
            memberId: this.generateUUID(),
            correlationId: this.generateUUID(),
            userContext: {
                userId: this.generateUUID(),
                applicationId: this.generateUUID(),
                sessionId: this.generateUUID(),
                tenantId: this.generateUUID(),
                verticalId: this.generateUUID(),
                serviceId: "benchmark-service",
                email: "test@example.com",
                phoneNumber: "+1234567890",
                userName: "testuser",
                displayName: "Test User",
                language: "en",
                roles: ["user"]
            }
        };
        
        return JSON.stringify(disburseCommand);
    }

    generateUUID() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            var r = Math.random() * 16 | 0, v = c == 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }

    async startProducing() {
        this.startTime = Date.now();
        console.log(`Starting to produce ${this.messagesPerSecond} messages per second to topic: ${this.topicName}`);
        
        const interval = 1000 / this.messagesPerSecond;
        let lastSent = Date.now();
        
        const sendMessages = async () => {
            const now = Date.now();
            const elapsed = now - lastSent;
            
            if (elapsed >= interval) {
                const messagesToSend = Math.floor(elapsed / interval);
                const messages = [];
                

                for (let i = 0; i < 1000; i++) {
                    messages.push({
                        value: Buffer.from(this.generateMessage(), 'utf-8')
                    });
                    this.messageCount++;
                }
                
                try {
                    await this.producer.send({
                        topic: this.topicName,
                        messages: messages
                    });
                    
                    lastSent = now;
                    
                    // Log stats every 10 seconds
                    if (this.messageCount % (this.messagesPerSecond * 10) === 0) {
                        const runtime = (Date.now() - this.startTime) / 1000;
                        const rate = this.messageCount / runtime;
                        console.log(`Sent ${this.messageCount} messages in ${runtime.toFixed(1)}s (${rate.toFixed(1)} msg/s)`);
                    }
                } catch (error) {
                    console.error('Error sending messages:', error);
                }
            }
            
            setImmediate(sendMessages);
        };
        
        sendMessages();
    }

    async shutdown() {
        console.log('Shutting down producer...');
        await this.producer.disconnect();
        const runtime = (Date.now() - this.startTime) / 1000;
        console.log(`Final stats: ${this.messageCount} messages in ${runtime.toFixed(1)}s (${(this.messageCount / runtime).toFixed(1)} msg/s)`);
    }
}

// Main execution
async function main() {
    const producer = new MessageProducer();
    
    // Handle graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\nReceived SIGINT, shutting down gracefully...');
        await producer.shutdown();
        process.exit(0);
    });
    
    process.on('SIGTERM', async () => {
        console.log('\nReceived SIGTERM, shutting down gracefully...');
        await producer.shutdown();
        process.exit(0);
    });
    
    try {
        await producer.initialize();
        await producer.startProducing();
    } catch (error) {
        console.error('Error in producer:', error);
        process.exit(1);
    }
}

main();