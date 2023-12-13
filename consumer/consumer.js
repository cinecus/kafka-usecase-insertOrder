const { Order, Product, sequelize } = require('./schema')
const { v4: uuidv4 } = require('uuid')
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'order-consumer',
    brokers: ['localhost:9092'], // Update with your Kafka broker addresses
});

const consumer = kafka.consumer({
    groupId: 'order-group',
    //  retry: {
    //     initialRetryTime: 100,
    //     retries: 5,
    // },
});

const kafkaDlq = new Kafka({
    clientId: 'order-service-dlq',
    brokers: ['localhost:9092'],
})

const producerDql = kafkaDlq.producer()

async function receiveOrders(orderMsg) {

    const { productId, userId } = orderMsg
  //  await new Promise((resolve) => setTimeout(resolve, 1000));
    const randomNumber = Math.floor((Math.random() * 10) + 1);
    if (randomNumber > 5) {
        throw new Error('error na')
    }
    console.log(" [x] Received %s", orderMsg)

    const product = await Product.findOne({
        where: {
            id: productId
        }
    })

    // if (product.amount <= 0) {
    // }

    // reduce amount
    product.amount -= 1
    await product.save()

    // create order with status pending
    const order = await Order.create({
        productId: product.id,
        orderId: uuidv4(),
        userLineUid: userId,
        status: 'pending'
    })

    console.log("Order saved to database with id:", order.orderId)
}


const runConsumer = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: 'order-topic', fromBeginning: false });

    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message, heartbeat }) => {
            const order = JSON.parse(message.value.toString());
            try {
                console.log({
                    partition,
                    offset: message.offset,
                    value: message.value.toString(),
                });
                await receiveOrders(order)

                await consumer.commitOffsets([
                    {
                        topic,
                        partition,
                        offset: (Number(message.offset) + 1).toString(),
                    }
                ]);


            } catch (error) {
                console.error('Error processing Kafka message:', error);
                await producerDql.send({
                    topic: 'order-dlq-topic',
                    messages: [{ value: JSON.stringify(order) }],
                });
            }
        },
    },);
};


const startApp = async () => {
    try {
        await sequelize.sync()
        await producerDql.connect()
        await runConsumer()

    } catch (error) {
        console.log(error);
    }
}

startApp()