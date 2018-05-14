/**
 *  This library requires a ./config/default.json file in your root folder
 *  with following keys:
 *
 "kafka": {
    "brokersList": ["localhost:9092"],
    "consumerGroupId": "tasksClient",
    "clientId": "test",
    "topics": ["three"]
 }
 *
 *  otherwise it calls a provided callback function with an error.
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under no licenses and is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

/**
 * Author - Sanzhar Kuliev
 * Materials - Property of Askartec LLC
 */
const Kafka = require('node-rdkafka');
const config = require('config');

let consumer = null;

let consumerConfigs = null;

let isReady = false;
let initCallback = null;
let messageCallback = null;

exports.init = (initCB, msgCB) => {
    initCallback = initCB;
    if (config.has('kafka')) {
        try {
            consumerConfigs = config.get('kafka')
        } catch (err) {
            console.error('Error reading kafka configs: ', err)
            let error = new Error('Error reading kafka configs: ' + err);
            error.code = -1;
            initCallback(error);
            return;
        }
    } else {
        console.error('Kafka configs are missing from the configuration file!');
        let error = new Error('Kafka configs are missing from the configuration file!');
        error.code = -1;
        initCallback(error);
        return;
    }

    messageCallback = msgCB;

    isReady = true;
    connect();
};

const connect = () => {
    let options = {
        'group.id': consumerConfigs.consumerGroupId,
        'metadata.broker.list': consumerConfigs.brokersList,
        'enable.auto.commit': false,
    };

    consumer = new Kafka.KafkaConsumer(options);
    consumer.connect({}, (err)=> {
        initCallback(err);
    });

    consumer
        .on('ready', () => {
            consumer.subscribe(consumerConfigs.topics);
            consumer.consume();
            initCallback(null);
        })
        .on('data', onMessage)
        .on('event.error', onError);
};

const onMessage = (message) => {
    messageCallback(message);
};

const onError = (err) => {
    console.error('Error: ', err)
};

process.once('SIGINT', () => {
    if(consumer != null) {
        consumer.disconnect();
    }
});