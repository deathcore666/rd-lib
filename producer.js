/**
 *  This library requires a ./config/default.json file in your root folder
 *  with following keys:
 *
 "kafka": {
    "brokersList": ["localhost:9092"],
    "consumerGroupId": "tasksClient",
    "clientId": "test",
    "topics": ["three"],
    "msgPerFile": 500
 }
 *
 *  otherwise it calls a provided callback function with an error.
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under no licenses and is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

/**
 * Materials - Property of Askartec LLC
 */

const Kafka = require('node-rdkafka');
const config = require('config');

let producerConfigs = null;
let producer = null;
let initCallback = null;

exports.isReady = false;

exports.init = (initCB) => {
    initCallback = initCB;
    if (config.has('kafka')) {
        try {
            producerConfigs = config.get('kafka')
        } catch (err) {
            console.error('Error reading kafka configs: ', err);
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

    connect();
};

exports.send = (_topic, _msg, _key = null) => {
    try {
        producer.produce(
            //topic
            _topic,
            //partition, default is -1
            -1,
            //message
            new Buffer(_msg),
            //key
            _key,
            //timestamp
            Date.now()
        );
    } catch (err) {
        console.error('Error sending messages to Kafka: ', err);
    }
};


const connect = () => {
    let options = {
        'client.id': producerConfigs.clientId,
        'metadata.broker.list': producerConfigs.brokersList,
        'compression.codec': 'none',
        'retry.backoff.ms': 200,
        'message.send.max.retries': 10,
        'socket.keepalive.enable': true,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.ms': 1000,
        'dr_cb': true
    };

    let topicOpts = {
        'request.required.acks': -1
    };

    producer = new Kafka.Producer(options, topicOpts);

    producer.connect({}, (err) => {
        if (err)
            initCallback(err);
    });

    //required for delivery-report event to be heard
    producer.setPollInterval(1000);

    producer
        .on('ready', onReady)
        .on('event.error', onError)
        .on('delivery-report', (err, report) => {
            if (err) {
                console.error(err);
            }
        })
};

const onError = (err) => {
    exports.isReady = false;
    // initCallback("Kafka producer has encountered an error: " + err);
};

const onReady = () => {
    exports.isReady = true;
    initCallback(null);
};
