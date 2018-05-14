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
 * Author - Sanzhar Kuliev
 * Materials - Property of Askartec LLC
 */

const Kafka = require('node-rdkafka');
const config = require('config');
const fs = require('fs');
const path = require('path');

const queueDir = './pendingQueue/';

let producerConfigs = null;
let producer = null;
let initCallback = null;

let isConnected = false;
let queue = [];
let currQueueFile = '';
let messagesWrittenToFile = 0;
let numOfFilesPending = 0;

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

const writeToFile = () => {
    let fileQueue = queue;
    let qlen = fileQueue.length;

    currQueueFile = queueDir + 'kafka' + numOfFilesPending.toString() + '.txt';

    for (let i = 0; i < qlen; i++) {
        try {
            fs.appendFileSync(currQueueFile, (JSON.stringify(fileQueue[i])) + '\n');
            messagesWrittenToFile++;
        }
        catch
            (err) {
            console.error('Error writing to file:', err);
            return;
        }
    }

    queue = [];

    numOfFilesPending++;
};

const sendFromFiles = () => {
    let files = {};

    try {
        files = fs.readdirSync(queueDir);
    } catch (err) {
        console.error('Error reading directory ' + queueDir + ' :', err);
        return;
    }

    if (!files) return;

    for (let i in files) {
        let isReadError = false;
        let currFile = files[i];

        if (path.extname(currFile) !== '.txt') continue;

        let currMsg = [];
        let currFilePath = path.join(queueDir, currFile);

        try {
            currMsg = fs.readFileSync(currFilePath, 'utf-8').split('\n');
        } catch (err) {
            console.error('Can\'t open ' + currFile + ' file: ', err);
            isReadError = true;
        }

        if (!isReadError) {
            for (let i in currMsg) {
                if (currMsg[i] === '') continue;

                let line = {};
                try {
                    line = JSON.parse(currMsg[i]);
                } catch (err) {
                    console.error('Error reading log record: ', err);
                    continue;
                }

                exports.send(line.topic, line.value.data.toString(), line.key)
            }
        }
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

    //required for delivery-report event
    producer.setPollInterval(1000);

    producer
        .on('ready', onReady)
        .on('event.error', onError)

        //In case we lost connection to Kafka a 'delivery-report' with an error is triggered
        //We don't want to lose the messages, hence we store them in a local file at `queueDir = './pendingQueue/'`
        //until we are back online with a Kafka cluster
        .on('delivery-report', (err, report) => {
            if (err) {
                onError();
                if (queue.length >= producerConfigs.queueLimit) {
                    writeToFile();
                }
                else {
                    queue.push(report);
                }
            } else {
                onBackOnline();
            }
        })
};

const onBackOnline = () => {
    isConnected = true;
    exports.isReady = true;
    sendFromFiles();
};

const onError = (err) => {
    isConnected = false;
    exports.isReady = false;
};

const onReady = () => {
    isConnected = true;
    exports.isReady = true;
    initCallback(null);
};
