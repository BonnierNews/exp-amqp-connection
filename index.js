"use strict";

const EventEmitter = require("events");
const crypto = require("crypto");
const async = require("async");
const bootstrap = require("./lib/bootstrap");
const transform = require("./lib/transform");

const TMP_Q_TTL = 60000;

const dummyLogger = {
  // eslint-disable-next-line no-console
  info: console.log,
  // eslint-disable-next-line no-console
  error: console.log
};

const defaultBehaviour = {
  ack: false,
  confirm: false,
  heartbeat: 10,
  productName: getProductName(),
  resubscribeOnError: true,
  queueArguments: {},
  prefetch: 20,
  logger: dummyLogger,
  configKey: "default",
  exchangeType: "topic",
  exchangeOptions: {}
};

function init(behaviour) {
  const api = new EventEmitter();
  const consumers = [];
  behaviour = Object.assign({}, defaultBehaviour, behaviour);

  // get connnection and add event listeners if it's brand new.
  const doBootstrap = function(callback) {
    bootstrap(behaviour, (bootstrapErr, bootstrapRes) => {
      if (bootstrapErr) api.emit("error", bootstrapErr);
      if (bootstrapRes && bootstrapRes.virgin) {
        bootstrapRes.connection.on("error", (err) => api.emit("error", err));
        bootstrapRes.connection.on("close", (why) => api.emit("error", why));
        api.emit("connected");
        bootstrapRes.pubChannel.on("error", (err) => api.emit("error", err));
        bootstrapRes.subChannel.on("error", (err) => api.emit("error", err));
        if (behaviour.exchange) {
           bootstrapRes.pubChannel.assertExchange(behaviour.exchange, behaviour.exchangeType, behaviour.exchangeOptions);
        }
      }
      callback(bootstrapErr, bootstrapRes);
    });
  };

  const doSubscribe = function(routingKeyOrKeys, queue, handler, attempt, cb) {
    doBootstrap((bootstrapErr, bootstrapRes) => {
      if (bootstrapErr) return; // Ok to ignore, emitted as error in doBootstrap()
      const routingKeys = Array.isArray(routingKeyOrKeys) ? routingKeyOrKeys : [routingKeyOrKeys];
      const subChannel = bootstrapRes.subChannel;
      subChannel.prefetch(behaviour.prefetch);
      const queueOpts = {
        durable: !!queue,
        autoDelete: !queue,
        exclusive: !queue,
        arguments: Object.assign(!queue ? { "x-expires": TMP_Q_TTL } : {}, behaviour.queueArguments)
      };
      const queueName = queue ? queue : `${getProductName()}-${getRandomStr()}`;
      subChannel.assertExchange(behaviour.exchange, behaviour.exchangeType, behaviour.exchangeOptions);
      subChannel.assertQueue(queueName, queueOpts);
      routingKeys.forEach((key) => subChannel.bindQueue(queueName, behaviour.exchange, key, {}));
      const amqpHandler = function(message) {
        if (!message) {
          api.emit("error", "Subscription cancelled");
        }
        const ackFun = () => subChannel.ack(message);
        const nackFun = (requeue) => subChannel.nack(message, false, requeue);
        let decodedMessage;
        try {
          decodedMessage = transform.decode(message);
        } catch (decodeErr) {
          behaviour.logger.error("Ignoring un-decodable message:", message, "reason:", decodeErr);
          if (behaviour.ack) {
            subChannel.ack(message);
          }
          return;
        }
        handler(decodedMessage, message, { ack: ackFun, nack: nackFun });
      };
      const consumeOpts = { noAck: !behaviour.ack };
      const wrappedCb = wrapConsumeCb(subChannel, cb);
      subChannel.consume(queueName, amqpHandler, consumeOpts, (err, ok) => {
        if (err) {
          api.emit("error", err);
          return wrappedCb(err);
        }
        api.emit("subscribed", { key: routingKeyOrKeys, queue: queueName, attempt });
        wrappedCb(err, ok);
      });
    });
  };

  api.subscribeTmp = function(routingKeyOrKeys, handler, cb) {
    api.subscribe(routingKeyOrKeys, undefined, handler, cb);
  };

  api.subscribe = function(routingKeyOrKeys, queue, handler, cb) {
    let resubTimer;
    let attempt = 1;
    const resubscribeOnError = (err) => {
      if (err && !resubTimer && behaviour.resubscribeOnError) {
        behaviour.logger.info("Amqp error received. Resubscribing in 5 secs.", err.message);
        resubTimer = setTimeout(() => {
          attempt = attempt + 1;
          doSubscribe(routingKeyOrKeys, queue, handler, attempt);
          resubTimer = null;
        }, 5000);
      }
    };

    doSubscribe(routingKeyOrKeys, queue, handler, attempt, cb);
    api.on("error", resubscribeOnError);
  };

  api.unsubscribeAll = function(cb) {
    cb = wrapCb(cb);
    async.series(
      consumers.map(({channel, consumerTag}) => (innerCb) =>
        channel.cancel(consumerTag, innerCb)
      ),
      (...args) => {
        consumers.slice(0, consumers.length);
        return cb(...args);
      }
    );
  };

  api.publish = function(routingKey, message, meta, cb) {
    if (typeof meta === "function") cb = meta;
    cb = wrapCb(cb || (() => {}));
    doBootstrap((bootstrapErr, bootstrapRes) => {
      if (bootstrapErr) {
        return cb(bootstrapErr);
      }
      const encodedMsg = transform.encode(message, meta);
      bootstrapRes.pubChannel.publish(behaviour.exchange, routingKey, encodedMsg.buffer, encodedMsg.props, cb);
    });
  };

  api.delayedPublish = function(routingKey, message, delay, meta, cb) {
    if (typeof meta === "function") cb = meta;
    cb = cb || function() {};
    doBootstrap((bootstrapErr, bootstrapRes) => {
      if (bootstrapErr) return cb(bootstrapErr);
      const name = `${behaviour.exchange}-exp-amqp-delayed-${delay}`;
      const channel = bootstrapRes.pubChannel;
      channel.assertExchange(name, "fanout", {
        durable: true,
        autoDelete: true
      });
      channel.assertQueue(name, {
        durable: true,
        autoDelete: true,
        arguments: {
          "x-dead-letter-exchange": behaviour.exchange,
          "x-message-ttl": delay,
          "x-expires": delay + 60000
        }
      });
      const encodedMsg = transform.encode(message, meta);
      async.series([
        function(done) {
          channel.bindQueue(name, name, "#", {}, done);
        },
        function(done) {
          channel.publish(name, routingKey, encodedMsg.buffer, encodedMsg.props, done);
        }
      ], cb);
    });
  };

  api.sendToQueue = function(queue, message, meta, cb) {
    if (typeof meta === "function") cb = meta;
    cb = wrapCb(cb);
    doBootstrap((bootstrapErr, bootstrapRes) => {
      if (bootstrapErr) return cb(bootstrapErr);
      const channel = bootstrapRes.pubChannel;
      const encodedMsg = transform.encode(message, meta);
      channel.sendToQueue(queue, encodedMsg.buffer, encodedMsg.props, cb);
    });
  };

  api.deleteQueue = function(queue, cb) {
    cb = wrapCb(cb || (() => {}));
    doBootstrap((bootstrapErr, bootstrapRes) => {
      if (bootstrapErr) return cb(bootstrapErr);
      const channel = bootstrapRes.pubChannel;
      channel.deleteQueue(queue, cb);
    });
  };

  api.purgeQueue = function(queue, cb) {
    cb = wrapCb(cb || (() => {}));
    doBootstrap((bootstrapErr, bootstrapRes) => {
      if (bootstrapErr) return cb(bootstrapErr);
      const channel = bootstrapRes.pubChannel;
      channel.purgeQueue(queue, cb);
    });
  };

  api.shutdown = function(queue, cb) {
    cb = wrapCb(cb || (() => {}));
    doBootstrap((bootstrapErr, bootstrapRes) => {
      if (bootstrapErr) return cb(bootstrapErr);
      const channel = bootstrapRes.pubChannel;
      channel.purgeQueue(queue, cb);
    });
  };


  api.shutdown = function(cb) {
    cb = wrapCb(cb || (() => {}));
    doBootstrap((err, res) => {
      if (err) {
        return cb(err);
      }
      res.connection.close(cb);
    });
  };

  function wrapConsumeCb(channel, cb) {
    cb = wrapCb(cb);
    return (err, ok) => {
      if (!err) {
        const {consumerTag} = ok;
        consumers.push({channel, consumerTag});
      }
      cb(err, ok);
    };
  }

  function wrapCb(cb) {
    let wrappedCb;
    if (!cb) {
      wrappedCb = function() {};
    } else {
      wrappedCb = (...args) => {
        try {
          return cb(...args);
        } catch (e) {
          api.emit("callback_error", e);
          return e;
        }
      };
    }
    return wrappedCb;
  }

  return api;
}

function getProductName() {
  try {
    const pkg = require(`${process.cwd()}/package.json`);
    const nodeEnv = (process.env.NODE_ENV || "development");
    return `${pkg.name}-${nodeEnv}`;
  } catch (e) {
    return "exp-amqp-connection";
  }
}

function getRandomStr() {
  return crypto.randomBytes(20).toString("hex").slice(1, 8);
}

module.exports = init;
