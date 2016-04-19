"use strict";
var amqp = require("amqplib/callback_api");
var _ = require("lodash");
var url = require("url");
var EventEmitter = require("events");
var qs = require("querystring");

var JSON_TYPE = "application/json";

var defaultBehaviour = {
  reuse: "default",
  ack: false,
  confirm: false,
  heartbeat: 10,
  productName: getProductName()
};

var savedConns = {};

function connect(amqpUrl, behaviour, callback) {
  behaviour = _.assign(defaultBehaviour, behaviour);
  if (behaviour.reuse && attemptReuse(behaviour.reuse, callback)) {
    return;
  }
  return doConnect(amqpUrl, behaviour, callback);
}

function attemptReuse(key, callback) {
  var savedConn = savedConns[key];
  if (savedConn && savedConn.api) {
    callback(null, savedConn.api);
    return true;
  }

  if (savedConn) {
    savedConn.once("bootstrapped", function (error, api) {
      callback(error, api);
    });
    return true;
  }
  return false;
}

function doConnect(amqpUrl, behaviour, callback) {
  var urlObj = url.parse(amqpUrl);
  urlObj.search = qs.stringify(_.defaults(qs.parse(urlObj.search), {heartbeat: behaviour.heartbeat}));
  amqpUrl = url.format(urlObj);
  var api = _.extend(new EventEmitter(), {
    subscribe: subscribe,
    publish: publish,
    deleteQueue: deleteQueue,
    close: close
  });

  var channel = null;
  var conn = null;
  var reuse = new EventEmitter();
  if (behaviour.reuse) {
    savedConns[behaviour.reuse] = reuse;
  }

  var opts = {clientProperties: {product: behaviour.productName}};
  amqp.connect(amqpUrl, opts, function (connErr, newConnection) {
    if (connErr) {
      savedConns[behaviour.reuse] = null;
      reuse.emit("bootstrapped", connErr);
      return callback(connErr);
    }

    conn = newConnection;
    var onChannel = function (channelErr, newChannel) {
      if (channelErr) return callback(channelErr);
      channel = newChannel;
      assertExchange(channel, behaviour.exchange);
      channel.on("close", function (why) {
        savedConns[behaviour.reuse] = null;
        handleError(why || "Connection closed unexpectedly");
      });
      channel.on("error", function (amqpError) {
        savedConns[behaviour.reuse] = null;
        handleError(amqpError);
      });
      conn.on("error", function (amqpError) {
        savedConns[behaviour.reuse] = null;
        handleError(amqpError);
      });
      reuse.emit("bootstrapped", null, api);
      reuse.api = api;
      return callback(null, api);
    };
    if (behaviour.confirm) {
      conn.createConfirmChannel(onChannel);
    } else {
      conn.createChannel(onChannel);
    }
  });

  function publish(routingKey, message, pubCallback) {
    var encodedMsg = encode(message);
    channel.publish(behaviour.exchange, routingKey, encodedMsg.buffer, encodedMsg.props, pubCallback);
  }

  function subscribe(routingKeyOrKeys, queueName, handler, subCallback) {
    subCallback = subCallback || function () {};
    var routingKeys = Array.isArray(routingKeyOrKeys) ? routingKeyOrKeys : [routingKeyOrKeys];
    conn.createChannel(function (channelErr, subChannel) {
      if (channelErr) return subCallback(channelErr);
      subChannel.prefetch(behaviour.prefetch);
      assertExchange(subChannel, behaviour.exchange);
      subChannel.assertQueue(queueName, {}, function (queueErr) {
        if (queueErr) return subCallback(queueErr);
        routingKeys.forEach(function (key) {
          subChannel.bindQueue(queueName, behaviour.exchange, key, {}, function (bindErr) {
            if (bindErr) return subCallback(bindErr);
          });
        });
        var amqpHandler = function (message) {
          if (!message) return handleError("Subscription cancelled");
          var ackFun = function () { subChannel.ack(message); };
          handler(decode(message), message, {ack: ackFun});
        };
        var consumeOpts = {noAck: !behaviour.ack};
        subChannel.consume(queueName, amqpHandler, consumeOpts, subCallback);
      });
    });
  }

  function close(closeCallback) {
    if (channel) {
      channel.close();
    }
    if (conn) {
      conn.close(closeCallback);
    } else {
      closeCallback();
    }
  }

  function deleteQueue(queueName) {
    channel.deleteQueue(queueName);
  }

  function handleError(err) {
    api.emit("error", err);
  }

  return api;
}

function encode(body) {
  if (typeof body === "string") {
    return {buffer: new Buffer(body, "utf8")};
  } else if (body instanceof Buffer) {
    return {buffer: body};
  } else {
    return {
      props: {contentType: "application/json"},
      buffer: new Buffer(JSON.stringify(body), "utf8")
    };
  }
}

function decode(message) {
  var messageStr = message.content.toString("utf8");
  return (message.properties.contentType === JSON_TYPE) ? JSON.parse(messageStr) : messageStr;
}

function assertExchange(channel, exchange) {
  if (exchange) {
    channel.assertExchange(exchange, "topic");
  }
}

function getProductName() {
  try {
    var pkg = require(process.cwd() + "/package.json");
    return pkg.name + " " + pkg.version;
  } catch (e) {
    return "exp-amqp-connection";
  }
}

module.exports = connect;
