"use strict";

var request = require("request");
var amqp = require("../index.js");
var crypto = require("crypto");
var assert = require("assert");
var async = require("async");
var util = require("util");
var extend = require("../extend");

var defaultBehaviour = {exchange: "e1", logger: console, consumerCancelNotification: true};
var defaultConnOpts = {};
var connection;

Feature("Connect", function () {

  Scenario("Ok connection", function () {
    after(disconnect);
    When("Trying to connect to default port", function () {
      // Noopt
    });
    Then("We should bet able to connect", function (done) {
      connect(defaultConnOpts, defaultBehaviour, ignoreErrors(done));
    });
    And("The connection should be ok", testConnection);

  });

  Scenario("Bad connection", function () {
    after(disconnect);
    When("Trying to connect to bad port", function () {
    });
    Then("We should get an error", function (done) {
      connect({port: 9999}, extend(defaultBehaviour, {logger: null}), ensureErrors(done));
    });
  });

  Scenario("Reconnect", function () {
    after(disconnect);
    And("We have a connection", function (done) {
      connect(defaultConnOpts, extend(defaultBehaviour, {logger: null}), ignoreErrors(done));
    });
    And("And we kill all rabbit connections", killRabbitConnections);
    Then("The connection should be ok", testConnection);
  });

});

Feature("Pubsub", function () {
  Scenario("Ok pubsub", function () {
    var message;
    after(disconnect);
    And("We have a connection", function (done) {
      connect(defaultConnOpts, defaultBehaviour, ignoreErrors(done));
    });
    And("We create a subscription", function (done) {
      connection.subscribe("testRoutingKey", "testQ", function (msg) {
        message = msg;
      }, done);
    });
    And("We publish a message", function (done) {
      connection.publish("testRoutingKey", {testData: "hello"}, done);
    });
    Then("It should arrive correctly", function () {
      assert.equal(message.testData, "hello");
    });
  });

  Scenario("Cancelled sub", function () {
    var message;
    after(disconnect);
    And("We have a connection", function (done) {
      connect(defaultConnOpts, defaultBehaviour, ignoreErrors(done));
    });
    And("We create a subscription", function (done) {
      connection.subscribe("testRoutingKey", "testQ", function (msg) {
        message = msg;
      }, done);
    });
    And("We delete the queue", function (done) {
      deleteRabbitQueue("testQ", done);
    });
    And("We wait a little", function (done) {setTimeout(done, 3000);});
    And("We publish a message", function (done) {
      connection.publish("testRoutingKey", {testData: "hello"}, done);
    });
    Then("It should arrive correctly", function () {
      assert.equal(message.testData, "hello");
    });
  });
});

Feature("Dead letter exchange", function () {
  Scenario("Publishing failed messages on dead letter exchange", function () {
    var message;
    var deadLetterExchangeName = "e1.dead";
    var behaviour = extend(defaultBehaviour, {
      deadLetterExchangeName: deadLetterExchangeName,
      subscribeOptions: {
        ack: true
      }
    });

    after(function (done) {
      killDeadLetter(behaviour.deadLetterExchangeName, function () {
        disconnect();
        done();
      });
    });
    And("We have a connection with a dead letter exchange", function (done) {
      connect(defaultConnOpts, behaviour, ignoreErrors(done));
    });
    And("We are listing to the dead letter exchange", function (done) {
      amqp(defaultConnOpts, {exchange: deadLetterExchangeName}, function (err, conn) {
        if (err) return done(err);
        conn.subscribe("#", "deadQ", function (msg) {
          message = msg;
        }, done);
      });
    });

    And("We reject all messages", function (done) {
      connection.subscribe("testRoutingKey", "testQ", function (msg, headers, deliveryInfo, ack) {
        ack.reject(false);
      }, done);
    });
    When("We publish a message", function (done) {
      connection.publish("testRoutingKey", {testData: "hello"}, done);
    });
    Then("The message should be in the dead letter queue", function (done) {
      setTimeout(function () {
        assert.equal(message.testData, "hello");
        done();
      }, 50);
    });

  });
});

function testConnection(done) {
  var randomRoutingKey = "RK" + crypto.randomBytes(6).toString("hex");
  connection.subscribe(randomRoutingKey, randomRoutingKey, function () {
    done();
  }, function () {
    connection.publish(randomRoutingKey, "someMessage");
  });
}

function ensureErrors(callback) {
  return function (err) {
    if (err) {
      callOnce(callback);
    }
  };
}

function ignoreErrors(callback) {
  return function (err) {
    if (!err) {
      callOnce(callback);
    }
  };
}

function callOnce(callback) {
  if (callback && !callback.alreadyCalled) {
    callback.alreadyCalled = true;
    callback();
  }
}

function connect(opts, behaviour, callback) {
  connection = amqp(opts, behaviour, callback);
}

function disconnect() {
  if (connection) connection.close();
}

function getRabbitConnections(callback) {
  request.get("http://guest:guest@localhost:15672/api/connections",
    function (err, resp, connections) {
      callback(err, JSON.parse(connections));
    });
}

function killRabbitConnections(done) {
  getRabbitConnections(function (err, connections) {
    if (err) return done(err);
    async.each(connections, killRabbitConnection, done);
  });
}

function killDeadLetter(deadLetterExchangeName, done) {
  var queueUrl = util.format("http://guest:guest@localhost:15672/api/queues/%2F/%s.deadLetterQueue", deadLetterExchangeName);
  var exchangeUrl = util.format("http://guest:guest@localhost:15672/api/exchanges/%2F/%s", deadLetterExchangeName);

  request.del(exchangeUrl, function (err) {
    if (err) return done(err);
    request.del(queueUrl, done);
  });
}

function killRabbitConnection(connection, done) {
  request.del("http://guest:guest@localhost:15672/api/connections/" + connection.name, done);
}

function deleteRabbitQueue(queue, done) {
  request.del("http://guest:guest@localhost:15672/api/queues/%2F/" + queue, done);
}
