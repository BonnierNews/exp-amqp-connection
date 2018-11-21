"use strict";

const utils = require("../testUtils");
const assert = require("assert");

Feature("Exchange type and options", () => {
  Scenario("Alternate exchange", () => {
    let broker;
    let alternateBroker;
    let alternateReceived;
    const msgContent = {
      msgId: 1
    };

    after((done) => utils.shutdown(broker, () => utils.shutdown(alternateBroker, done)));

    Given("We have a connection", () => {
      broker = utils.init({
        exchange: "pe",
        exchangeOptions: {
          alternateExchange: "ae"
        },
        configKey: "default"
      });
    });

    And("A connection to an alternate exchange", () => {
      alternateBroker = utils.init({
        reuse: "alternate",
        exchange: "ae",
        configKey: "alternate"
      });
    });

    And("A subscription to alternate exchange", (done) => {
      alternateBroker.subscribeTmp("testAlternateRoutingKey", (msg) => {
        alternateReceived = msg;
      }, done);
    });

    When("We publish a message", (done) => {
      broker.publish("testAlternateRoutingKey", msgContent, done);
    });

    Then("We receive it on the alternate exchange", () => {
      utils.waitForTruthy(() => alternateReceived, () => {
        assert.equal(alternateReceived.msgId, 1);
      });
    });
  });

  Scenario("Exchange type", () => {
    let broker;
    let received1;
    let received2;
    const msgContent = {
      msgId: 1
    };

    after((done) => utils.shutdown(broker, done));

    Given("We have a connection to a fanout exchange", () => {
      broker = utils.init({
        exchange: "fanout",
        exchangeType: "fanout",
        configKey: "fanout"
      });
    });

    And("A subscription", (done) => {
      broker.subscribeTmp("foo", (msg) => {
 received1 = msg;
}, done);
    });

    And("Another subscription", (done) => {
      broker.subscribeTmp("bar", (msg) => {
 received2 = msg;
}, done);
    });

    When("We publish a message", (done) => {
      broker.publish("baz", msgContent, done);
    });

    Then("We receive it on both queues, disregarding routing key", () => {
      utils.waitForTruthy(() => received1 && received2, () => {
        assert.equal(received1.msgId, 1);
        assert.equal(received2.msgId, 1);
      });
    });
  });
});
