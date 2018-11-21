"use strict";

const utils = require("../testUtils");
const assert = require("assert");

Feature("Unsubscribe", () => {

  Scenario("Unsubscribe all handlers", () => {
    let broker;
    let receivedOnUnsubscribed;
    let receivedOnResubscribed;

    after((done) => utils.shutdown(broker, done));
    Given("We have a connection", () => {
      broker = utils.init();
    });
    And("We create a subscription", (done) => {
      broker.subscribe("testUnsubscribeRoutingKey", "persistent-queue", (msg) => {
        receivedOnUnsubscribed = msg;
      }, done);
    });
    And("We unsubscribe all subscriptions", (done) => {
      broker.unsubscribeAll(done);
    });
    When("We publish a message and wait 500 ms", (done) => {
      broker.publish("testUnsubscribeRoutingKey", "message");
      setTimeout(done, 500);
    });
    And("We resubscribe", (done) => {
      broker.subscribe("testUnsubscribeRoutingKey", "persistent-queue", (msg) => {
        receivedOnResubscribed = msg;
      }, done);
    });
    Then("It should not have arrived on the unsubscribed handler", () => {
      assert.deepEqual(receivedOnUnsubscribed, undefined);
    });
    And("We wait for the message to arrive", (done) => {
      utils.waitForTruthy(() => receivedOnResubscribed, done);
    });
    And("It should have arrived on the resubscribed handler", () => {
      assert.deepEqual(receivedOnResubscribed, "message");
    });
  });
});
