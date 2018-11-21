"use strict";

const utils = require("../testUtils");
const assert = require("assert");

Feature("Callback throws exception shouldn't crash everything", () => {
  Scenario("Cb throws exception", () => {
    const messages = [];
    let broker;
    const handler = (message) => {
      messages.push(message.testData);
    };
    let maybeFoo;

    function fooThrower() {
      throw new Error("foo");
    }

    Given("We have a connection", () => {
      broker = utils.init();
      broker.on("callback_error", (e) => {
        maybeFoo = e;
      });
    });

    And("We create a subscription for routing key 1", (done) => {
      broker.subscribe("cbthrowsbug", "cbthrowsbugq", handler, done);
    });

    When("We publish a message with routing key 1, passing a throwing callback", (done) => {
      broker.publish("cbthrowsbug", {testData: "m1"}, fooThrower);
      utils.waitForTruthy(() => maybeFoo && messages.length > 0, done);
    });

    Then("a foo should have been thrown", () => {
      assert("foo", maybeFoo.message);
    });

    And("the message should have been delivered", () => {
      assert.deepEqual(["m1"], messages);
    });

    after((done) => utils.shutdown(broker, done));
  });

});
