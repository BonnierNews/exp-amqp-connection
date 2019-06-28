"use strict";

var JSON_TYPE = "application/json";

function encode(body, meta, persistent) {
  var message = {
    props: meta || {},
    buffer: {}
  };

  if (typeof body === "string") {
    message.buffer = new Buffer(body, "utf8");
  } else if (body instanceof Buffer) {
    message.buffer = body;
  } else {
    message.props.contentType = "application/json";
    message.buffer = new Buffer(JSON.stringify(body), "utf8");
  }

  if (message.props.persistent === undefined && persistent !== undefined) message.props.persistent = persistent;
  return message;
}

function decode(message) {
  var props = message.properties || {};
  var messageStr = message.content.toString("utf8");

  return props.contentType === JSON_TYPE ? JSON.parse(messageStr) : messageStr;
}

module.exports = {
  encode: encode,
  decode: decode
};
