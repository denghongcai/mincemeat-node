/**
 * Author: dhc
 */

'use strict'

var crypto = require("crypto");
var superJson = require("super-json");
var iterator = require("./iterator").Iterator;
var oiterator = require("object-iterator");
var net = require("net");
var logger = require('tracer').colorConsole({
    dateformat: "MM:ss.L"
});

var EventEmitter = require("events").EventEmitter;
var event = new EventEmitter();

var VERSION = "0.1.1";

var DEFAULT_PORT = 11235;


//Parent Class define

function Protocol() {
    this.buffer = "";
    this.auth = undefined;
    this.mid_command = false;
    this.terminator = "";
    this.password = "";


    this.init = function (conn) {
        var self = this;
        if (typeof conn !== "undefined") {
            this.session = conn;
        }
        else {

        }
        this.terminator = "\n";
    };

    this.send_command = function (command, data) {
        if (command.indexOf(":") == -1) {
            command += ":";
        }
        if (data != undefined) {
            var myJson = superJson.create();
            var pdata = myJson.stringify(data);
            logger.debug(" <- %s", pdata);
            command += pdata.length;
            logger.debug(" <- %s", command);
            this.session.write(command + "\n" + pdata);
        }
        else {
            logger.debug(" <- %s", command);
            this.session.write(command + "\n");
        }
    };

    this.process_data = function(){
        var data;
        if(typeof this.terminator === "number"){
            if(this.buffer.length >= this.terminator){
                data = this.buffer.slice(0, this.terminator);
                this.buffer = this.buffer.slice(this.terminator);
                this.found_terminator(data);
                this.process_data();
            }
        }
        else{
            if(this.buffer.indexOf(this.terminator) === -1){

            }
            else{
                data = this.buffer.slice(0, this.buffer.indexOf(this.terminator));
                this.buffer = this.buffer.slice(this.buffer.indexOf(this.terminator) + 1);
                this.found_terminator(data);
                this.process_data();
            }
        }
    };

    this.found_terminator = function (data) {
        var command;
        if (this.auth != "Done") {
            //logger.debug(" <- %s", data);
            data = (data.split(":", 2));
            command = data[0];
            data = data[1];
            this.process_unauthed_command(command, data);
        }
        else if (this.mid_command === false) {
            //logger.debug("-> %s", data);
            data = (data.split(":", 2));
            command = data[0];
            var length = data[1];
            if (command === "challenge") {
                this.process_command(command, length);
            }
            else if (length) {
                this.terminator = parseInt(length);
                this.mid_command = command;
            }
            else {
                this.process_command(command);
            }
        }
        else {
            if (this.auth != "Done") {
                logger.error("Recieved pickled data from unauthed source");
                process.exit();
            }
            var myJson = superJson.create();
            logger.debug(" <- %s", data);
            data = myJson.parse(data);
            this.terminator = "\n";
            command = this.mid_command;
            this.mid_command = false;
            this.process_command(command, data);
        }
    };

    this.send_challenge = function () {
        this.auth = crypto.randomBytes(20).toString("hex");
        this.send_command(["challenge", this.auth].join(":"));
    };

    this.respond_to_challenge = function (command, data) {
        logger.debug(" <- %s", data);
        var hmac = crypto.createHmac("sha1", this.password);
        hmac.update(data);
        this.send_command(["auth", hmac.digest("hex")].join(":"));
        this.post_auth_init();
    };

    this.verify_auth = function (command, data) {
        logger.debug(" <- %s", data);
        var hmac = crypto.createHmac("sha1", this.password);
        hmac.update(this.auth);
        if (data == hmac.digest("hex")) {
            this.auth = "Done";
            logger.info("Authenticated other end");
        }
        else {
            this.session.destroy();
        }
    };

    this.handle_close = function(){
        this.session.destroy();
    };

    this.process_unauthed_command = function(command, data){
        var commands = {
            'challenge': this.respond_to_challenge,
            'auth': this.verify_auth
        };

        if(command in commands){
            commands[command].call(this, command, data);
        }
        else{
            logger.error("Unknown unauthed command received: %s", command);
        }
    }
};

Protocol.prototype.process_command = function (command, data) {
    var commands = {
        'challenge': this.respond_to_challenge,
        'disconnect': this.handle_close
    };

    if (command in commands) {
        commands[command].call(this, command, data)
    }
    else {
        logger.error("Unknown command received: %s", command);
    }
};

module.exports = Protocol;
