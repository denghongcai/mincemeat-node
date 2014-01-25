/**
 * Author: dhc
 */

"use strict"


var Protocol = require("./Protocol");

var superJson = require("super-json");
var iterator = require("./iterator").Iterator;
var generator = require("./iterator").Generator;
var oiterator = require("object-iterator");
var net = require("net");
var logger = require('tracer').colorConsole({
    dateformat: "MM:ss.L"
});

var EventEmitter = require("events").EventEmitter;
var event = new EventEmitter();

function Client(){

};

Client.prototype = new Protocol();

Client.prototype.constructor = Client;

Client.prototype.conn = function (server, port) {
    var self = this;
    this.terminator = "\n";
    this.client = new net.Socket();
    this.client.connect(port, server, function(){
        logger.info("Connect to server");
        self.session = self.client;
    });
    this.client.on("data", function(data){
        self.session.pause();
        //Process stream data
        self.buffer = self.buffer.concat(data);
        self.process_data();
        self.session.resume();
    })
};

Client.prototype.set_mapfn = function (command, mapfn) {
    this.mapfn = superJson.create().parse(mapfn);
};

Client.prototype.set_collectfn = function (command, collectfn) {
    this.collectfn = superJson.create().parse(collectfn);
};

Client.prototype.set_reducefn = function (command, reducefn) {
    this.reducefn = superJson.create().parse(reducefn);
};

Client.prototype.call_mapfn = function (command, data) {
    logger.info("Mapping %s", data[0].toString());
    var results = {};
    var result = generator(this.mapfn, data[0], data[1]);
    result = result();
    try {
        var result_item;
        while(result_item = result.next()){
            for (var k in result_item) {
                if (!(k in results)) {
                    results[k] = [];
                }
                results[k].push(result_item[k]);
            }
        }
    }
    catch (err) {

    }
    if (typeof this.collectfn !== "undefined") {
        for (var k in results) {
            results[k] = [this.collectfn(num, results[k])];
        }
    }
    this.send_command('mapdone', [data[0], results]);
};

Client.prototype.call_reducefn = function (command, data) {
    logger.info("Reducing %s", data[0]);
    var results = this.reducefn(data[0], data[1]);
    this.send_command('reducedone', [data[0], results]);
};

Client.prototype.process_command = function (command, data) {
    var commands = {
        'mapfn': this.set_mapfn,
        'collectfn': this.set_collectfn,
        'reducefn': this.set_reducefn,
        'map': this.call_mapfn,
        'reduce': this.call_reducefn
    };

    if (command in commands) {
        commands[command].call(this, command, data);
    }
    else {
        Protocol.prototype.process_command.call(this, command, data);
    }
};

Client.prototype.post_auth_init = function () {
    if (this.auth === undefined) {
        this.send_challenge();
    }
};

module.exports = Client;