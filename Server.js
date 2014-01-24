/**
 * Author: dhc
 */

"use strict"

var Protocol = require("./Protocol");

var superJson = require("super-json");
var iterator = require("./iterator").Iterator;
var oiterator = require("object-iterator");
var net = require("net");
var logger = require('tracer').colorConsole({
    dateformat: "MM:ss.L"
});

var EventEmitter = require("events").EventEmitter;
var event = new EventEmitter();

function Server() {
    this._datasource = undefined;

    this.socket_map = [];
    this.mapfn = undefined;
    this.reducefn = undefined;
    this.collectfn = undefined;
    this.password = undefined;
    this.taskmanager = undefined;

    this.run_server = function (password, port) {
        var self = this;
        this.password = password;
        net.createServer(function(sock){
            self.socket_map.push(sock);
            var sc = new ServerChannel(sock, self, self.password);
        }).listen(port);
        logger.info("Server Start!");
        event.on("task done", function(){
            logger.info("Done");
            console.log(self.taskmanager.results);
            process.exit(0);
        })
    };

    this.handle_close = function(){
        event.emit("task done");
    }
}

Server.prototype.__defineSetter__("datasource", function(val){
    this._datasource = val;
    this.taskmanager = new TaskManager(this._datasource, this);
});

Server.prototype.__defineGetter__("datasource", function(){
    return this._datasource;
});

function ServerChannel(conn, server, password) {

    this.init(conn);
    this.server = server;
    this.password = password;
    this.start_auth();

    var self = this;
    this.session.on("data", function(data){
        self.session.pause();
        //Process stream data
        self.buffer = self.buffer.concat(data);
        self.process_data();
        self.session.resume();
    });
}

ServerChannel.prototype = new Protocol();

ServerChannel.prototype.constructor = ServerChannel;

ServerChannel.prototype.start_auth = function () {
    this.send_challenge();
};

ServerChannel.prototype.start_new_task = function () {
    var data = this.server.taskmanager.next_task();
    if (data[0] == "undefined") {
        return
    }
    this.send_command(data[0], data[1]);
};

ServerChannel.prototype.map_done = function (command, data) {
    this.server.taskmanager.map_done(data);
    this.start_new_task();
};

ServerChannel.prototype.reduce_done = function (command, data) {
    this.server.taskmanager.reduce_done(data);
    this.start_new_task();
};

ServerChannel.prototype.process_command = function (command, data) {
    var self = this;
    var commands = {
        'mapdone': self.map_done,
        'reducedone': self.reduce_done,
        'disconnect': self.handle_close
    };

    if (command in commands) {
        commands[command].call(self,command, data);
    }
    else {
        Protocol.prototype.process_command.call(this, command, data);
    }
};

ServerChannel.prototype.post_auth_init = function () {
    var myJson = superJson.create();
    if (typeof this.server.mapfn != "undefined") {
        this.send_command('mapfn', myJson.stringify(this.server.mapfn));
    }
    if (typeof this.server.reducefn != "undefined") {
        this.send_command('reducefn', myJson.stringify(this.server.reducefn));
    }
    if (typeof this.server.collectfn != "undefined") {
        this.send_command('collectfb', myJson.stringify(this.server.collectfn));
    }
    this.start_new_task()
};

function TaskManager(datasource, server) {

    this.START = 0;
    this.MAPPING = 1;
    this.REDUCING = 2;
    this.FINISHED = 3;

    this.datasource = datasource;
    this.server = server;
    this.state = this.START;

    this.working_maps = {};
    this.map_results = {};
    this.map_iter = undefined;
    this.reduce_iter = undefined;
    this.working_reduces = {};
    this.results = {};

    this.map_key = 0;

    this.next_task = function (channel) {
        if (this.state == this.START) {
            this.map_iter = iterator(this.datasource);
            this.working_maps = {};
            this.map_results = {};
            this.state = this.MAPPING;
        }
        if (this.state == this.MAPPING) {
            try {
                var map_item = [this.map_key, this.map_iter.next()];
                logger.debug(" <- %s", this.map_key);
                this.working_maps[map_item[0]] = map_item[1];
                logger.debug(" <- %s", this.working_maps);
                this.map_key++;
                return ['map', map_item];
            }
            catch (err) {
                logger.debug(" <- %s", err);
                if (this.working_maps.length > 0) {
                    var key = Math.ceil(Math.random() * (this.working_maps.length - 1));
                    return ['map', [key, this.working_maps[key]]];
                }
                this.state = this.REDUCING;
                this.reduce_iter = oiterator(this.map_results);
                this.working_reduces = {};
                this.results = {};
            }
        }
        if (this.state == this.REDUCING) {
            try {
                var reduce_item_tmp = this.reduce_iter();
                var reduce_item_array = [];
                if(reduce_item_tmp.type === "array"){
                    reduce_item_tmp = this.reduce_iter();
                    while(reduce_item_tmp.type !== "end-array"){
                        reduce_item_array.push(reduce_item_tmp.value);
                        reduce_item_tmp = this.reduce_iter();
                    }
                }
                else if(reduce_item_tmp.type === "end-object"){
                    throw new Error("StopIteration");
                }
                else if(reduce_item_tmp.type === "object"){
                    this.reduce_iter();
                    reduce_item_tmp = this.reduce_iter();
                    while(reduce_item_tmp.type !== "end-array"){
                        reduce_item_array.push(reduce_item_tmp.value);
                        reduce_item_tmp = this.reduce_iter();
                    }
                }
                var reduce_item = [];
                reduce_item[0] = reduce_item_tmp.key;
                reduce_item[1] = reduce_item_array;
                this.working_reduces[reduce_item[0]] = reduce_item[1];
                return ['reduce', reduce_item];
            }
            catch (err) {
                if (this.working_reduces.length > 0) {
                    var keys = [];
                    for (var k in obj) keys.push(k);
                    return ['reduce', [key, this.working_reduces[Math.ceil(Math.random() * (keys.length - 1))]]];
                }
                this.state = this.FINISHED;
            }
        }
        if (this.state == this.FINISHED) {
            this.server.handle_close();
            return ['disconnect', undefined];
        }
    };

    this.map_done = function (data) {
        if (!(data[0] in this.working_maps)) {
            return;
        }
        for (var k in data[1][0]) {
            if (!(data[1][0][k] in this.map_results)) {
                this.map_results[data[1][0][k]] = [];
            }
            this.map_results[data[1][0][k]].push(data[1][1][k]);
        }
        delete this.working_maps[data[0]];
    };

    this.reduce_done = function (data) {
        if (!(data[0] in this.working_reduces)) {
            return;
        }
        this.results[data[0]] = data[1];
        delete this.working_reduces[data[0]];
    };
}

module.exports = Server;