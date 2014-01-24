/**
 * Copyright (c) 2014 Dhc
 *
 # Permission is hereby granted, free of charge, to any person obtaining a copy
 # of this software and associated documentation files (the "Software"), to deal
 # in the Software without restriction, including without limitation the rights
 # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 # copies of the Software, and to permit persons to whom the Software is
 # furnished to do so, subject to the following conditions:
 #
 # The above copyright notice and this permission notice shall be included in
 # all copies or substantial portions of the Software.
 #
 # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 # THE SOFTWARE.
 */

'use strict'

var pickle = require("./pickle-js").pickle;
var crypto = require("crypto");
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
}

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


function Client(){

}

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


var s = new Server();

s.mapfn = function(key, value){
    var i = 0;
    var tmp = [];
    var splitword = value.split(" ");
    while( i < value.split(" ").length){
        tmp.push([splitword[i++], 1]);
    }
    return tmp;
};
s.reducefn = function(key, value){
    var total = 0;
    for(var k in value){
        total += value[k];
    }
    return total;
};
s.datasource = ["Humpty Dumpty sat on a wall",
    "Humpty Dumpty had a great fall",
    "All the King's horses and all the King's men",
    "Couldn't put Humpty together again"
];


s.run_server("123456", 8185);

function run_client() {
    var client = new Client();
    client.password = "123456";
    client.conn("127.0.0.1", 8185);
}

run_client();
