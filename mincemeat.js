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
var pickle = require("./pickle-js");
var crypto = require("crypto");
var superJson = require("super-json");
var iterator = require("iterator");
var net = require("net");
var logger = require('tracer').colorConsole();

VERSION = "0.1.1";

DEFAULT_PORT = 11235;


//Parent Class define

function Protocol() {
    this.buffer = "";
    this.auth = undefined;
    this.mid_command = false;
    this.terminator = "";
    var self = this;

    this.init = function (conn) {
        if (typeof conn !== "undefined") {
            self.session = net.createServer(function (sock) {
                sock.setEncoding("utf8");
                sock.on('data', function (data) {
                    sock.pause();
                    //Process stream data
                    self.buffer = self.buffer.concat(buffer);
                    self.process_data();
                    sock.resume();
                });
            }).listen(DEFAULT_PORT, "127.0.0.1");
        }
        else {
            self.session = new net.Socket();
        }
        self.terminator = "\n";
    };

    this.send_command = function (command, data) {
        if (command.indexOf(":") == -1) {
            command += ":";
        }
        if (data != undefined) {
            var pdata = pickle.dumps(data);
            command += pdata.length;
            logger.debug(" <- %s", command);
            this.push(command + "\n" + pdata);
        }
        else {
            logger.debug(" <- %s", command);
            this.push(command + "\n");
        }
    };

    this.process_data = function(){
        var data;
        if(typeof self.terminator === "number"){
            if(self.buffer.length >= self.terminator){
                data = self.buffer.slice(0, self.terminator);
                self.found_terminator(data);
                self.buffer = self.buffer.slice(self.terminator);
                self.process_data();
            }
        }
        else{
            if(self.buffer.indexOf(self.terminator) === -1){

            }
            else{
                data = self.buffer.slice(0, self.buffer.indexOf(self.terminator));
                self.found_terminator(data);
                self.buffer = self.buffer.slice(self.buffer.indexOf(self.terminator) + 1);
                self.process_data();
            }
        }
    };

    this.found_terminator = function (data) {
        var command;
        if (self.auth != "Done") {
            data = (data.split(":", 2));
            command = data[0];
            data = data[1];
            self.process_unauthed_command(command, data);
        }
        else if (this.mid_command === false) {
            logger.error("-> %s", data);
            data = (data.split(":", 2));
            command = data[0];
            var length = data[1];
            if (command === "challenge") {
                self.process_command(command, length);
            }
            else if (length) {
                self.set_terminator(parseInt(length));
                self.mid_command = command;
            }
            else {
                self.process_command(command);
            }
        }
        else {
            if (self.auth != "Done") {
                logger.error("Recieved pickled data from unauthed source");
                process.exit();
            }
            data = pickle.loads(data);
            self.terminator = "\n";
            command = self.mid_command;
            self.mid_command = false;
            self.process_command(command, data);
        }
    };

    this.send_challenge = function () {
        self.auth = crypto.randomBytes(20).toString("hex");
        self.send_command(["challenge", self.auth].join(":"));
    };

    this.respond_to_challenge = function (command, data) {
        var hmac = crypto.createHmac("sha1", self.password, data);
        hmac.update(self.auth);
        self.send_command(["auth", hmac.digest("hex")].join(":"));
        self.post_auth_init();
    };

    this.verify_auth = function (command, data) {
        var hmac = crypto.createHmac("sha1", self.password);
        hmac.update(self.auth);
        if (data == hmac.digest("hex")) {
            self.auth = "Done";
            logging.info("Authenticated other end");
        }
        else {

        }
    };
}

Protocol.prototype.process_command = function (command, data) {
    var commands = {
        'challenge': this.respond_to_challenge,
        'auth': this.verify_auth
    };

    if (command in commands) {
        commands[command](command, data)
    }
    else {
        logger.error("Unknown unauthed command received: %s", command);
    }
};


function Client(){

}

Client.prototype = new Protocol();

Client.prototype.constructor = Client;

Client.prototype.conn = function (server, port) {
    self.client = new net.Socket();
    self.client.connect(port, server);
};

Client.prototype.set_mapfn = function (command, mapfn) {
    self.mapfn = superJson.create().parse(mapfn);
};

Client.prototype.set_collectfn = function (command, collectfn) {
    self.collectfn = superJson.create().parse(collectfn);
};

Client.prototype.set_reducefn = function (command, reducefn) {
    self.reducefn = superJson.create().parse(reducefn);
};

Client.prototype.call_mapfn = function (command, data) {
    logger.info("Mapping %s", data[0].toString());
    var results = {};
    var result = self.mapfn(data[0], data[1]);
    for (var k in result) {
        if (!(result[k][0] in results)) {
            results[result[k][0]] = [];
        }
        results[k].append(result[k][1]);
    }
    if (typeof self.collectfn !== "undefined") {
        for (var k in results) {
            results[k] = [self.collectfn(num, results[k])];
        }
    }
    self.send_command('mapdone', [data[0], results]);
};

Client.prototype.call_reducefn = function (command, data) {
    logger.info("Reducing %s", data[0].toString());
    var results = self.reducefn(data[0], data[1]);
    self.send_command('reducedone', [data[0], results]);
};

Client.prototype.process_command = function (command, data) {
    commands = {
        'mapfn': self.set_mapfn,
        'collectfn': self.set_collectfn,
        'reducefn': self.set_reducefn,
        'map': self.call_mapfn,
        'reduce': self.call_reducefn
    };

    if (command in commands) {
        commands[command](command, data);
    }
    else {
        Protocol.prototype.process_command.call(this, command, data);
    }
};

Client.prototype.post_auth_init = function () {
    if (self.auth === undefined) {
        self.send_challenge();
    }
};

function Server() {
    this.datasource = undefined;
    var self = this;
    this.socket_map = [];
    this.mapfn = undefined;
    this.reducefn = undefined;
    this.collectfn = undefined;
    this.password = undefined;

    this.run_server = function (password, port) {
        net.createServer(function(sock){
            self.socket_map.push(sock);
            sock.on("close", function(){
                logger.warn("CLIENT CLOSED:" + sock.remoteAddress + " " + sock.remotePort);
            })
        }).listen(port);
    };

    this.set_datasource = function (ds) {
        self._datasource = ds;
        self.taskmanager = new TaskManager(self._datasource);
    };

    this.get_datasource = function () {
        return self._datasource;
    };

    this.prototype = {
        get datasource() {
            return self._datasource;
        },
        set datasource(val) {
            self._datasource = val;
        }
    };
}

function ServerChannel(server) {
    var self = this;
    this.init(conn);
    this.server = server;
    this.start_auth();
}

ServerChannel.prototype = new Protocol();

ServerChannel.prototype.constructor = ServerChannel;

ServerChannel.prototype.start_auth = function () {
    self.send_challenge();
};

ServerChannel.prototype.start_new_task = function () {
    var data = self.server.taskmanager.next_task();
    if (data[0] == "undefined") {
        return
    }
    self.send_command(data[0], data[1]);
};

ServerChannel.prototype.mapdone = function (command, data) {
    self.server.taskmanager.map_done(data);
    self.start_new_task();
};

ServerChannel.prototype.reducedone = function (command, data) {
    self.server.taskmanager.reduce_done();
    self.start_new_task();
};

ServerChannel.prototype.process_command = function (command, data) {
    var commands = {
        'mapdone': self.map_done,
        'reducedone': self.reduce_done
    };

    if (command in commands) {
        commands[command](command, data)
    }
    else {
        Protocol.process_command(command, data);
    }
};

ServerChannel.prototype.post_auth_init = function () {
    if (typeof self.server.mapfn != "undefined") {
        self.send_command('mapfn', superJson.create().stringify(self.server.mapfn));
    }
    if (typeof self.server.reduce != "undefined") {
        self.send_command('reducefn', superJson.create().stringify(self.server.reducefn));
    }
    if (typeof self.server.collectfn != "undefined") {
        self.send_command('collectfb', superJson.create().stringify(self.server.collectfn));
    }
    self.start_new_task()
};

function TaskManager(datasource, server) {
    var self = this;
    this.START = 0;
    this.MAPPING = 1;
    this.REDUCING = 2;
    this.FINISHED = 3;

    this.datasource = datasource;
    this.server = server;
    this.state = this.START;

    this.next_task = function (channel) {
        if (self.state == self.START) {
            self.map_iter = iterator(this.datasource);
            self.working_maps = {};
            self.map_results = {};
            self.state = self.MAPPING;
        }
        if (self.state == self.MAPPING) {
            try {
                var map_key = this.map_iter.next();
                var map_item = [map_key, self.datasource[map_key]];
                self.working_maps[map_item[0]] = map_item[1];
                return ['map', map_item];
            }
            catch (err) {
                if (self.working_maps.length > 0) {
                    var keys = [];
                    for (var k in obj) keys.push(k);
                    return ['map', [key, self.working_maps[Math.ceil(Math.random() * (keys.length - 1))]]];
                }
                self.state = self.REDUCING;
                self.reduce_iter = self.map_results.iteritems();
                self.working_reduces = {};
                self.results = {};
            }
        }
        if (self.state == self.REDUCING) {
            try {
                var reduce_item = self.reduce_iter.next();
                self.working_reduces[reduce_item[0]] = reduce_item[1];
                return ('reduce', reduce_item);
            }
            catch (err) {
                if (self.working_reduces.length > 0) {
                    var keys = [];
                    for (var k in obj) keys.push(k);
                    return ['map', [key, self.working_reduces[Math.ceil(Math.random() * (keys.length - 1))]]];
                }
                self.state = self.FINISHED;
            }
        }
        if (self.state == self.FINISHED) {
            self.server.close();
            return ['disconnect', undefined];
        }
    };

    this.map_done = function (data) {
        if (!(data[0] in self.working_maps)) {
            return;
        }

        for (var k in data[1].toArray()) {
            if (!(k in self.map_results)) {
                self.map_results[key] = [];
            }
            self.map_results[key].append(data[1].toArray()[k]);
        }

        delete self.working_maps[data[0]];
    };

    this.reduce_done = function (data) {
        if (!(data[0] in self.working_reduces)) {
            return;
        }
        self.results[data[0]] = data[1];
        delete self.working_reduces[data[0]];
    };
}

function run_client() {
    var client = new Client();
    client.password = "123456";
}

