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
    this.buffer = [];
    this.auth = undefined;
    this.mid_command = false;

    this.init = function (conn) {
        if (typeof conn !== "undefined") {
            var self = this;
            this.session = net.createServer(function (sock) {
                sock.on('data', function (data) {
                    self.buffer.append(data);
                });
            }).listen(DEFAULT_PORT, "127.0.0.1");
        }
        else {
            this.session = new net.Socket();
        }
    }

    this.send_command = function (command, data) {
        if (command.indexOf(":") == -1) {
            command += ":";
        }
        if (data != undefined) {
            var pdata = pickle.dumps(data);
            command += pdata.toString();
            logger.debug(" <- %s", command);
            this.push(command + "\n" + pdata);
        }
        else {
            logger.debug(" <- %s", command);
            this.push(command + "\n" + pdata);
        }
    };

    this.found_terminator = function () {
        if (this.auth != "Done") {
            var data = ((''.join(this.buffer)).split(":", 2));
            var command = data[0];
            data = data[1];
            this.process_unauthed_command(command, data);
        }
        else if (this.mid_command === false) {
            logger.error("-> %s", ''.join(self.buffer));
            var data = ((''.join(this.buffer)).split(":", 2));
            var command = data[0];
            var length = data[1];
            if (command === "challenge") {
                this.process_command(command, length);
            }
            else if (length) {
                this.set_terminator(parseInt(length));
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
            var data = pickle.loads("".join(this.buffer));
            this.set_terminator("\n");
            var command = this.mid_command;
            this.mid_command = undefined;
            this.process_command(command, data);
        }
        this.buffer = [];
    };

    this.send_challenge = function () {
        this.auth = crypto.randomBytes(20).toString("hex");
        this.send_command(":".join(["challenge", this.auth]));
    };

    this.respond_to_challenge = function (command, data) {
        var hmac = crypto.createHmac("sha1", this.password);
        hmac.update(this.auth);
        this.send_command(":".join(["auth", hmac.digest("hex")]));
        this.post_auth_init();
    }

    this.verify_auth = function (command, data) {
        var hmac = crypto.createHmac("sha1", this.password);
        hmac.update(this.auth);
        if (data == hmac.digest("hex")) {
            this.auth == "Done";
            logging.info("Authenticated other end");
        }
        else {
            this.handle_close();
        }
    };

    this.process_command = function (command, data) {
        var commands = {
            'challenge': this.respond_to_challenge,
            'auth': this.verify_auth,
            'disconnect': this.handle_close()
        };

        if (command in commands) {
            commands[command](command, data)
        }
        else {
            logger.error("Unknown unauthed command received: %s", command);
            this.handle_close();
        }
    };
};

function Client() {

};

Client.prototype = new Protocol();

Client.prototype.constructor = Client;

Client.prototype.conn = function (server, port) {

};

Client.prototype.handle_connect = function () {
    return;
};

Client.prototype.handle_close = function () {
    this.close();
}

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
    var result = this.mapfn(data[0], data[1]);
    for (var k in result) {
        if (!(result[k][0] in results)) {
            results[result[k][0]] = [];
        }
        results[k].append(result[k][1]);
    }
    if (typeof this.collectfn !== "undefined") {
        for (var k in results) {
            results[k] = [this.collectfn(num, results[k])];
        }
    }
    this.send_command('mapdone', (data[0], results));
};

Client.prototype.call_reducefn = function (command, data) {
    logger.info("Reducing %s", data[0].toString());
    var results = this.reducefn(data[0], data[1]);
    this.send_command('reducedone', (data[0], results));
};

Client.prototype.process_command = function (command, data) {
    commands = {
        'mapfn': this.set_mapfn,
        'collectfn': this.set_collectfn,
        'reducefn': this.set_reducefn,
        'map': this.call_mapfn,
        'reduce': this.call_reducefn
    };

    if (command in commands) {
        commands[command](command, data);
    }
    else {
        Protocol.process_command(command, data);
    }
};

Client.prototype.post_auth_init = function () {
    if (this.auth === undefined) {
        this.send_challenge();
    }
};

function Server() {
    this.datasource = undefined;

    this.constructor = function () {
        this.socket_map = {};
        this.mapfn = undefined;
        this.reducefn = undefined;
        this.collectfn = undefined;
        this.password = undefined;
    };

    this.run_server = function (password, port) {
        this.password = password;
    };

    this.handle_accept = function () {

    };

    this.handle_close = function () {

    };

    this.set_datasource = function (ds) {
        this._datasource = ds;
        this.taskmanager = new TaskManager(this._datasource);
    };

    this.get_datasource = function () {
        return this._datasource;
    }

    this.prototype = {
        get datasource() {
            return this._datasource;
        },
        set datasource(val) {
            this._datasource = val;
        }
    }
};

function ServerChannel() {

};
d
ServerChannel.prototype = new Protocol();

ServerChannel.prototype.constructor = ServerChannel;

ServerChannel.prototype.handle_close = function () {
    logger.info("Client disconnected");
};

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

ServerChannel.prototype.mapdone = function (command, data) {
    this.server.taskmanager.map_done(data);
    this.start_new_task();
};

ServerChannel.prototype.reducedone = function (command, data) {
    this.server.taskmanager.reduce_done();
    this.start_new_task();
};

ServerChannel.prototype.process_command = function (command, data) {
    var commands = {
        'mapdone': this.map_done,
        'reducedone': this.reduce_done
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
        this.send_command('mapfn', superJson.create().stringify(this.server.mapfn));
    }
    if (typeof self.server.reduce != "undefined") {
        this.send_command('reducefn', superJson.create().stringify(this.server.reducefn));
    }
    if (typeof self.server.collectfn != "undefined") {
        this.send_command('collectfb', superJson.create().stringify(this.server.collectfn));
    }
    this.start_new_task()
};

function TaskManager() {
    this.START = 0;
    this.MAPPING = 1;
    this.REDUCING = 2;
    this.FINISHED = 3;

    this.constructor = function (datasource, server) {
        this.datasource = datasource;
        this.server = server;
        this.state = TaskManager.START;
    };

    this.next_task = function (channel) {
        if (this.state == TaskManager.START) {
            this.map_iter = iterator(this.datasource);
            this.working_maps = {};
            this.map_results = {};
            this.state = TaskManager.MAPPING;
        }
        if (this.state == TaskManager.MAPPING) {
            try {
                var map_key = this.map_iter.next();
                var map_item = [map_key, this.datasource[map_key]];
                this.working_maps[map_item[0]] = map_item[1];
                return ('map', map_item);
            }
            catch (err) {
                if (this.working_maps.length > 0) {
                    var keys = [];
                    for (var k in obj) keys.push(k);
                    return ('map', (key, this.working_maps[Math.ceil(Math.random() * (keys.length - 1))]));
                }
                this.state = TaskManager.REDUCING;
                this.reduce_iter = this.map_results.iteritems();
                this.working_reduces = {};
                this.results = {};
            }
        }
        if (this.state == TaskManager.REDUCING) {
            try {
                var reduce_item = this.reduce_iter.next();
                this.working_reduces[reduce_item[0]] = reduce_item[1];
                return ('reduce', reduce_item);
            }
            catch (err) {
                if (this.working_reduces.length > 0) {
                    var keys = [];
                    for (var k in obj) keys.push(k);
                    return ('map', (key, this.working_reduces[Math.ceil(Math.random() * (keys.length - 1))]));
                }
                this.state = TaskManager.FINISHED;
            }
        }
        if (this.state == TaskManager.FINISHED) {
            this.server.handle_close();
            return ('disconnect', undefined);
        }
    };

    this.map_done = function (data) {
        if (!(data[0] in this.working_maps)) {
            return;
        }

        for (var k in data[1].toArray()) {
            if (!(k in this.map_results)) {
                this.map_results[key] = [];
            }
            this.map_results[key].append(data[1].toArray()[k]);
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

function run_client() {
    var client = new Client();
    client.password = "123456";
}

run_client();
