/**
 * Author: dhc
 */

'use strict'

var Server = require("./Server");
var Client = require("./Client");
var cluster = require("cluster");

var numCPUs = require("os").cpus().length;

function run_client() {
    var client = new Client();
    client.password = "123456";
    client.conn("127.0.0.1", 8185);
}

if(cluster.isMaster){
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
    for(var i = 0; i < numCPUs; i++ ){
        cluster.fork();
    }
}
else{
    run_client();
}
