exports.DATABASE = {
    "workers": "./workers.sqlite3",
    "jobs": {
        "host": "127.0.0.1",
        "port": 27017, 
        "db": "jobs"
    },
    "redis": {
        "host": "127.0.0.1",
        "port": 6379
    },
    "redis_local": {
        "host": "127.0.0.1",
        "port": 6379
    }
};
// The port and address on which the controller should listen
exports.PORT = 4000;
exports.ADDR = "127.0.0.1";
exports.WORKERS = [
// Host, port pairs for workers under this controller. e.g., 
// ["127.0.0.1", 8000],
// ["192.168.1.15", 8192],
// ...
["127.0.0.1", 9000],
//["127.0.0.1", 9001],
//["127.0.0.1", 9002]
];

exports.NAME = exports.ADDR + ":" + exports.PORT;
