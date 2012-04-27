var restify = require("restify"),
    mongodb = require("mongodb"),
    redis = require("redis"),
    sqlite3 = require("sqlite3"),
    util = require("util"),
    settings = require("./settings"),
    fs = require("fs"),
    path = require("path"),
    cutils = require("./cloud_utils"),
    async = require("async"),
    events = new (require("events").EventEmitter)();

/*GLOBAL*/
var wdb;
var redisCli;
var localRedisCli
var jdb;
var currentJid = null;
/*
var currentJid = null;
var currentJob = null;
var lastFragment = 0;
var pulling_new = false;
*/

/* _bootstrap
 * Initialize the databases.
 * Uses the async library to start initializing loading all three databases 
 * in parallel.
*/
function _bootstrap(callback){
    console.log("in _bootstrap");
    /* insert_workers()
     * Read the list of workers from the settings module(./settings.js), and 
     * add all the live workers to the main workers database(SQLite3)
     */
    function insert_workers(cb){
        console.log("in insert_workers");
        // Check if each worker is live and add to the workers table
        settings.WORKERS.forEach(function(worker){
            var cli = restify.createStringClient({
                "url": "http://" + worker.join(":")
            });
            // When we send a GET to /ping, we expect a "pong" back.
            cli.get("/ping", function(err, req, res, data){
                if(err || data.trim() != "pong"){
                    console.log("*** Ping to http://%s:%d/ping failed.", 
                                worker[0], 
                                worker[1]);
                    return;
                }
                var insql = 'INSERT INTO WORKERS(ADDR, PORT, STATUS) '
                          + 'VALUES(?, ?, ?)';
                wdb.run(insql, 
                        [worker[0], worker[1], 0],
                        function(err){
                            if(err){
                                console.log("error in SQL run ", err);
                                console.log(util.inspect(err));
                                cb(err);
                                return;
                            }
                            cb(null);
                        }
                );
            });
        });
    }
    /* _setupWorkersDB()
     * If the (stale) workers database exists, purge the entries. If not, create it.
     * Then call insert_workers() to do the actual insertion
     */
    function _setupWorkersDB(cb){
        console.log("in _setupWorkersDB");
        var wdb_path = settings.DATABASE["workers"];
        var exists = path.existsSync(wdb_path);
        
        wdb = new sqlite3.Database(wdb_path);
        if(exists){
            // Purge the old database entries
            wdb.run("DELETE FROM WORKERS", function(err){
                if(err){
                    console.log("Error deleting from table: ", util.inspect(err));
                    cb(err);
                    return;
                }
                insert_workers(cb);
            });
        }else{
            // Or create the workers table.
            var sql = "CREATE TABLE WORKERS(ADDR VARCHAR(20), PORT INT, STATUS INT, "
                    + "PRIMARY KEY(ADDR, PORT))";
            wdb.run(sql, function(err){
                if(err){
                    console.log("error creating wtable", util.inspect(err));
                    cb(err);
                    return;
                }
                insert_workers(cb);
            });
        }   
    }
    /* _setupRedis()
     * Redis is used in two places in cloud.js - One, the job queue, which reside
     * on the Redis server pointed to by the settings.DATABASE.redis.{host, port} 
     * settings and the fragment queue, preferably maintained locally that queues
     * the fragments of a job - one worker can process one fragment at a time.
     * The handles to these two connections are stored in redisCli and 
     * localRedisCli globals respectively.
     */
    function _setupRedis(cb){
        console.log("in _setupRedis");
        // The redis client:
        redisCli = redis.createClient(settings.DATABASE["redis"].port,
                                      settings.DATABASE["redis"].host,
                                      {});
        // The local redis client:
        localRedisCli = redis.createClient(settings.DATABASE["redis_local"].port,
                                      settings.DATABASE["redis_local"].host,
                                      {});
        cb(null);
    }
    /* _setupJobDB()
     * Get a connection handle to the main job server where we eventually will 
     * have to store the results into the global jdb.
     */
    function _setupJobDB(cb){
        console.log("in _setupJobDB");
        // The main jobs database:
        var s = new mongodb.Server(settings.DATABASE.jobs.host, settings.DATABASE.jobs.port, {});
        var db = new mongodb.Db(settings.DATABASE.jobs.db, s, {});
        db.open(function(err, client){
            if(err) throw err;
            jdb = client;
        });
        cb(null);
    }
    /* Now call all the above setup functions in parallel(or at least hope ;))*/
    async.parallel([_setupWorkersDB, _setupRedis, _setupJobDB], function(err, val){
        console.log("in async.parallel's callback in _bootstrap");
        if(err){
            console.log("error passed to _bootstrap parallel() ", err);
            callback(err);
            return;
        }
        callback(null);
    });
}
/* pull_new_job()
 * get a new job by issuing a BRPOP on the Redis queue that has the same name 
 * as this controller (settings.NAME).
 *
 * When available, take this job, and in a local Redis queue with name = the
 * job id, push the argument list for each invocation, the fragment id starting
 * from 0.
 * Also, in a local Redis key <job-id>:func, store the job function for this 
 * job-id and in the key <job-id>:ctx, the context.
 */
function pull_new_job(callback){
    console.log("in pull_new_job");
    var job, job_id;
    /* _getJobIfAvailable()
     * Call the given callback successfully ONLY when there is a job available,
     * while assigning the job and job_id in the enclosing scope to the job 
     * popped.
     *
     * callback(null) indicates success - calling the passed in callback with
     * a non-null value indicates a failure/exception 
     */
    function _getJobIfAvailable(callback){
        console.log("in _getJobIfAvailable");
        redisCli.brpop(settings.NAME, 0, function(err, pop){
            if(err){
                console.log("error in brpop ", err);
                callback(err);
                return;
            }
            var _job = JSON.parse(pop[1]);
            job = _job.job;
            job_id = _job.job_id;
            console.log("got job ", job, "id: ", job_id);
            callback(null);
        });
    }
    /* _splitAndQueue()
     * Once we have a job, we fragment it into parallelizable components and 
     * throw them on the fragment queue
     */
    function _splitAndQueue(callback){
        console.log("in _splitAndQueue");
        var the_args = [];
        for(var i = 0; i < job.args.length; ++i){
            the_args.push(JSON.stringify(
                {
                    "fragment_id": i,
                    "args": job.args[i]
                })
            );
        }

        localRedisCli.lpush(job_id, the_args, function(err){
            if(err){
                console.log("error in LPUSH: ", err);
                callback(err);
            }
            /* once the fragments are pushed, set the <job_id>:func and
             * <job_id>:ctx keys to the string representations of the 
             * job function and the job context respectively.
             */
            localRedisCli.mset(job_id + ":func", job.func,
                               job_id + ":ctx", JSON.stringify(job.ctx),
                               function(err){
                                    if(err){
                                        console.log("error at MSET: ", err);
                                        callback(err);
                                        return;
                                    }
                                    callback(null);
                               }
            );
        });
    }
    /* Call the above two in series - i.e., never call _splitAndQueue() UNTIL 
     * _getJobIfAvailable returns with a job from the queue
     */
    async.series([_getJobIfAvailable, _splitAndQueue],
                 function(err, val){
                    if(err){
                        console.log("Error passed: ", err);
                        callback(err);
                        return;
                    }
                    console.log("done");
                    /* Once all is done, delete the previous job queues and the
                     * old :func and :ctx keys and make the new job the current
                     */
                    localRedisCli.del(currentJid, currentJid + ":func", currentJid + ":ctx",
                            function(e){
                                if(e){
                                    console.log(e);
                                }
                            }
                    );
                    currentJid = job_id;
                    callback(null);
                 }
    );
}

/* assign_next_fragment()
 * ping the worker to see if it is live. If not, return.
 * mark this worker as busy.
 * try to get a fragment from the local fragment queue.
 * If no fragment is there, emit "current_job_done" event and quit.
 * If found, get the func, ctx from the local store, encode and POST 
 * to the worker, calling the callback after this.
 */
function assign_next_fragment(worker, job_id, callback){
    console.log("in assign_next_fragment");
    var worker_url = "http://" + worker.ADDR + ":" + worker.PORT;
    var fragment_id, args, func, ctx;
    var c;
    // Old ping-pong to make sure the worker is live
    function _ensureWorkerIsLive(cb){
        console.log("Ensuring worker is live...");
        c = restify.createStringClient({"url": worker_url});
        c.get("/ping/", function(err, req, res, data){
            if(err || data.trim() !== 'pong'){
                console.log("error pinging %s", worker_url);
                console.log(err);
                cb(err);
                return;
            }
            console.log("done");
            cb(null);
        });
    }
    // Mark the passed in worker as busy in our workers database
    function _markWorkerAsBusy(cb){
        console.log("marking worker %s as busy...",worker_url);
        var sql = "UPDATE WORKERS SET STATUS = 1 WHERE ADDR = ? AND PORT = ?";
        wdb.run(sql, worker.ADDR, worker.PORT, function(err){
            if(err){
                console.log("error marking worker as busy ", err);
                cb(err);
                return;
            }
            console.log("done");
            cb(null);
        });
    }
    /* _tryGetFragment()
    /* Try to get a fragment from the fragments queue. If we do not have one, 
    /* the current job is done - so call the passed callback with 
     * "no_more_fragments" which is technically not an error condition, but 
     * should signal the fetching of a new job.
     *
     * If found, assign the fragment_id and args vars of the enclosing scope
     * appropriately.
     */
    function _tryGetFragment(cb){
        console.log("trying to get a fragment...");
        localRedisCli.rpop(job_id, function(err, popped){
            if(err){
                console.log("error in RPOP", err);
                cb(err);
                return;
            }
            if(popped === null){
                console.log("no more :(")
                cb("no_more_fragments");
                return;
            }
            console.log("got %s", popped);
            var o = JSON.parse(popped);
            fragment_id = o.fragment_id;
            args = o.args;

            cb(null);
        });
    }
    /* Get the function and the context for this job */ 
    function _tryGetFuncAndCtx(cb){
        console.log("trying to get func and ctx...");
        localRedisCli.mget(job_id + ":func", job_id + ":ctx", function(e, v){
            if(e){
                console.log("error at MGET ", e);
                cb(e);
                return;
            }
            func = v[0];
            ctx = JSON.parse(v[1]);
            console.log("done ", func, ctx);
            cb(null);
        });
    }
    /* send the job over to the worker using POST*/
    function _postJob(cb){
        console.log("posting job");
        console.log("TYPE is ", typeof(ctx));
        var _job = {
            "func": func,
            "args": args,
            "ctx": ctx
        };
        /* base64 encode and then URLEncode the job object's string 
         * representation.
         * see cutils.buEncode()
         * The worker's job submission endpoint is /submit/
         */
        var _e_job = cutils.buEncode(_job);
        c.post("/submit/",
                {
                    "job_id": job_id,
                    "fragment_id": fragment_id,
                    "job": _e_job,
                    "response_port": settings.PORT
                },
                function(err, req, res, data){
                    if(err){
                        console.log("error while posting ", err);
                        cb(err);
                        return;
                    }
                    console.log("post done");
                    cb(null);
                }
        );
    }
    /* check if worker is live -> mark it busy -> get a fragment -> get function
     * and ctx -> post the job
     * The above chain should be executed in strict order, falling out if any 
     * call indicates failure. 
     */
    async.series([_ensureWorkerIsLive,
                  _markWorkerAsBusy,
                  _tryGetFragment,
                  _tryGetFuncAndCtx,
                  _postJob], function(err, val){
        if(err){
            // No more fragments means the current job is done
            if(err === "no_more_fragments"){
                // So we free the engaged worker, 
                mark_as_free(worker, function(e){
                    if(e){
                        console.log("error while freeing worker: ", e);
                    }
                    // And then emit the current_job_done event, which should
                    // trigger pull_new_job()
                    events.emit("current_job_done");
                });
                return;
            }
            console.log("Error in assign_next_fragment callback: ", err);
            callback(err);
            return;
        }
        // success
        callback(null);
    });

}

/* with_free_workers_do()
 * Wrapper that calls callback() with an error object as the first arg
 * and the list of free workers as per the database as the second.
 */
function with_free_workers_do(callback){
    var sql = "SELECT * FROM WORKERS WHERE STATUS = 0";
    wdb.all(sql, callback);
}

function mark_as_free(worker, callback){
    var sql = "UPDATE WORKERS SET STATUS = 0 WHERE ADDR = ? AND PORT = ?";
    wdb.run(sql, worker.ADDR, worker.PORT, callback);
}

/* schedule()
 * Get a list of free workers
 * Assign next fragment. If no_more_fragments is emitted, 
 * pull a new job*/
function schedule(callback){
    console.log("in schedule");
    var workers;
    /* _get_free_workers()
     * push all the free workers onto the workers list in the enclosing scope.
     */
    function _get_free_workers(cb){
        with_free_workers_do(function(e, w){
            if(e){
                console.log("Error getting free workers ", e);
                cb(e);
                return;
            }
            console.log(w);
            workers = w;
            cb(null);
        });
    }
    /* _assign_as_many()
     * For each free worker in the workers list of the enclosing scope, try to
     * assign a fragment to it.
     * If the fragments queue is empty ("no_more_fragments"), mark the engaged 
     * worker free, and callback with this error.
     */
    function _assign_as_many(cb){
        var go_next = true;
        var count = workers.length;
        for(var i = 0; go_next && (i < workers.length); i++){
            assign_next_fragment(workers[i], currentJid, function(err){
                if(err){
                    go_next = false;
                    if(err === "no_more_fragments"){
                        console.log("No more fragments");
                        mark_as_free(workers[i], function(e){
                            if(e){
                                console.log("error updating to free ", e);
                            }
                            cb(err);
                        });
                        return;
                    }
                    console.log("Error ", err);
                    cb(err);
                    return;
                }
                if(--count == 0)
                    cb(null);
            });
        }
    }
    // get a list of free workers -> assign fragments
    async.series([_get_free_workers, _assign_as_many], function(e,v){
        if(e){
            // If we receive "no_more_fragments" at anytime, we emit "current_job_done"
            if(e === "no_more_fragments"){
                events.emit("current_job_done");
                return;
            }
            console.log("Error at schedule ", e);
            callback(e);
            return;
        }
        // success
        callback(null);
    });
}
/* start(port:int)
 * start the controller at the specified port.
 */
function start(port){

    // Setup event handlers:
    events.on("worker_freed", function(){ 
        schedule(function(e){});
    });
    /* current_job_done event should trigger the chain pull_new_job() ->
     * schedule()
     */
    events.on("current_job_done", function(){
        async.series([pull_new_job, schedule], function(e, v){
            if(e){
                console.log("in event handler: ", e);
            }
        });
    });
    // Our server
    var server = restify.createServer();
    // Needed to let Restify handle POST parameters transparently
    // and make them available in request.body
    server.use(restify.bodyParser({"mapParams": false}));
    // PING endpoint
    server.get("/ping", function(req, res, next){
        res.send("pong");
        next();
    });
    // Results endpoint
    // Workers post the results here.
    server.post("/submit_result/", function(request, response, next){
        console.log("result ");
        var  __r = {
            "result": cutils.buDecode(request.body.result),
            "job_id": request.body.job_id, 
            "fragment_id": request.body.fragment_id,
            "worker_port": request.body.worker_port
        };
        console.log(__r);
        /* Free the worker associated with this job fragment */
        function _free_worker(cb){
            var sql = "UPDATE WORKERS SET STATUS = 0 WHERE ADDR = ? AND PORT = ?";
            wdb.run(sql, request.socket.remoteAddress, request.body.worker_port, 
                function(err){
                    if(err){
                        console.log("error updating WORKERS ", err);
                        cb(err);
                        return;
                    }
                    events.emit("worker_freed");
                    cb(null);
            });
        }
        /* Write into the jobs database (global var jdb) the result of this fragment.
         * First, we select the document that contains the job details from the job id, 
         * then in the subdocument called `result' the key, value pair fragment_id, 
         * fragment_result. We also decrement the `remaining' field of the job 
         * to signify that a fragment is complete. Note that the `remaining' field
         * is set originally to the number of fragments.
         */
        function _update_result(cb){
            function insert_result(e, collection){
                console.log("in insert_result");
                if(e){
                    console.log("error getting collection ", e);
                    cb(e);
                    return;
                }
                var r_name = "result." + __r.fragment_id;
                var up_obj = {"$set": {}, "$inc": {"remaining": -1}};
                up_obj["$set"][r_name] = __r.result;
                console.log("updating with ", up_obj);

                collection.update({"job_id": __r.job_id},
                                  up_obj, {"safe": true}, 
                                  function(err){
                                    console.log("in the update callback");
                                    if(err){
                                        console.warn(err.message);
                                        cb(err);
                                        return;
                                    }
                                    cb(null);
                                  }
                );
            }
            jdb.collection("job", insert_result);
        }
        // We can free the worker and update the result in parallel
        async.parallel([_free_worker, _update_result], function(e, v){
            if(e){
                console.log("Error in parallel callback ", e);
            }
             next();
        });
       
    });
    /**************************************************************************/
    server.listen(port);
    /**************************************************************************/
    /* At start-up, we first set up the DBs and then pull our first job :)    */
    async.series([_bootstrap, pull_new_job], function(err, val){
        if(err){
            console.log("error passed to main ", err);
            return;
        }
        console.log("all okay");
        events.emit("worker_freed");
    });
}
start(4000);
