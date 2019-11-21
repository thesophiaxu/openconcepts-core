import EventSource from 'eventsource';
import request from 'request';
import MongoClient, { Timestamp, ObjectID } from 'mongodb';

var url = 'https://stream.wikimedia.org/v2/stream/recentchange';
var wiki = "wikidatawiki"

var dbUrl = 'mongodb://localhost:27017';
var dbName = 'openConceptsInfra';
var collectionName = "wikidataRaw";

var collectionNameJobs: string = "wikidataRawJobs";

/* Controller-managed Global Variables */

var timestamp: number = 0; // KAFKA's event pointer.

var accepted_requests = 0;
var all_requests = 0;
var inserted_jobs = 0;
var success_rate: number = 0.0;
var queue_length: number = 0;

/* ----------------------------------- */

class MetaController {
    dbInterface: MongoClient.Collection;

    constructor (args: {_dbn: string, _colln: string, _dbc: MongoClient.MongoClient}) {
        this.dbInterface = args._dbc.db(args._dbn).collection(args._colln)

        this.fillTimestamp();
        
        setInterval(() => {
            this.setTimestamp(timestamp);
            console.log("================ Last 30 seconds ================")
            console.log("Req+ " + accepted_requests + "                          Req- " + (all_requests-accepted_requests))
            console.log("Insertions per minute: " + (2 * accepted_requests))
            console.log("")
            console.log("Inserted jobs: " + inserted_jobs)
            console.log("Current Queue Length: " + queue_length);
            console.log("Job queued until date: " + new Date(timestamp).toISOString())
            console.log("Estimated remaininig time in days: " + (queue_length / (120*24 * accepted_requests)))
            console.log("=================================================")
            success_rate = accepted_requests / all_requests;
            accepted_requests = 0;
            all_requests = 0;
            inserted_jobs = 0;

        }, 30000) // every half a minute
    }

    fillTimestamp(): void {
        this.dbInterface.find().limit(1).toArray((err: any, doc: any) => {
            if (err) {console.log("This should NOT happen - failed to get timestamp from configuration database"+ err.message); return(undefined)};
            if (doc[0]) {
                timestamp = doc[0].timestamp;
            }
        })
    }

    setTimestamp(ts: number): void {
        this.dbInterface.updateOne({config: true}, {$set: {timestamp: ts}})
    }
}

interface JobSchema {
    _id: ObjectID
    pageTitle: string,
    pageURL: string,
    createdAt: Timestamp,
    claimed: boolean,
    tries: 0
}

/**
 * StreamToqrikoq: Listens a Apache KAFKA event stream and distributes site updates to a job queue defined by a database location.
 * @etymology toqrikoq (t'oqrikoq): local leaders of the Mita system, typically manage a city and its hinterland.
 */
class StreamToqrikoq {
    url: string = "";
    OffsetStr: string = "";

    eventSource: EventSource;
    jobQueueInterface: MongoClient.Collection;

    constructor (args: {listeningURL: string, OffsetStr: string, _dbn: string, _colln: string, dbConnection: MongoClient.MongoClient}) {
        this.url = args.listeningURL;
        this.OffsetStr = args.OffsetStr;

        this.eventSource = new EventSource(this.url + "?since=" + args.OffsetStr);
        console.log(`[DEBUG] Connecting to EventStreams at ${this.eventSource.url}`);

        this.jobQueueInterface = args.dbConnection.db(args._dbn).collection(args._colln);

        this.registerMessageListeners();
    }

    registerMessageListeners () {
        var myOnMessage = this.onMessage;
        var myOnError = this.onError;
        var myOnOpen = this.onOpen;
        var here = this;

        this.eventSource.addEventListener('message', function (e: any) {
            myOnMessage.call(here, e)
        })
        
        this.eventSource.addEventListener('error', function (e: any) {
            myOnError.call(here, e)
        })

        this.eventSource.addEventListener('open', function (e: any) {
            myOnOpen.call(here, e)
        })
    }

    onError(event: any): void {
        //throw new Error("[DEBUG] An error occured.");
        // Try again!
        this.eventSource = new EventSource(this.url + "?since=" + this.OffsetStr);
    }

    onOpen(event: any): void {
        console.log ('[DEBUG] Successfully connected to EventStream.')
    }
    
    /**
     * onMessage: receives a message and pass it to jobs database.
     * @param event The Apache KAFKA event.
     */
    onMessage(event: any): void {
        
        if (!event) return; // Don't waste time here
        // event.data will be a JSON string containing the message event.
        var change = JSON.parse(event.data);
        //if (change.wiki == wiki) console.log("Got update "+change.title);
        if (change.wiki == "wikidatawiki" && (change.title.charAt(0) == 'L' || change.title.charAt(0) == 'Q' || change.title.charAt(0) == 'P')) {
            //console.log(change);
            
            var finalTitle = change.title;
            timestamp = change.timestamp * 1000;
            //console.log("Time: ", timestamp)
            // Pre-process title string
            if (change.title.charAt(0) == 'L') finalTitle = change.title.substring(7);
            if (change.title.charAt(0) == 'P') finalTitle = change.title.substring(9);
            
            this.jobQueueInterface.updateOne({pageTitle: finalTitle}, {$set: {
                pageTitle: finalTitle,
                pageURL: 'https://www.wikidata.org/wiki/Special:EntityData/'+finalTitle+'.json',
                createdAt: Date.now(),
                claimed: false,
                tries: 0
            }}, {upsert: true}, 
            function(err, r) {
                if (err) throw new Error("[ERROR] An error occured when inserting to job queue: " + err.errmsg);
                //if (r) console.log("[LEADER] Job " + finalTitle + " inserted.")
                inserted_jobs += 1;
            });
        }

        this.jobQueueInterface.estimatedDocumentCount((err, count) => {
            queue_length = count;
        });
        //console.log(JSON.parse(event.data));
    }
}

function proxyInterval(oper: any, delay: any) {
    var timeout = delay(oper());
    setTimeout(function() { proxyInterval(oper, delay); }, timeout-0 || 1000);
    /* Note: timeout-0 || 1000 is just in case a non-numeric
     * timeout was returned by the operation. We also
     * could use a "try" statement, but that would make
     * debugging a bad repeated operation more difficult. */
}

class Worker {
    jobQueueInterface: MongoClient.Collection;
    dataInterface: MongoClient.Collection;

    constructor (args: {_dbn: string, _collnQueue: string, _collnData: string, dbConnection: MongoClient.MongoClient}) {
        //console.log(`[DEBUG] Worker started!`);

        this.jobQueueInterface = args.dbConnection.db(args._dbn).collection(args._collnQueue);
        this.dataInterface = args.dbConnection.db(args._dbn).collection(args._collnData);

        this.ScheduleWork();
    }

    getJob() : JobSchema | undefined{
        this.jobQueueInterface.find({claimed: false, tries: {$lt: 5} }).sort( {createdAt: 1 }).limit(1).toArray((err: any, doc: any) => {
            if (err) {console.log("[ERROR] Worker: An error occured: "+ err.message); return(undefined)};
            if (doc[0]) {
                return(this.work(doc[0]))
            }
        });
        return(undefined);
    }

    updateTimeout() {
        /* Compute new timeout value here based on the
         * result returned by the operation.
         * You can use whatever calculation here that you want.
         * I don't know how you want to decide upon a new
         * timeout value, so I am just giving a random
         * example that calculates a new time out value
         * based on the return value of the repeated operation
         * plus an additional random number. */
        if (success_rate < 0.5) return 50;
        if (success_rate >= 0.5) return 25;
        /* This might have been NaN. We'll deal with that later. */
    }

    ScheduleWork(): void {
        var myJobQueueInterface = this.jobQueueInterface;

        proxyInterval(() => {
            this.work(this.getJob())
        }, this.updateTimeout);
    }

    async work (work: JobSchema | undefined): Promise<void> {
        if (!work) return;

        //console.log("[WORKER] Started working on "+ work.pageTitle)
        var myDataInterface = this.dataInterface;
        var myJobQueueInterface = this.jobQueueInterface;

        myJobQueueInterface.updateOne({_id: work._id}, {$set: {claimed: true}});

        if (work) {
            all_requests += 1;
            try {
                request(work.pageURL, { json: true }, (err, res, body) => {
                    if (err) { return console.log(err); }
                    if (body.entities != null && body.entities != undefined) {
                        myDataInterface.findOneAndUpdate(
                            //@ts-ignore
                            {id: Object.values(body.entities)[0].id}, 
                            {$set: Object.values(body.entities)[0]}, 
                            {upsert: true, returnOriginal: false}, 
                            function(err,doc) {
                                if (err) { console.log ("A MongoDB error occured: id = "+ work.pageTitle+ ", message: " + err.errmsg) }
                                if (doc) {myJobQueueInterface.deleteOne({_id: work._id}); accepted_requests += 1;/*console.log(`[WORKER] My work here (${work._id}) is done.`);*/}
                            });  
                    } else {
                        console.log ("An error occured (body.entities = null): id = "+work.pageTitle); 
                        myJobQueueInterface.updateOne({_id: work._id}, {$set: {claimed: false, tries: (work.tries + 1)}});
                    }
                });
            } catch (e) {
                console.log ("An error occured: id = "+work.pageTitle + ", message: " + e)
            } 
        }
        //})

    }

}

class JobsStatistician {
    jobQueueInterface: MongoClient.Collection;

    history_inserts_10s: Array<number>;

    constructor (args: {_dbconn: MongoClient.MongoClient, _dbn: string, _colln: string}) {
        this.jobQueueInterface = args._dbconn.db(args._dbn).collection(args._colln);

        this.history_inserts_10s = Array.apply(null, Array(10)).map(Number.prototype.valueOf,0);

        this.showStats();
    }

    showStats() {
        setInterval(() => {
            console.log("not implemented")
        }, 1000)
    }


}

var dbConnection_3 = new MongoClient.MongoClient(dbUrl);

var dbConnection = new MongoClient.MongoClient(dbUrl);

var dbConnection_2 = new MongoClient.MongoClient(dbUrl);

dbConnection_3.connect(function(err) {
    console.log("Connected successfully to server");

    postDbInit_3();

});

function postDbInit_2() {
    var worker: Worker = new Worker({
        _dbn: dbName,
        _collnQueue: collectionNameJobs,
        _collnData: collectionName,
        dbConnection: dbConnection_2
    })

}

function postDbInit() {
    var toq: StreamToqrikoq = new StreamToqrikoq({
        listeningURL: url,
        OffsetStr: new Date(timestamp).toISOString(),
        _dbn: dbName,
        _colln: collectionNameJobs,
        dbConnection: dbConnection
    })
}


function postDbInit_3() {
    var ctrl: MetaController = new MetaController({
        _dbn: dbName,
        _colln: collectionName + "Config",
        _dbc: dbConnection_3,
    })

    dbConnection.connect(function(err) {
        console.log("Connected successfully to server");
    
        postDbInit ();
    
    });
    
    dbConnection_2.connect(function(err) {
        console.log("Connected successfully to server");
    
        postDbInit_2();
    
    });
}

/**
 * Refactor notes:
 * @before a function that is pretty much procedural that takes any event and insert it into the wikidata database. Very much not consistent and single-threaded.
 * @after listeners and workers that work on the openConceptsInfra.wikidataRawJobs - 
 */