import EventSource from 'eventsource';
import request from 'request';
import MongoClient, { Timestamp, ObjectID } from 'mongodb';

var url = 'https://stream.wikimedia.org/v2/stream/recentchange';
var wiki = "wikidatawiki"

var dbUrl = 'mongodb://localhost:27017';
var dbName = 'openConceptsInfra';
var collectionName = "wikidataRaw";

var collectionNameJobs: string = "wikidataRawJobs";

interface JobSchema {
    pageTitle: string,
    pageURL: string,
    createdAt: Timestamp
}

/**
 * StreamToqrikoq: Listens a Apache KAFKA event stream and distributes site updates to a job queue defined by a database location.
 * @etymology toqrikoq (t'oqrikoq): local leaders of the Mita system, typically manage a city and its hinterland.
 */
class StreamToqrikoq {
    url: string = "";
    dateOffsetStr: string = "";

    eventSource: EventSource;
    jobQueueInterface: MongoClient.Collection;

    constructor (args: {listeningURL: string, dateOffsetStr: string, _dbn: string, _colln: string, dbConnection: MongoClient.MongoClient}) {
        this.url = args.listeningURL;
        this.dateOffsetStr = args.dateOffsetStr;

        this.eventSource = new EventSource(this.url + this.dateOffsetStr);
        console.log(`[DEBUG] Connecting to EventStreams at ${this.eventSource.url}`);

        this.jobQueueInterface = args.dbConnection.db(args._dbn).collection(args._colln);

        this.registerMessageListeners();
    }

    registerMessageListeners () {
        this.eventSource.onmessage = (evt) => this.onMessage.call(this, evt);
        this.eventSource.onopen = (evt) => this.onOpen.call(this, evt);
        this.eventSource.onerror = (evt) => this.onError.call(this, evt);
    }

    onError(event: MessageEvent | undefined): (evt: MessageEvent) => any {
        throw new Error("[DEBUG] An error occured.");
    }

    onOpen(event: MessageEvent | undefined): void {
        console.log ('[DEBUG] Successfully connected to EventStream.')
    }
    
    /**
     * onMessage: receives a message and pass it to jobs database.
     * @param event The Apache KAFKA event.
     */
    onMessage(event: MessageEvent | undefined): void {
        if (!event) return; // Don't waste time here
        // event.data will be a JSON string containing the message event.
        var change = JSON.parse(event.data);
        //if (change.wiki == wiki) console.log("Got update "+change.title);
        if (change.wiki == "wikidatawiki" && (change.title.charAt(0) == 'L' || change.title.charAt(0) == 'Q' || change.title.charAt(0) == 'P')) {
            //console.log(change);
            
            var finalTitle = change.title;

            // Pre-process title string
            if (change.title.charAt(0) == 'L') finalTitle = change.title.substring(7);
            if (change.title.charAt(0) == 'P') finalTitle = change.title.substring(9);

            this.jobQueueInterface.insertOne ({
                pageTitle: finalTitle,
                pageURL: 'https://www.wikidata.org/wiki/Special:EntityData/'+finalTitle+'.json',
                createdAt: Date.now()
            }, function(err, r) {
                if (err) throw new Error("[ERROR] An error occured.");
            });
        }
        //console.log(JSON.parse(event.data));
    }
}

class Worker {
    jobQueueInterface: MongoClient.Collection;
    dataInterface: MongoClient.Collection;

    constructor (args: {_dbn: string, _collnQueue: string, _collnData: string, dbConnection: MongoClient.MongoClient}) {
        console.log(`[DEBUG] Worker started!`);

        this.jobQueueInterface = args.dbConnection.db(args._dbn).collection(args._collnQueue);
        this.dataInterface = args.dbConnection.db(args._dbn).collection(args._collnData);

        this.ScheduleWork();
    }

    ScheduleWork(): void {
        setInterval(() => {
            this.jobQueueInterface.find().sort( {createdAt: 1 }).limit(1).toArray((err, doc) => {
                if (err) throw new Error("[ERROR] Worker: An error occured: "+ err.message);
                if (doc[0]) {this.work(doc[0]._id)}
            });
        }, 200) // every 0.2 second
    }

    work (workID: ObjectID): void {
        var myDataInterface = this.dataInterface;
        var myJobQueueInterface = this.jobQueueInterface;
        this.jobQueueInterface.findOne({_id: workID}, function (err, document) {
            if (err) { throw new Error("[ERROR] something happened: " + err.message);}
            if (document) {
                try {
                    request(document.pageURL, { json: true }, (err, res, body) => {
                        if (err) { return console.log(err); }
                        //console.log(Object.values(body.entities)[0]);
                        // @ts-ignore: type "unknown" is actually JSON object
                        //console.log("Update started "+finalTitle);
                        if (body.entities != null && body.entities != undefined) {
                            myDataInterface.findOneAndUpdate(
                                {id: document.pageTitle}, 
                                {$set: Object.values(body.entities)[0]}, 
                                {upsert: true, returnOriginal: false}, 
                                function(err,doc) {
                                    if (err) { console.log ("An error occured: id = "+ document.pageTitle) }
                                    //else { console.log("Updated "+finalTitle+", _id: "+doc.value._id); }
                                    if (doc) {myJobQueueInterface.deleteOne({_id: workID}); console.log(`[WORKER] My work here (${workID}) is done.`)}
                                });  
                        } else {console.log ("An error occured (body.entities = null): id = "+document.pageTitle);}
                    });
                } catch (e) {
                    console.log ("An error occured: id = "+document.pageTitle)
                } 
            }
        })
    }

}

var dbConnection = new MongoClient.MongoClient(dbUrl);

dbConnection.connect(function(err) {
    console.log("Connected successfully to server");

    postDbInit ();

});

function postDbInit () {
    var toq: StreamToqrikoq = new StreamToqrikoq({
        listeningURL: url,
        dateOffsetStr: "",
        _dbn: dbName,
        _colln: collectionNameJobs,
        dbConnection: dbConnection
    })

    var worker: Worker = new Worker({
        _dbn: dbName,
        _collnQueue: collectionNameJobs,
        _collnData: collectionName,
        dbConnection: dbConnection
    })

}

/*
function postDbInit () {

    console.time('timer')

    console.log(`Connecting to EventStreams at ${url}`);
    var eventSource = new EventSource(url);

    eventSource.onopen = function(event) {
        console.log('--- Opened connection.');
    };

    eventSource.onerror = function(event) {
        console.error('--- Encountered error', event);
    };

    eventSource.onmessage = function(event) {
        
    };

}*/

/**
 * Refactor notes:
 * @before a function that is pretty much procedural that takes any event and insert it into the wikidata database. Very much not consistent and single-threaded.
 * @after listeners and workers that work on the openConceptsInfra.wikidataRawJobs - 
 */