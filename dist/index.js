"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var eventsource_1 = __importDefault(require("eventsource"));
var request_1 = __importDefault(require("request"));
var mongodb_1 = __importDefault(require("mongodb"));
var url = 'https://stream.wikimedia.org/v2/stream/recentchange';
var wiki = "wikidatawiki";
var dbUrl = 'mongodb://localhost:27017';
var dbName = 'openConceptsInfra';
var collectionName = "wikidataRaw";
var collectionNameJobs = "wikidataRawJobs";
/* Controller-managed Global Variables */
var timestamp = 0; // KAFKA's event pointer.
var accepted_requests = 0;
var all_requests = 0;
var inserted_jobs = 0;
var success_rate = 0.0;
var queue_length = 0;
/* ----------------------------------- */
var MetaController = /** @class */ (function () {
    function MetaController(args) {
        var _this = this;
        this.dbInterface = args._dbc.db(args._dbn).collection(args._colln);
        this.fillTimestamp();
        setInterval(function () {
            _this.setTimestamp(timestamp);
            console.log("================ Last 30 seconds ================");
            console.log("Req+ " + accepted_requests + "                          Req- " + (all_requests - accepted_requests));
            console.log("Insertions per minute: " + (2 * accepted_requests));
            console.log("");
            console.log("Inserted jobs: " + inserted_jobs);
            console.log("Current Queue Length: " + queue_length);
            console.log("Job queued until date: " + new Date(timestamp).toISOString());
            console.log("Estimated remaininig time in days: " + (queue_length / (120 * 24 * accepted_requests)));
            console.log("=================================================");
            success_rate = accepted_requests / all_requests;
            accepted_requests = 0;
            all_requests = 0;
            inserted_jobs = 0;
        }, 30000); // every half a minute
    }
    MetaController.prototype.fillTimestamp = function () {
        this.dbInterface.find().limit(1).toArray(function (err, doc) {
            if (err) {
                console.log("This should NOT happen - failed to get timestamp from configuration database" + err.message);
                return (undefined);
            }
            ;
            if (doc[0]) {
                timestamp = doc[0].timestamp;
            }
        });
    };
    MetaController.prototype.setTimestamp = function (ts) {
        this.dbInterface.updateOne({ config: true }, { $set: { timestamp: ts } });
    };
    return MetaController;
}());
/**
 * StreamToqrikoq: Listens a Apache KAFKA event stream and distributes site updates to a job queue defined by a database location.
 * @etymology toqrikoq (t'oqrikoq): local leaders of the Mita system, typically manage a city and its hinterland.
 */
var StreamToqrikoq = /** @class */ (function () {
    function StreamToqrikoq(args) {
        this.url = "";
        this.OffsetStr = "";
        this.url = args.listeningURL;
        this.OffsetStr = args.OffsetStr;
        this.eventSource = new eventsource_1.default(this.url + "?since=" + args.OffsetStr);
        console.log("[DEBUG] Connecting to EventStreams at " + this.eventSource.url);
        this.jobQueueInterface = args.dbConnection.db(args._dbn).collection(args._colln);
        this.registerMessageListeners();
    }
    StreamToqrikoq.prototype.registerMessageListeners = function () {
        var myOnMessage = this.onMessage;
        var myOnError = this.onError;
        var myOnOpen = this.onOpen;
        var here = this;
        this.eventSource.addEventListener('message', function (e) {
            myOnMessage.call(here, e);
        });
        this.eventSource.addEventListener('error', function (e) {
            myOnError.call(here, e);
        });
        this.eventSource.addEventListener('open', function (e) {
            myOnOpen.call(here, e);
        });
    };
    StreamToqrikoq.prototype.onError = function (event) {
        //throw new Error("[DEBUG] An error occured.");
        // Try again!
        this.eventSource = new eventsource_1.default(this.url + "?since=" + this.OffsetStr);
    };
    StreamToqrikoq.prototype.onOpen = function (event) {
        console.log('[DEBUG] Successfully connected to EventStream.');
    };
    /**
     * onMessage: receives a message and pass it to jobs database.
     * @param event The Apache KAFKA event.
     */
    StreamToqrikoq.prototype.onMessage = function (event) {
        if (!event)
            return; // Don't waste time here
        // event.data will be a JSON string containing the message event.
        var change = JSON.parse(event.data);
        //if (change.wiki == wiki) console.log("Got update "+change.title);
        if (change.wiki == "wikidatawiki" && (change.title.charAt(0) == 'L' || change.title.charAt(0) == 'Q' || change.title.charAt(0) == 'P')) {
            //console.log(change);
            var finalTitle = change.title;
            timestamp = change.timestamp * 1000;
            //console.log("Time: ", timestamp)
            // Pre-process title string
            if (change.title.charAt(0) == 'L')
                finalTitle = change.title.substring(7);
            if (change.title.charAt(0) == 'P')
                finalTitle = change.title.substring(9);
            this.jobQueueInterface.updateOne({ pageTitle: finalTitle }, { $set: {
                    pageTitle: finalTitle,
                    pageURL: 'https://www.wikidata.org/wiki/Special:EntityData/' + finalTitle + '.json',
                    createdAt: Date.now(),
                    claimed: false,
                    tries: 0
                } }, { upsert: true }, function (err, r) {
                if (err)
                    throw new Error("[ERROR] An error occured when inserting to job queue: " + err.errmsg);
                //if (r) console.log("[LEADER] Job " + finalTitle + " inserted.")
                inserted_jobs += 1;
            });
        }
        this.jobQueueInterface.estimatedDocumentCount(function (err, count) {
            queue_length = count;
        });
        //console.log(JSON.parse(event.data));
    };
    return StreamToqrikoq;
}());
function proxyInterval(oper, delay) {
    var timeout = delay(oper());
    setTimeout(function () { proxyInterval(oper, delay); }, timeout - 0 || 1000);
    /* Note: timeout-0 || 1000 is just in case a non-numeric
     * timeout was returned by the operation. We also
     * could use a "try" statement, but that would make
     * debugging a bad repeated operation more difficult. */
}
var Worker = /** @class */ (function () {
    function Worker(args) {
        //console.log(`[DEBUG] Worker started!`);
        this.jobQueueInterface = args.dbConnection.db(args._dbn).collection(args._collnQueue);
        this.dataInterface = args.dbConnection.db(args._dbn).collection(args._collnData);
        this.ScheduleWork();
    }
    Worker.prototype.getJob = function () {
        var _this = this;
        this.jobQueueInterface.find({ claimed: false, tries: { $lt: 5 } }).sort({ createdAt: 1 }).limit(1).toArray(function (err, doc) {
            if (err) {
                console.log("[ERROR] Worker: An error occured: " + err.message);
                return (undefined);
            }
            ;
            if (doc[0]) {
                return (_this.work(doc[0]));
            }
        });
        return (undefined);
    };
    Worker.prototype.updateTimeout = function () {
        /* Compute new timeout value here based on the
         * result returned by the operation.
         * You can use whatever calculation here that you want.
         * I don't know how you want to decide upon a new
         * timeout value, so I am just giving a random
         * example that calculates a new time out value
         * based on the return value of the repeated operation
         * plus an additional random number. */
        if (success_rate < 0.5)
            return 50;
        if (success_rate >= 0.5)
            return 25;
        /* This might have been NaN. We'll deal with that later. */
    };
    Worker.prototype.ScheduleWork = function () {
        var _this = this;
        var myJobQueueInterface = this.jobQueueInterface;
        proxyInterval(function () {
            _this.work(_this.getJob());
        }, this.updateTimeout);
    };
    Worker.prototype.work = function (work) {
        return __awaiter(this, void 0, void 0, function () {
            var myDataInterface, myJobQueueInterface;
            return __generator(this, function (_a) {
                if (!work)
                    return [2 /*return*/];
                myDataInterface = this.dataInterface;
                myJobQueueInterface = this.jobQueueInterface;
                myJobQueueInterface.updateOne({ _id: work._id }, { $set: { claimed: true } });
                if (work) {
                    all_requests += 1;
                    try {
                        request_1.default(work.pageURL, { json: true }, function (err, res, body) {
                            if (err) {
                                return console.log(err);
                            }
                            if (body.entities != null && body.entities != undefined) {
                                myDataInterface.findOneAndUpdate(
                                //@ts-ignore
                                { id: Object.values(body.entities)[0].id }, { $set: Object.values(body.entities)[0] }, { upsert: true, returnOriginal: false }, function (err, doc) {
                                    if (err) {
                                        console.log("A MongoDB error occured: id = " + work.pageTitle + ", message: " + err.errmsg);
                                    }
                                    if (doc) {
                                        myJobQueueInterface.deleteOne({ _id: work._id });
                                        accepted_requests += 1; /*console.log(`[WORKER] My work here (${work._id}) is done.`);*/
                                    }
                                });
                            }
                            else {
                                console.log("An error occured (body.entities = null): id = " + work.pageTitle);
                                myJobQueueInterface.updateOne({ _id: work._id }, { $set: { claimed: false, tries: (work.tries + 1) } });
                            }
                        });
                    }
                    catch (e) {
                        console.log("An error occured: id = " + work.pageTitle + ", message: " + e);
                    }
                }
                return [2 /*return*/];
            });
        });
    };
    return Worker;
}());
var JobsStatistician = /** @class */ (function () {
    function JobsStatistician(args) {
        this.jobQueueInterface = args._dbconn.db(args._dbn).collection(args._colln);
        this.history_inserts_10s = Array.apply(null, Array(10)).map(Number.prototype.valueOf, 0);
        this.showStats();
    }
    JobsStatistician.prototype.showStats = function () {
        setInterval(function () {
            console.log("not implemented");
        }, 1000);
    };
    return JobsStatistician;
}());
var dbConnection_3 = new mongodb_1.default.MongoClient(dbUrl);
var dbConnection = new mongodb_1.default.MongoClient(dbUrl);
var dbConnection_2 = new mongodb_1.default.MongoClient(dbUrl);
dbConnection_3.connect(function (err) {
    console.log("Connected successfully to server");
    postDbInit_3();
});
function postDbInit_2() {
    var worker = new Worker({
        _dbn: dbName,
        _collnQueue: collectionNameJobs,
        _collnData: collectionName,
        dbConnection: dbConnection_2
    });
}
function postDbInit() {
    var toq = new StreamToqrikoq({
        listeningURL: url,
        OffsetStr: new Date(timestamp).toISOString(),
        _dbn: dbName,
        _colln: collectionNameJobs,
        dbConnection: dbConnection
    });
}
function postDbInit_3() {
    var ctrl = new MetaController({
        _dbn: dbName,
        _colln: collectionName + "Config",
        _dbc: dbConnection_3,
    });
    dbConnection.connect(function (err) {
        console.log("Connected successfully to server");
        postDbInit();
    });
    dbConnection_2.connect(function (err) {
        console.log("Connected successfully to server");
        postDbInit_2();
    });
}
/**
 * Refactor notes:
 * @before a function that is pretty much procedural that takes any event and insert it into the wikidata database. Very much not consistent and single-threaded.
 * @after listeners and workers that work on the openConceptsInfra.wikidataRawJobs -
 */ 
