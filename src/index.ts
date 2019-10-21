import EventSource from 'eventsource';
import request from 'request';
import MongoClient from 'mongodb';

var url = 'https://stream.wikimedia.org/v2/stream/recentchange';
var wiki = "wikidatawiki"

var dbUrl = 'mongodb://localhost:27017';
var dbName = 'openconcepts';
var collectionName = "wikidata_raw";

var currentUpdates = 0;

var dbConnection = new MongoClient.MongoClient(dbUrl);

dbConnection.connect(function(err) {
    console.log("Connected successfully to server");

    postDbInit ();

});

function exportStats () {
    console.timeEnd('timer')
    console.log('processed: ' + currentUpdates)
    currentUpdates = 0;
    console.time('timer')
}

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
        // event.data will be a JSON string containing the message event.
        var change = JSON.parse(event.data);
        //if (change.wiki == wiki) console.log("Got update "+change.title);
        if (change.wiki == wiki && (change.title.charAt(0) == 'L' || change.title.charAt(0) == 'Q' || change.title.charAt(0) == 'P')) {
            //console.log(change);
            
            var finalTitle = change.title;

            // Pre-process title string
            if (change.title.charAt(0) == 'L') finalTitle = change.title.substring(7);
            if (change.title.charAt(0) == 'P') finalTitle = change.title.substring(9);
            try {
                request('https://www.wikidata.org/wiki/Special:EntityData/'+finalTitle+'.json', { json: true }, (err, res, body) => {
                    if (err) { return console.log(err); }
                    //console.log(Object.values(body.entities)[0]);
                    const collection = dbConnection.db(dbName).collection(collectionName)
                    // @ts-ignore: type "unknown" is actually JSON object
                    //console.log("Update started "+finalTitle);
                    if (body.entities != null && body.entities != undefined) {
                        collection.findOneAndUpdate({id: finalTitle}, {$set: Object.values(body.entities)[0]}, {upsert: true, returnOriginal: false}, function(err,doc) {
                            if (err) { console.log ("An error occured: id = "+finalTitle) }
                            //else { console.log("Updated "+finalTitle+", _id: "+doc.value._id); }
                            currentUpdates ++;
                            if (currentUpdates >= 50) exportStats ();
                        });  
                    } else {console.log ("An error occured (body.entities = null): id = "+finalTitle); var waitTill = new Date(new Date().getTime() + 2 * 1000);
                    while(waitTill > new Date()){console.log("Cooling down!", (waitTill.getTime() - new Date().getTime()))}}
                });
            } catch (e) {
                console.log ("An error occured: id = "+finalTitle)
            }
        }
        //console.log(JSON.parse(event.data));
    };

}

