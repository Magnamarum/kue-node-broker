// import * as express from "express";
// import * as cors from "cors";

// setup
const EventEmitter = require('events');
const workerEmitter = new EventEmitter();

var routerPort = process.env.ROUTERPORT || 5554;
var workersByType = {};

var zmq = require("zeromq");
var router = zmq.socket("router");
router.on("error", function (err) {
  console.log("SOCKET ERROR", err);
});
router.identity = "MessageBroker";

const redisAddress = process.env.REDIS_ADDRESS || "redis://localhost:6379";
const apiIdentity = process.env.ApiIdentity || "api";
var kue = require("kue"),
  queue = kue.createQueue({ redis: redisAddress });

const bindAddress = process.env.ZMQ_BIND_ADDRESS || `tcp://*:5554`;
router.bindSync(bindAddress);

function processJob(type, payload, done) {
  console.log('processing job of type '+type+ ' with payload '+payload );

  let workersToProcess = workersByType[type];
  let worker = workersToProcess.shift();
  router.send([worker, "", payload]);
  done();
}

console.log("MessageBroker listening on " + bindAddress);
router.on("message", function () {
  console.log(arguments);

  var argl = arguments.length,
    requesterIdentity = arguments[0].toString("utf8"),
    payload = JSON.parse(arguments[argl - 1].toString("utf8"));
  console.log("Received message from " + requesterIdentity);
  console.log("with body: ");
  console.log(payload);
  //console.log(message);
  console.log(payload.type);
  console.log(JSON.stringify(payload.body));
  if (payload.type == "RequestJob") {
    // queue.inactiveCount(payload.body.id, function(err, total) {
    //   if (err) {
    //     console.log(err);
    //   }
    //   if (total > 0) {
    var workers = workersByType[payload.body.id];
    if (workers) {
      console.log('Encolando worker '+requesterIdentity);
      workers.push(requesterIdentity);
    }
    else {
      
      console.log('Encolando worker '+requesterIdentity);
      workersByType[payload.body.id] = [requesterIdentity];
      console.log('Procesando cola '+payload.body.id);      
      queue.process(payload.body.id, function (job, done) {
        console.log(
          "Processing job " + payload.body.id + " with data " + job.data
        );
        console.log(workersByType);
        console.log(workersByType[payload.body.id]);
        if (!workersByType[payload.body.id]) { // si no hay workers disponibles
          console.log('No workers of '+payload.body.id+' available')
          workerEmitter.once(payload.body.id, processJob(payload.body.id, job.data, done));
        } else {
          processJob(payload.body.id, job.data, done);
        }
      });
    }
    workerEmitter.emit(payload.type);

    // }
    // });
  } 
  else if(payload.type == 'Response'){
    
    console.log('Received response to job '+payload.uid+' of type '+payload.type+' and body '+JSON.stringify(payload.body));    
    router.send([apiIdentity, "", JSON.stringify(payload)]);
  }
  else {
    console.log('Queueing job of type '+payload.type+' and body '+payload.body);
    queue.create(payload.type, JSON.stringify(payload)).save();
  }

});

// app
// const app = express();
// app.use(cors());

// app.get("/", async (req, res) => {
//   res.send("");
// });

// app.post("/", async (req, res) => {});

// app.listen(3000, function() {
//   console.log("listening on port 3000!");
// });
