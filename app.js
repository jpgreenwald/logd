var dbService = require('./db');
var dbClient = new dbService();
var async = require('async');

var restify = require('restify');

var server = restify.createServer();
server.use(restify.queryParser());
server.use(restify.gzipResponse());
server.use(restify.bodyParser());

server.pre(function (req, res, next)
{
    //always assume json
    req.headers['content-type'] = 'application/json';
    return next();
});


var saveLog = function (req, res, next)
{
    dbClient.cql('insert into events.logs (id, partition, log, source_ip) values (?, 1, ?, ?)',
        [
            dbClient.uuid4(),
            req.params.log,
            req.connection.remoteAddress
        ], function (err, results)
        {
            res.send(200, { status: 'ok' });
            return next();
        });
};

server.post('/log', saveLog);


//ensure that db is setup

console.log('checking keyspace and columnfamily');
async.series([
    function (done)
    {
        dbClient.cql("select * from events.logs limit 1", function (err, results)
        {
            if (err)
            {
                if (err.message.indexOf('Keyspace') >= 0)
                {
                    console.log('creating missing keyspace');
                    //missing keyspace so create default simple strategy keyspace
                    dbClient.cql("create keyspace events with replication = {'class':'SimpleStrategy', 'replication_factor':3} ;", null, function (err, result)
                    {
                        if (err)
                        {
                            console.log('failed to create db, exiting with err: %s', err);
                            process.exit(2);
                        }
                        console.log('creating missing columnfamily');
                        dbClient.cql("create table events.logs ( id uuid, partition int, log text, source_ip text, primary key((id, partition)))", null, function (err, result)
                        {
                            if (err)
                            {
                                console.log('failed to create db, exiting with err: %s', err);
                                process.exit(2);
                            }
                            done();
                        });
                    });
                }
                if (err.message.indexOf("columnfamily") >= 0)
                {
                    console.log('creating missing columnfamily');
                    dbClient.cql("create table events.logs ( id uuid, partition int, log text, source_ip text, primary key((id, partition)))", null, function (err, result)
                    {
                        if (err)
                        {
                            console.log('failed to create db, exiting with err: %s', err);
                            process.exit(2);
                        }
                        done();
                    });
                }
            }
            else
            {
                done();
            }
        });
    }
    , function (done)
    {
        console.log('db up to date');
        done();
    }, function (done)
    {
        server.listen(8080, function ()
        {
            console.log('%s listening at %s', server.name, server.url);
        });
        done();
    }]);





