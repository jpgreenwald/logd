var Db = function (hosts)
{
    var self = this;
    if (hosts === undefined)
    {
        hosts = 'localhost:9160';
    }
    var helenus = require('helenus'),
        pool = new helenus.ConnectionPool({
            hosts: [hosts],
            timeout: 3000
        });

    var uuid = require('node-uuid');

    this.uuid1 = function ()
    {
        return uuid.v1();
    };

    this.uuid4 = function ()
    {
        return uuid.v4();
    };

    this.cql = function (stmt, params, callback)
    {
        pool.on('error', function (err)
        {
            console.error(err.name, err.message);
        });

        pool.connect(function (err, keyspace)
        {
            if (err)
            {
                throw(err);
            }
            else
            {
                pool.cql(stmt, params, function (err, result)
                {
                    callback(err, result);
                });
            }
        });

    };
}

module.exports = Db;