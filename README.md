# SQLCacheService
A simple C# SQL query cache service designed for large static rarely changed data such as directories, dictionaries, site configuration, etc. All data save in cache and then transfer to the socket client in JSON format as arrays without property names to save the size of data. It is able to convert normal SQL responses to JSON arrays. The list of queries can be preloaded from a configuration file in JSON format or added dynamically by socket clients. The limited version has not included any security features and can be used in the inner infrastructure.

This service can speed up requests to SQL servers for static data even in the case when data was not saved in the cache preliminary. While accessing the data it is possible of splitting the request into two operations. At the beginning of the web script, an app needs to open a non-blocking socket connection to SQLCacheService. Later after some initial job, the script can read socket data without blocking. Keeping the connection open it is possible to request other data from/through the cache.

Limited versions support only MySQL and PostgreSQL data providers.
### Config file example:

<pre><code>
{
    "ListenPort": 999,
    "DefaultTimer": 600,
    "Queries": [
        {
            "db_type": "MySqlConnection",
            "json_result": 1,
            "query_id": "query1",
            "sql": "SELECT json_array(f1, f2, f3) as '0' FROM big_table",
            "timer": 1,
            "connection_string": "SERVER=localhost;PIPENAME=\\\\.\\pipe\\MYSQL8;DATABASE=test;UID=user;PASSWORD=1234;"
        },
        {
            "db_type": "MySqlConnection",
            "json_result": 2,
            "query_id": "query2",
            "sql": "SELECT json_arrayagg(json_array(f1, f2, f3)) as '0' FROM big_table",
            "timer": 1,
            "connection_string": "SERVER=localhost;PORT=3306;DATABASE=test;UID=user;PASSWORD=1234;"
        },
        {
            "db_type": "NpgsqlConnection",
            "json_result": 0,
            "datetime_format": "yyyy-MM-dd HH:mm:ss",
            "query_id": "query3",
            "sql": "SELECT * FROM big_table",
            "timer": 1,
            "connection_string": "SERVER=localhost;PORT=3306;DATABASE=test;UID=user;PASSWORD=1234;"
        }
    ]
}
</code></pre>

**ListenPort** - TCP port.

**DefaultTimer** - Time in seconds to reload all queries.

**Queries** - The array of queries objects: 

**sql** - SQL command.

**connection_string** - The connection string to the server. A stored data can be found by comparing query_id and connection string.

**db_type** - MySqlConnection or NpgsqlConnection.

**query_id** - Query id.

**datetime_format** - Format of DateTime fields "yyyy-MM-dd HH:mm:ss", "u" or so on for converting to JSON when json_result=0.

**json_result** - specify the type of response from the SQL server:
0 - Normal SQL response. It will convert to a JSON array of arrays.
1 - Each row is JSON. It will convert to a JSON array of arrays.
2 - JSON array of arrays. Save as it is.

To add a new query to cache dynamically by TCP connection use **"cmd": 10** with the Query object format.

<pre><code>
{
    "cmd": 10,
    "db_type": "NpgsqlConnection",
    "json_result": 0,
    "datetime_format": "yyyy-MM-dd HH:mm:ss",
    "query_id": "query4",
    "sql": "SELECT * FROM public.oc_category_description;",
    "timer": 1,
    "connection_string": "SERVER=1.2.3.4;PORT=3306;DATABASE=test;UID=user;PASSWORD=1234;"
}
</code></pre>

### Retrieve saved data

Saved data will search by comparing two values: **query_id** and **connection_string**. Below there is an example of a client request to get saved data. If an app is trying to add a new query to the list the Service first will check by query_id and connection string the existence of data. If it is already present no new query will be added. The "sql" fields of stored data do not compare.

<pre><code>
{
    "query_id": "query3",
    "connection_string": "SERVER=localhost;PORT=3306;DATABASE=test;UID=user;PASSWORD=1234;"
}
</code></pre>


