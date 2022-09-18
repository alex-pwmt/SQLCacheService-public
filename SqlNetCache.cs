using System.Data;
using MySqlConnector;
using Npgsql;
using System.Data.Common;
//using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Timers;
using Timer = System.Timers.Timer;

namespace SqlCacheService
{
	public struct SqlResultBuffer
	{
		public int Readers;
		public int Rows;
		public int OutBufferSize;
		public MemoryStream? OutMemoryStream;
	}

	public class QueryData
	{
		const int BuffersNumber = 4;

		public QueryData( string connectionString )
		{
			BufferList = new SqlResultBuffer[BuffersNumber];
			ConnectionString = ReBuildConnectionString( connectionString, _connectionDict, true );
		}
		//
		public string? QueryId;
		public string? Sql;
		public string? DbType;
		public string? DtFormat;
		public readonly string? ConnectionString;
		public int JsonResultType;
		public int Timer;
		public int TimerCount;
		//
		public int ConnectionIndex;
		public int Uploaded;
		public int Updated;
		public int Rows;
		public int DataSize;
		public int MaxBufferIndex;
		
		public readonly object LockQuery = new object();
		public readonly SqlResultBuffer[] BufferList;
		public int CurrentBufferIndex = -1;

		readonly Dictionary<string, string> _connectionDict = new Dictionary<string, string>();

		public string ConnectionServer => _connectionDict["SERVER"];

		/// <summary>
		/// Rebuild connection string in the same order.
		/// </summary>
		/// <param name="connectionString">Connection string to rebuild.</param>
		/// <param name="connectionDict">Dictionary where to save parts.</param>
		/// <param name="addDefaults"></param>
		/// <returns>Reordered connection string.</returns>
		static string ReBuildConnectionString( string connectionString, IDictionary<string, string> connectionDict, bool addDefaults )
		{
			var conSplit = connectionString.Split(';');
			foreach( var cs in conSplit )
			{
				var partS = cs.Trim();
				if( partS.Length==0 ) continue;

				var part = partS.Split('=');
				part[0] = part[0].Trim().ToUpper();
				part[0] = part[0] switch
				{
					"PIPE" => "PIPENAME",
					"PWD" => "PASSWORD",
					_ => part[0]
				};
				connectionDict.Add( part[0], part[1].Trim() );
			}

			if( addDefaults )
			{
				if( !connectionDict.ContainsKey( "POOLING" ) )
				{
					connectionDict["Pooling"] = "True";
				}
			}
			
			return string.Join( ';', connectionDict
				.OrderBy( pair => pair.Key )
				.Select( (k) => k.Key+'='+k.Value ) );
		}
		
		/// <summary>
		/// Compare connection string with saved in QueryData object by fields: UID, PWD, DATABASE, SERVER.
		/// </summary>
		/// <param name="connectionString">Connection string to compare.</param>
		/// <returns>True if equal.</returns>
		public bool CompareConnectionString( string connectionString )
		{
			var connectionDict = new Dictionary<string, string>();
			ReBuildConnectionString( connectionString, connectionDict, false );
			
			// Compare with _connectionDict and connectionDict by UID, PWD, DATABASE, SERVER
			return _connectionDict["UID"]==connectionDict["UID"] 
			       && _connectionDict["PASSWORD"]==connectionDict["PASSWORD"]
			       && _connectionDict["SERVER"]==connectionDict["SERVER"]
			       && _connectionDict["DATABASE"]==connectionDict["DATABASE"];
		}

		/// <summary>
		/// Return index of free buffer from the list of BuffersNumber buffers.
		/// </summary>
		/// <returns>Index of free buffer in SqlResultBuffer[].</returns>
		/// <exception cref="IndexOutOfRangeException">There is no free buffer with Readers count equal 0.</exception>
		public int GetFreeSqlBuffer()
		{
			for( var i=0; i<BuffersNumber; i++ )
			{
				if( i!=CurrentBufferIndex && BufferList[i].Readers==0 )
				{
					BufferList[i].Rows = 0;
					BufferList[i].OutBufferSize = 0;
					if( BufferList[i].OutMemoryStream==null )
					{
						BufferList[i].OutMemoryStream = new MemoryStream();
					}
					else
					{
						BufferList[i].OutMemoryStream!.SetLength(0);
					}
					return i;
				}
			}
			// TODO All buffer for SQL queries are looked!
			throw new IndexOutOfRangeException( "All buffer for SQL queries are looked!" );
		}

		/// <summary>
		/// Free old buffers that are out of date.
		/// </summary>
		public int RemoveSqlBuffer()
		{
			var k = 0;
			for( var i=0; i<BuffersNumber; i++ )
			{
				if (BufferList[i].Readers==0 && i!=CurrentBufferIndex)
				{
					k++;
					if( BufferList[i].OutMemoryStream!=null )
					{
						BufferList[i].OutMemoryStream?.SetLength(0);
					}
				}
			}
			return k;
		}
	}

	/// <summary>
	/// Encapsulate thread safety methods to work with List of QueryData.
	/// </summary>
	internal class QueryController
	{
		public readonly List<QueryData> QueryList = new List<QueryData>();

		public void AddQueryData( QueryData cd )
		{
			lock( QueryList )
			{
				if (cd.QueryId!=null && FindQueryData(cd.QueryId)==-1)
				{
					QueryList.Add(cd);
				}
			}
		}
		public void RemoveQueryData( string queryId )
		{
			lock( QueryList )
			{
				QueryList.RemoveAt(QueryList.FindIndex( listItem => listItem.QueryId==queryId) );
			}
		}
		public int FindQueryData( string queryId )
		{
			int result;
			lock( QueryList )
			{
				result = QueryList.FindIndex( listItem => listItem.QueryId==queryId );
			}
			return result;
		}
		public int FindQueryData(string queryId, string connectionString  )
		{
			int result;
			lock( QueryList )
			{
				result = QueryList.FindIndex( listItem => 
					listItem.QueryId==queryId && listItem.CompareConnectionString(connectionString) );
			}
			return result;
		}
	}

	/// <summary>
	/// Method for work with DbConnection object of different providers and keep open and reuse.
	/// </summary>
	internal class Dbw : IDisposable
	{
		//readonly string _provider;
		readonly Func<DbConnection> _dbwConnection;
		readonly string _connectionString;
		long _useCount;
		DbConnection? _connectionObject;
		readonly CancellationToken _cancellationToken;

		public Dbw( string dbType, string connectionString, CancellationToken cancellationToken )
		{
			//_provider = dbType;
			_cancellationToken = cancellationToken;
			_connectionString = connectionString;
			_dbwConnection = dbType switch
			{
				"MySqlConnection" => DbwMySqlConnection,
				"NpgsqlConnection" => DbwNpgConnection,
				_ => ThrowNotImplemented
			};
		}

		public DbConnection? ConnectionObject
		{
			get {
				if (BeginUse()==0)
				{
					if( _connectionObject is null )
					{
						_connectionObject = _dbwConnection();
						_connectionObject.OpenAsync().Wait( _cancellationToken );
					}
					if( (_connectionObject.State & ConnectionState.Open)!=0 ) return _connectionObject; 
					_connectionObject.Close();
				}
				return null;
			}
		}

		/// <summary>
		/// Check if this DbConnection object is currently in use. If somebody uses it to make a clone.
		/// </summary>
		/// <returns>0 if DbConnection was locked, otherwise 1.</returns>
		long BeginUse() => Interlocked.CompareExchange( ref _useCount, 1, 0 );

		/// <summary>
		/// Free DbConnection object. Set internal _useCount to 0.
		/// </summary>
		/// <returns></returns>
		public long EndUse() => Interlocked.Exchange( ref _useCount, 0 );
		
		/// <summary>
		/// Close associated object.
		/// </summary>
		public void Close()
		{
			// nothing to wait here
			_connectionObject?.CloseAsync();
			Interlocked.Exchange( ref _useCount, 0 );
		}

		// MySQL
		DbConnection DbwMySqlConnection() => new MySqlConnection( _connectionString );
		
		// PostgreSQL
		DbConnection DbwNpgConnection() => new NpgsqlConnection( _connectionString );

		// Not Implemented
		DbConnection ThrowNotImplemented()
		{
			// todo NotImplementedException!
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			ConnectionObject?.Dispose();
		}
	}
 
	/// <summary>
	/// Abstract class of external methods. Reloaded in SqlNetCache.
	/// </summary>
	public abstract class SqlNetCacheData
	{
		/// <summary>
		/// Looking for stored query by <b>id</b> and <b>connection string</b> and increase readers count. 
		/// </summary>
		/// <param name="id">Query <b>id</b></param>
		/// <param name="connection">DBConnection object <b>connection string</b>.</param>
		/// <returns>Tuple (int bufferIndex, int queryIndex, MemoryStream? dataStream) where
		/// bufferIndex - index in BufferList (SqlResultBuffer[]) or -1 if the query have not found.,
		/// queryIndex - index in QueryList (list of QueryData) or -1 if the query have not found.
		/// dataStream - MemoryStream object with the data or null.</returns>
		public abstract ( int bufferIndex, int queryIndex, MemoryStream? dataStream ) StartReadQueryDataById( string id, string connection );
		public abstract ( int bufferIndex, int queryIndex, MemoryStream? dataStream ) StartReadQueryDataById( string id );
		
		/// <summary>
		/// Decrease readers count of the SqlResultBuffer object by bufferIndex and queryIndex
		/// returned by StartReadQueryDataById. Should be call after <b>StartReadQueryDataById()</b>.
		/// </summary>
		/// <param name="bufferIndex">bufferIndex - index in BufferList (SqlResultBuffer[])</param>
		/// <param name="queryIndex">queryIndex - index in QueryList (list of QueryData)</param>
		/// <returns>true if success.</returns>
		public abstract bool StopReadQueryDataById( int bufferIndex, int queryIndex );
		
		/// <summary>
		/// Fetching data from SQL server.
		/// </summary>
		/// <param name="connectionString">Should be rebuild by ReBuildConnectionString() before call.</param>
		/// <param name="sql">SQL command string.</param>
		/// <param name="jsonResultType">Type of result. See JSON config file description.</param>
		/// <param name="memoryStream">MemoryStream to which save the data.</param>
		/// <param name="dbType">MySqlConnection or NpgsqlConnection</param>
		/// <param name="dtFormat">Format of DateTime fields for converting to JSON when json_result=0.</param>
		/// <returns>Task int - number of fetched rows.</returns>
		public abstract Task<int> FetchSqlData(string? connectionString, string? sql, int jsonResultType, 
			MemoryStream memoryStream, string dbType,  string? dtFormat );
		
		/// <summary>
		/// Add new QueryData object with saved data to the list.
		/// </summary>
		/// <param name="cd">QueryData object.</param>
		/// <returns>0 if success or -1.</returns>
		public abstract int AddNewQuery( QueryData? cd );

		/// <summary>
		/// Parse Json object with a request to add query to cache.
		/// </summary>
		/// <param name="request">String to parse.</param>
		/// <returns>Tuple with the request field (ok, cmd, jsonResult, queryId,
		/// connectionString, dbType, sql, dtFormat). <b>ok</b> - true is success.</returns>
		public abstract (bool ok, int cmd, int jsonResult, int timer, string? queryId, string? connectionString, 
						string? dbType, string? sql, string? dtFormat) ParseJsonRequest( string request );

		/// <summary>
		/// Display the list of stored queries.
		/// </summary>
		public abstract void DisplayStoredQueries( StringBuilder output );

		/// <summary>
		/// Push to reload all queries.
		/// </summary>
		public abstract void ReloadQueries( StringBuilder output );

		/// <summary>
		/// Push to reload all queries.
		/// </summary>
		public abstract void DisplayTcpClients( StringBuilder output );
	}

	/// <summary>
	/// Cached big result queries which changes very seldom (like a config data) as json an array of arrays
	/// and spread for sockets client. Cached queries updates by timer. Queries can be preloaded from a config
	/// file or added dynamically by client. Client select query from cache by string query id.
	/// This is only to use for inner infrastructure and suitable for caching remote data and local data. 
	/// </summary>
	public class SqlNetCache : SqlNetCacheData
	{
		//readonly IConfiguration _configuration;
		//readonly int _connectionLimit = 1;
		int _onExit;
		int _queriesNumber;
		readonly int _listenPort = 38888;
		readonly int _defaultTimer = 60;
		readonly bool _configured;
		readonly QueryController _queryController = new QueryController();
		readonly List<string> _sqlConnectionList = new List<string>();
		readonly CancellationToken _cancellationToken;
		readonly ILogger _logger;
		readonly Dictionary<string, Dbw> _connectionPool = new Dictionary<string, Dbw>();

		Timer? _systemTimer;
		Task? _sqlTask;
		Task? _listenerTask;
		TcpSocketListener? _tcpListener;

		public override int AddNewQuery( QueryData? cd )
		{
			if(cd is null) return -1;
			_queryController.AddQueryData(cd);
			_queriesNumber++;
			return 0;
		}
		public override ( int, int, MemoryStream? ) StartReadQueryDataById( string id, string connection )
		{
			int buffer;
			int index = _queryController.FindQueryData( id, connection );
			
			if (index==-1)
			{
				return (-1, -1, null);
			}

			lock( _queryController.QueryList[index].LockQuery )
			{
				// Here it is needed to get CurrentBufferIndex and increase Readers
				buffer = _queryController.QueryList[index].CurrentBufferIndex;
				if ( buffer==-1 ) return (-1, -1, null);
				// Return is OK inside of lockq
				_queryController.QueryList[index].BufferList[buffer].Readers++;
			}

			return ( buffer, index, _queryController.QueryList[index].BufferList[buffer].OutMemoryStream );
		}

		public override (int, int, MemoryStream?) StartReadQueryDataById( string id )
		{
			int buffer;
			var index = _queryController.FindQueryData(id);
			if( index==-1 ) return ( -1, -1, null );

			lock (_queryController.QueryList[index].LockQuery)
			{
				buffer = _queryController.QueryList[index].CurrentBufferIndex;
				if ( buffer==-1 ) return ( -1, -1, null );
				// return is OK here
				_queryController.QueryList[index].BufferList[buffer].Readers++;
			}
			
			return ( buffer, index, _queryController.QueryList[index].BufferList[buffer].OutMemoryStream );
		}
		
		public override bool StopReadQueryDataById( int bufferIndex, int queryIndex )
		{
			// doest need to use lock here 
			Interlocked.Add( ref _queryController.QueryList[queryIndex].BufferList[bufferIndex].Readers, -1 );
			Interlocked.Add( ref _queryController.QueryList[queryIndex].Uploaded, 1 );
			return true;
		}

		/// <summary>
		/// Unpack dictionary with string, object of JSON query object.
		/// </summary>
		/// <param name="queryConfigDic">Dictionary type string, object</param>
		/// <returns>Tuple represent query data object with the field "ok" which is true when operation success.</returns>
		static (bool ok, int cmd, int jsonResult, int timer, string? queryId, 
			string? connectionString, string? dbType, string? sql, string? dtFormat) 
			UnpackJsonDicTuple( IReadOnlyDictionary<string, object>? queryConfigDic )
		{
			bool ok = false;
			int cmd = -1;
			int jsonResult = 0;
			int timer = 1;
			string? queryId = null;
			string? connectionString = null;
			string? dbType = null;
			string? dtFormat = null;
			string? sql = null;

			if( queryConfigDic==null ) return ( ok, cmd, jsonResult, timer, queryId, connectionString, dbType, sql, dtFormat );

			try
			{
				object? node;
				
				// int
				cmd = queryConfigDic.TryGetValue("cmd", out node) ? ((JsonElement)node).GetInt32():1;
				jsonResult = queryConfigDic.TryGetValue("json_result", out node) ? ((JsonElement)node).GetInt32():-1;
				timer = queryConfigDic.TryGetValue("timer", out node) ? ((JsonElement)node).GetInt32():1;
				
				// string
				queryId = queryConfigDic.TryGetValue("query_id", out node) ? node.ToString():null;
				connectionString = queryConfigDic.TryGetValue("connection_string", out node) ? node.ToString():null;
				dbType = queryConfigDic.TryGetValue("db_type", out node) ? node.ToString():null;
				dtFormat = queryConfigDic.TryGetValue("datetime_format", out node) ? node.ToString():Globals.TextDateFormat;
				sql = queryConfigDic.TryGetValue("sql", out node) ? node.ToString():null;

				if (cmd > 10) cmd = 1;

				if( connectionString==null || queryId==null 
					|| (cmd==10 && (dbType==null || sql==null || jsonResult is < 0 or > 2 )) )
				{
					connectionString = Globals.ParseJsonError;
				}
				else
				{
					ok = true;
				}
			}
			catch( Exception ex ) when( ex is ArgumentNullException or FormatException )
			{
				Console.WriteLine( Globals.Exception + ex.Message );
				connectionString = ex.Message;
			}

			return ( ok, cmd, jsonResult, timer, queryId, connectionString, dbType, sql, dtFormat );
		}
		
		public override (bool,int,int,int,string?,string?,string?,string?,string?) ParseJsonRequest( string request )
		{
			string errorMessage;

			try
			{
				JsonDocument jsonConfig = JsonDocument.Parse(request,
					new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip });
				var queryConfigDic = jsonConfig.RootElement.Deserialize<Dictionary<string, object>>();
				return UnpackJsonDicTuple(queryConfigDic);
			}
			catch( Exception ex ) when( ex is JsonException or ArgumentException or NotSupportedException )
			{
				Console.WriteLine( Globals.Exception + ex.Message);
				errorMessage = ex.Message;
			}

			if( errorMessage.Length>0 )
			{
				_logger.LogError( Globals.LoggerTemplate, DateTimeOffset.Now, errorMessage );
			}
			
			return ( false, -1, 0, 1, null, errorMessage, null, null, null );
		}

		public SqlNetCache( CancellationToken cancellationToken, ILogger logger, IConfiguration configuration )
		{
			_cancellationToken = cancellationToken;
			_logger = logger;
			//_configuration = configuration;

			var fileName = configuration["config"];
			try
			{
				if( fileName==null )
				{
					// Initialize config from json file
					// fileName = @"/mnt/e/Users/Alex/source/TestData/configfileLinux.json";
					fileName = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? "configfileLinux.json" : "configfile.json";
				}
				
				var json = File.ReadAllText(fileName);
				JsonDocument jsonConfig = JsonDocument.Parse( json, new JsonDocumentOptions{ CommentHandling = JsonCommentHandling.Skip });

				if (jsonConfig.RootElement.TryGetProperty("ListenPort", out JsonElement gradeElement))
				{
					_listenPort = gradeElement.GetInt32();
				}

				if (jsonConfig.RootElement.TryGetProperty("DefaultTimer", out gradeElement))
				{
					_defaultTimer = gradeElement.GetInt32();
				}

				// if( jsonConfig.RootElement.TryGetProperty("ConnectionLimit", out gradeElement) )
				// {
				// 	_connectionLimit = gradeElement.GetInt32();
				// }

				if( jsonConfig.RootElement.TryGetProperty( "Queries", out gradeElement ) )
				{
					var queriesConfigDict = gradeElement.Deserialize<Dictionary<string, object>[]>();

					if( queriesConfigDict!=null )
					{
						_queriesNumber = queriesConfigDict.Length;
						if (_queriesNumber <= 0)
						{
							_queriesNumber = 0;
							return;
						}

						var k = 0;
						foreach (var queryConfig in queriesConfigDict)
						{
							if (!_sqlConnectionList.Contains( queryConfig["connection_string"].ToString()! ))
							{
								_sqlConnectionList.Add(queryConfig["connection_string"].ToString()!);
								k++;
							}

							var queryJson = UnpackJsonDicTuple(queryConfig);
								
							if( queryJson.ok )
							{
								_queryController.AddQueryData(new QueryData( queryJson.connectionString! )
								{
									JsonResultType = queryJson.jsonResult,
									ConnectionIndex = k - 1,
									Timer = queryJson.timer,
									QueryId = queryJson.queryId,
									Sql = queryJson.sql,
									DbType = queryJson.dbType,
									DtFormat = queryJson.dtFormat,
									TimerCount = queryJson.timer,			// to fetch at the begin 
								});
							}
							else
							{
								_logger.LogError( Globals.LoggerTemplate, DateTimeOffset.Now, queryJson.connectionString );
							}
						}

						_configured = true;
					}
				}
			}
			catch ( JsonException ex )
			{
				_logger.LogError(Globals.LoggerTemplate, DateTimeOffset.Now, ex.Message );
			}
			catch ( Exception ex ) when( ex is FileNotFoundException or DirectoryNotFoundException )
			{
				_logger.LogError(Globals.LoggerTemplate, DateTimeOffset.Now, Globals.FileNotFoundError + ex.Message );
				_configured = true;
			}
		}

		/// <summary>
		/// Set system timer to fire SqlDataReader every _defaultTimer seconds. For the first time it sets timer to minimum. 
		/// </summary>
		public Task StartService()
		{
			_tcpListener = new TcpSocketListener( _listenPort, this, _cancellationToken, _logger );
			_listenerTask = new Task( () => _tcpListener.StartListening(), _cancellationToken );

			if( !_configured )
			{
				_logger.LogError( Globals.LoggerTemplate, DateTimeOffset.Now, Globals.ConfigError );
				return _listenerTask;
			}
			
			SqlDataReader().Wait( _cancellationToken );
			_listenerTask.Start();
			
			// Start data reader
			#if DEBUG && VERBOSE
				Console.Write("\nStarting timer task in thread = {0}.", Thread.CurrentThread.ManagedThreadId);
			#endif
			
			_systemTimer = new Timer( TimeSpan.FromSeconds(_defaultTimer).TotalMilliseconds );
			_systemTimer.Elapsed += TimeHandler!;
			_systemTimer.AutoReset = false;
			_systemTimer.Start();
			
			return _listenerTask;
		}

		/// <summary>
		/// TimeHandler check exit flag, run SqlDataReader() and run TcpSocketListener in the first call.
		/// </summary>
		/// <param name="sender"></param>
		/// <param name="elapsed"></param>
		void TimeHandler( object sender, ElapsedEventArgs elapsed )
		{
			_systemTimer?.Stop();

			if( _onExit>0 )
			{
				_sqlTask!.Dispose();
				return;
			}

			#if DEBUG && CONSOLE && VERBOSE
				Console.Write("\nTimeHandler() in thread={0}, RunCounter={1}\n", Environment.CurrentManagedThreadId, _sqlRunCounter);
			#endif

			_sqlTask = Task.Run( SqlDataReader, _cancellationToken );
			_sqlTask.Wait( _cancellationToken );
			
			if( _onExit==0 )
			{
				_systemTimer!.Interval = TimeSpan.FromSeconds(_defaultTimer).TotalMilliseconds; //_defaultTimer
				_systemTimer.Start();
			}
		}

		/// <summary>
		/// Called by timer every DefaultTimer seconds to reload sql data.
		/// </summary>
		async Task SqlDataReader()
		{
			// Reload all queries (or load for the first time) from QueryController.QueryList.
			// For each request, store the new data in a new buffer and then overwrite the current buffer number with a short lock.
			// During the next call of SqlDataReader() all old buffered data with Readers==0 can be released.
			// Probably kill stuck requests that haven't had clients in a while.
			// If a new cache request comes from a client over the network, then the download occurs in the context
			// network service tasks. When the fetching data is finished the new query will be added to the list with
			// a short lock of the list of queries.

			foreach( QueryData query in _queryController.QueryList )
			{
				query.TimerCount++;
				if( query.TimerCount<query.Timer ) continue;
				query.TimerCount = 0;

				var nextBuffer = query.GetFreeSqlBuffer();
				query.BufferList[nextBuffer].OutMemoryStream ??= new MemoryStream();
				var rows = await FetchSqlData( query.ConnectionString, 
					query.Sql, query.JsonResultType, query.BufferList[nextBuffer].OutMemoryStream!, query.DbType!, query.DtFormat );

				if( rows==0 )
				{
					query.CurrentBufferIndex = -1;
				}

				if( query.MaxBufferIndex<nextBuffer ) query.MaxBufferIndex = nextBuffer;
				query.BufferList[nextBuffer].Rows = rows;
				query.DataSize = (int)query.BufferList[nextBuffer].OutMemoryStream!.Length;
				query.Updated++;
				query.Rows = rows;

				lock( query.LockQuery )
				{
					query.CurrentBufferIndex = nextBuffer;
				}
				
				#if DEBUG && VERBOSE
					int free = query.RemoveSqlBuffer();
					Console.WriteLine($"Updated {query.QueryId} in thread {Environment.CurrentManagedThreadId}");
					Console.WriteLine($"There are {free} free buffers. Selected index {nextBuffer}");
				#else
					query.RemoveSqlBuffer();
				#endif
			}
		}

		public override async Task<int> FetchSqlData( string? connectionString, string? sql, int jsonResultType,
			MemoryStream memoryStream, string dbType, string? dtFormat )
		{
			if (connectionString==null || sql==null) return 0;

			int rows = 0;
			string errorMessage = "";
			bool newConnection = false;

			JsonWriterOptions writerOptions = new JsonWriterOptions { Indented = false, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };
			Utf8JsonWriter jsonWriter = new Utf8JsonWriter( memoryStream, writerOptions );
			StreamWriter streamWriter = new StreamWriter( memoryStream );

			Dbw? db = null;
			DbConnection? dbcon;
			DbCommand? dbcmd = null;

			dtFormat ??= Globals.TextDateFormat;
			try
			{
				if( !_connectionPool.ContainsKey(connectionString) )
				{
					db = new Dbw( dbType, connectionString, _cancellationToken );
					_connectionPool.Add(connectionString, db);
				}
				else
				{
					db = _connectionPool[connectionString];
				}

				// null is possible here if this object is locked
				dbcon = db.ConnectionObject;

				if( dbcon is null )
				{
					// Create a new Dbw for a once use
					db = new Dbw(dbType, connectionString, _cancellationToken);
					dbcon = db.ConnectionObject;
					newConnection = true;
                    _logger.LogInformation( Globals.LoggerTemplate, DateTimeOffset.Now, Globals.TextDbObjectInBusy );
                }

                dbcmd = dbcon!.CreateCommand();
				dbcmd.CommandText = sql;

				if (jsonResultType==2)
				{
					// received a long string as a json array of arrays with
					var reader = await dbcmd.ExecuteScalarAsync();
					streamWriter.Write(reader);
					streamWriter.Write("\n\r");
					streamWriter.Flush();
					rows = 1;
				}
				else if( jsonResultType==1 )
				{
					// each record is a json array so pack it into json array of arrays 
					DbDataReader reader = await dbcmd.ExecuteReaderAsync(_cancellationToken);
					streamWriter.Write('[');
					if (await reader.ReadAsync(_cancellationToken)) // at least one record already arrived
					{
						streamWriter.Write(reader[0]);
						rows++;
						while (await reader.ReadAsync(_cancellationToken))
						{
							streamWriter.Write(',');
							streamWriter.Write(reader[0]);
							rows++;
						}
					}

					_ = reader.CloseAsync();
					streamWriter.Write("]\n\r");
					streamWriter.Flush();
				}
				else if( jsonResultType==0 )
				{
					// normal sql result converting to json without fields' names
					DbDataReader reader = await dbcmd.ExecuteReaderAsync(_cancellationToken);
					var position = reader.FieldCount;
					
					#if DEBUG && CONSOLE && VERBOSE
						Stopwatch stopwatch = Stopwatch.StartNew();
					#endif
					//var dbnull = DBNull.Value;
					jsonWriter.WriteStartArray();
					while( await reader.ReadAsync(_cancellationToken) )
					{
						jsonWriter.WriteStartArray();
						for (var i = 0; i < position; i++)
						{
							switch (reader[i])
							{
								case string:
									jsonWriter.WriteStringValue(Encoding.UTF8.GetBytes(reader[i].ToString()!));
									break;

								case int:
									jsonWriter.WriteNumberValue(reader.GetInt32(i));
									break;

								case long:
									jsonWriter.WriteNumberValue(reader.GetInt64(i));
									break;

								case decimal:
									jsonWriter.WriteNumberValue(reader.GetDecimal(i));
									break;

								//case DateTimeOffset:
									//jsonWriter.WriteStringValue(reader.GetDateTime(i).ToString(Globals.TextDateFormat));
									//break;

								case DateTime or DateTimeOffset:
									jsonWriter.WriteStringValue(reader.GetDateTime(i).ToString(dtFormat));
									//CultureInfo.InvariantCulture
									break;

								case bool:
									jsonWriter.WriteBooleanValue(reader.GetBoolean(i));
									break;

								case float:
									jsonWriter.WriteNumberValue(reader.GetFloat(i));
									break;

								case double:
									jsonWriter.WriteNumberValue(reader.GetDouble(i));
									break;

								case byte:
									jsonWriter.WriteNumberValue(reader.GetByte(i));
									break;

								default:
									if( reader[i]==DBNull.Value ) jsonWriter.WriteNullValue();
									else jsonWriter.WriteStringValue(Encoding.UTF8.GetBytes(reader[i].ToString()!));
									break;
							}
						}

						rows++;
						jsonWriter.WriteEndArray();
					}

					_ = reader.CloseAsync();
					jsonWriter.WriteEndArray();
					jsonWriter.Flush();
					memoryStream.WriteByte(10);
					memoryStream.WriteByte(13);
					#if DEBUG && CONSOLE && VERBOSE
						stopwatch.Stop();
						Console.WriteLine($"JsonWriter took {stopwatch.ElapsedMilliseconds} ms. for {rows} rows.");
					#endif
				}
			}
			catch( Exception ex ) 
				//when (ex is InvalidOperationException or MySqlException or NpgsqlException or ArgumentException )
			{
				errorMessage = "Exception: " + ex.GetType() + " " + ex.Message;
				rows = 0;
			}
			finally
			{
				// clean up
				dbcmd?.Dispose();
				if( db!=null )
				{
					db.EndUse();
					if (newConnection)
					{
						db.Dispose();
						db.Close();
					}
				}
			}

			if( errorMessage.Length>0 )
			{
				_logger.LogError(Globals.LoggerTemplate, DateTimeOffset.Now, errorMessage );
			}
			return rows;
		}

		public void Exit()
		{
			_onExit++;
			_systemTimer!.Stop();
			_systemTimer.Dispose();
			
			var taskList = new List<Task>();
			
			if (_sqlTask!=null) taskList.Add(_sqlTask);
			if (_tcpListener!=null)
			{
				taskList.Add( Task.Run(() => _tcpListener.Exit(), _cancellationToken) );
				if( _listenerTask!=null ) taskList.Add( _listenerTask );
			}

			Task.WaitAll(taskList.ToArray(), _cancellationToken );
			_logger.LogInformation(Globals.LoggerTemplate, DateTimeOffset.Now, Globals.TextListenerClosed );
		}

		public override void DisplayStoredQueries( StringBuilder output )
		{
			if( _queryController.QueryList.Count==0 )
			{
				output.Append( Globals.TextNoQueries );
				return;
			}
			
			var separator = new string('-', 109);
			
			output.Append( $"\n\r{"Server",-20} {"DB Type",-20} {"Query ID",-18} {"Rows",6} {"Data size",10} {"Uploads",8} {"Updates",8} {"Max buffers",12}\n\r" );
			output.Append( separator );

			var totalDataSize = 0;
			foreach( QueryData query in _queryController.QueryList )
			{
				totalDataSize += query.DataSize;
				output.Append(
					$"\n\r{query.ConnectionServer,-20} {query.DbType,-20} {query.QueryId,-18} {query.Rows,6} {query.DataSize.ToString("0,0"),10} {query.Uploaded,8} {query.Updated,8} {query.MaxBufferIndex+1,12}" );
			}
			output.Append( Globals.TextLineEnd );
			output.Append( separator );
			output.Append( $"\n\r{totalDataSize.ToString("0,0"),78}" );
			output.Append( Globals.TextLineEnd );
		}

		public override void ReloadQueries( StringBuilder output )
		{
			// Starting updates by resetting the timer to 1ms.
			if( _systemTimer!=null ) _systemTimer.Interval = 1;
			output.Append( "Updates started..." );
			output.Append( Globals.TextLineEnd );
		}
		
		public override void DisplayTcpClients( StringBuilder output )
		{
			output.Append( Globals.NotImplementedError );
			output.Append( Globals.TextLineEnd );
		}
	}
}