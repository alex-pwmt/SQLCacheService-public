using MySqlConnector;
using Npgsql;
using System.Data.Common;
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
		public QueryData()
		{
			BufferList = new SqlResultBuffer[_buffersNumber];
		}
		//
		public string? QueryId;
		public string? Sql;
		public string? DbType;
		public string? DtFormat;
		public string? ConnectionString;
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
		readonly int _buffersNumber = 4;
		public readonly SqlResultBuffer[] BufferList;
		public int CurrentBufferIndex = -1;

		public int GetFreeSqlBuffer()
		{
			for (int i=0; i<_buffersNumber; i++ )
			{
				if( i!=CurrentBufferIndex && BufferList[i].Readers==0 )
				{
					BufferList[i].Rows = 0;
					BufferList[i].OutBufferSize = 0;
					if (BufferList[i].OutMemoryStream==null)
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
			throw new Exception("All buffer for SQL queries are looked!");
		}

		/// <summary>
		/// Free old buffers that are out of date.
		/// </summary>
		public int RemoveSqlBuffer()
		{
			int k = 0;
			for( var i=0; i<_buffersNumber; i++ )
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

	internal class QueryController
	{
		public readonly List<QueryData> QueryList = new List<QueryData>();
		public void AddQueryData(QueryData cd)
		{
			lock( QueryList )
			{
				if (cd.QueryId!=null && FindQueryData(cd.QueryId)==-1)
				{
					QueryList.Add(cd);
				}
			}
		}
		public void RemoveQueryData(string queryId)
		{
			lock( QueryList )
			{
				QueryList.RemoveAt(QueryList.FindIndex(listItem => listItem.QueryId==queryId));
			}
		}
		public int FindQueryData(string queryId)
		{
			int result;
			lock (QueryList)
			{
				result = QueryList.FindIndex(listItem => listItem.QueryId==queryId);
			}
			return result;
		}
	}

	internal class Dbw : IDisposable
	{
		readonly Func<DbConnection> _dbwConnection;
		readonly string _connectionString;
		//readonly string _provider;
		long _useCount;
		DbConnection? _connectionObject;
		readonly CancellationToken _cancellationToken;

		public Dbw( string dbType, string connectionString, CancellationToken cancellationToken )
		{
			_cancellationToken = cancellationToken;
			//_provider = dbType;
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
			get
			{
				if (BeginUse()==0)
				{
					if( _connectionObject is null )
					{
						_connectionObject = _dbwConnection();
						_connectionObject.OpenAsync().Wait(_cancellationToken);
					}
					return _connectionObject;
				}
				Console.WriteLine("A very rare event! Create a new Dbw for a once use with state: " + _connectionObject?.State);
				return null;
			}
		}


		/// <summary>
		/// Check if this DbConnection object is currently in use. If somebody uses it to make a clone.
		/// </summary>
		/// <returns>0 if DbConnection was locked, otherwise 1.</returns>
		long BeginUse() => Interlocked.CompareExchange(ref _useCount, 1, 0);

		/// <summary>
		/// Free DbConnection object. Set internal _useCount to 0.
		/// </summary>
		/// <returns></returns>
		public long EndUse() => Interlocked.Exchange(ref _useCount, 0);
		
		public void Close()
		{
			// nothing to wait here
			_connectionObject?.CloseAsync();
			Interlocked.Exchange(ref _useCount, 0);
		}

		// MySQL
		DbConnection DbwMySqlConnection() => new MySqlConnection(_connectionString);
		
		// PostgreSQL
		DbConnection DbwNpgConnection() => new NpgsqlConnection(_connectionString);

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
 
	
	public abstract class SqlNetCacheData
	{
		public abstract (int, int, MemoryStream?) StartReadQueryDataById( string id, string connection );
		public abstract (int, int, MemoryStream?) StartReadQueryDataById( string id );
		public abstract bool StopReadQueryDataById( string id, int bufferIndex, int queryIndex );
		public abstract Task<int> FetchSqlData(string? connectionString, string? sql, int jsonResultType, 
			MemoryStream memoryStream, string dbType );
		public abstract int AddNewQuery( QueryData? cd );

		// ok, cmd, jsonResult, queryId, connectionString, dbType, sql, dtFormat
		public abstract (bool ok, int cmd, int jsonResult, int timer, string? queryId, string? connectionString, 
						string? dbType, string? sql, string? dtFormat) ParseJsonRequest(string request);
	}

	/// <summary>
	/// Cached big result queries which changes very seldom (like a config data) as json an array of arrays
	/// and spread for sockets client. Cached queries updates by timer. Queries can be preloaded from a config
	/// file or added dynamically by client. Client select query from cache by string query id.
	/// This is only to use for inner infrastructure and suitable for caching remote data and local data. 
	/// </summary>
	public class SqlNetCache : SqlNetCacheData
	{
		private readonly IConfiguration _configuration;
		int _onExit;
		readonly int _listenPort = 38888;
		//readonly int _connectionLimit = 1;
		readonly int _defaultTimer = 300;
		int _queriesNumber;
		readonly bool _configured;
		readonly QueryController _queryController = new QueryController();
		//readonly List<SqlConnection> _sqlConnectionList = new List<SqlConnection>();
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
		public override (int, int, MemoryStream?) StartReadQueryDataById( string id, string connection )
		{
			int buffer;
			int index = _queryController.FindQueryData(id);
			
			if (index==-1) return ( -1, -1, null );

			lock( _queryController.QueryList[index].LockQuery )
			{
				// Here it is needed to get CurrentBufferIndex and increase Readers
				buffer = _queryController.QueryList[index].CurrentBufferIndex;

				if ( buffer==-1 || _queryController.QueryList[index].ConnectionString!=connection )
				{
					return (-1, -1, null);
				}
				// Return is OK inside of lock
				_queryController.QueryList[index].BufferList[buffer].Readers++;
			}

			return ( buffer, index, _queryController.QueryList[index].BufferList[buffer].OutMemoryStream );
		}

		public override (int, int, MemoryStream?) StartReadQueryDataById( string id )
		{
			int buffer;
			int index = _queryController.FindQueryData(id);
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
		
		public override bool StopReadQueryDataById( string id, int bufferIndex, int queryIndex )
		{
			// doest need to use lock here 
			Interlocked.Add(ref _queryController.QueryList[queryIndex].BufferList[bufferIndex].Readers,-1);
			Interlocked.Add(ref _queryController.QueryList[queryIndex].Uploaded,1);
			return true;
		}

		static (bool ok, int cmd, int jsonResult, int timer, string? queryId, string? connectionString, string? dbType, string? sql, string? dtFormat) 
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
					|| (cmd==10 && (dbType==null || sql==null || jsonResult<0 || jsonResult>2 )) )
				{
					connectionString = Globals.ParseJsonError;
				}
				else
				{
					ok = true;
				}
			}
			catch (FormatException ex)
			{
				Console.WriteLine( Globals.Exception + ex.Message);
				connectionString = ex.Message;
			}

			return ( ok, cmd, jsonResult, timer, queryId, connectionString, dbType, sql, dtFormat );
		}
		public override (bool,int,int,int,string?,string?,string?,string?,string?) ParseJsonRequest( string request )
		{
			string errorMessage;

			try
			{
				var jsonConfig = JsonDocument.Parse(request, 
					new JsonDocumentOptions{ CommentHandling = JsonCommentHandling.Skip });
				var queryConfigDic = jsonConfig.RootElement.Deserialize<Dictionary<string, object>>();
				return UnpackJsonDicTuple( queryConfigDic );
			}
			catch (JsonException ex)
			{
				Console.WriteLine( Globals.JsonException + ex.Message);
				errorMessage = ex.Message;
			}
			catch (ArgumentException ex)
			{
				Console.WriteLine( Globals.FormatException + ex.Message);
				errorMessage = ex.Message;
			}
			catch (NotSupportedException ex)
			{
				Console.WriteLine( Globals.Exception + ex.Message);
				errorMessage = ex.Message;
			}

			if( errorMessage.Length>0 )
			{
				_logger.LogError(Globals.LoggerTemplate, DateTimeOffset.Now, errorMessage);
			}
			
			return ( false, -1, 0, 1, null, errorMessage, null, null, null );
		}

		public SqlNetCache( CancellationToken cancellationToken, ILogger logger, IConfiguration configuration )
		{
			_cancellationToken = cancellationToken;
			_logger = logger;
			_configuration = configuration;

			var fileName = _configuration["config"];
			try
			{
				if( fileName==null )
				{
					// Initialize config from json file
					// fileName = @"/mnt/e/Users/Alex/source/TestData/configfileLinux.json";
					fileName = RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? @"configfileLinux.json" : @"configfile.json";
				}
				
				var json = File.ReadAllText(fileName);
				var jsonConfig = JsonDocument.Parse( json, new JsonDocumentOptions{ CommentHandling = JsonCommentHandling.Skip });

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
								_queryController.AddQueryData(new QueryData
								{
									JsonResultType = queryJson.jsonResult,
									ConnectionIndex = k - 1,
									Timer = queryJson.timer,
									QueryId = queryJson.queryId,
									Sql = queryJson.sql,
									DbType = queryJson.dbType,
									DtFormat = queryJson.dtFormat,
									ConnectionString = queryJson.connectionString,
								});
							}
							else
							{
								_logger.LogError(Globals.LoggerTemplate, DateTimeOffset.Now, queryJson.connectionString );
							}
						}

						_configured = true;
					}
				}
			}
			catch ( JsonException ex )
			{
				//Console.WriteLine("Error: " + ex.Message);
				_logger.LogError(Globals.LoggerTemplate, DateTimeOffset.Now, ex.Message );
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
			_systemTimer.AutoReset = true;
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
			_systemTimer!.Stop();

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
				_systemTimer.Interval = TimeSpan.FromSeconds(_defaultTimer).TotalMilliseconds; //_defaultTimer
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
					query.Sql, query.JsonResultType, query.BufferList[nextBuffer].OutMemoryStream!, query.DbType! );

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
			
			#if DEBUG && !VERBOSE
				DisplayStoredQueries();
			#endif
		}

		public override async Task<int> FetchSqlData( string? connectionString, string? sql, int jsonResultType,
			MemoryStream memoryStream, string dbType )
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

			try
			{
				if (!_connectionPool.ContainsKey(connectionString))
				{
					db = new Dbw(dbType, connectionString, _cancellationToken);
					_connectionPool.Add(connectionString, db);
				}
				else
				{
					db = _connectionPool[connectionString];
				}

				// null is possible here if this object is locked
				dbcon = db.ConnectionObject;

				if (dbcon is null)
				{
					// Creat a new Dbw for a once use
					db = new Dbw(dbType, connectionString, _cancellationToken);
					dbcon = db.ConnectionObject;
					newConnection = true;
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
				else if (jsonResultType==1)
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
				else if (jsonResultType==0)
				{
					// normal sql result converting to json without fields' names
					DbDataReader reader = await dbcmd.ExecuteReaderAsync(_cancellationToken);
					var position = reader.FieldCount;
					
					#if DEBUG && CONSOLE && VERBOSE
						Stopwatch stopwatch = Stopwatch.StartNew();
					#endif
					//var dbnull = DBNull.Value;
					jsonWriter.WriteStartArray();
					while (await reader.ReadAsync(_cancellationToken))
					{
						jsonWriter.WriteStartArray();
						for (int i = 0; i < position; i++)
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
									jsonWriter.WriteStringValue(reader.GetDateTime(i).ToString(Globals.TextDateFormat));
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
			catch( InvalidOperationException ex )
			{
				errorMessage = "Error: " + ex.Message;
				rows = 0;
			}
			catch( DbException ex )
			{
				errorMessage = "Error: " + ex.Message;
				rows = 0;
			}
			finally
			{
				// clean up
				if (dbcmd!=null) dbcmd.Dispose();
				if (db!=null)
				{
					db.EndUse();
					if (newConnection)
					{
						db.Dispose();
						db.Close();
					}
				}
			}

			if( rows==0 && errorMessage.Length>0 )
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
				taskList.Add(Task.Run(() => _tcpListener.Exit(), _cancellationToken));
				if (_listenerTask!=null) taskList.Add(_listenerTask);
			}

			Task.WaitAll(taskList.ToArray(),_cancellationToken );

			_logger.LogInformation(Globals.LoggerTemplate, DateTimeOffset.Now, Globals.TextListenerClosed );
			//Console.Write("\nSqlCache finished. Goodbye!\n");
		}

		/// <summary>
		/// Display the list of stored queries.
		/// </summary>
		public void DisplayStoredQueries()
		{
			string separator = new String('-', 116);
			Console.WriteLine($"\n\n{"Server",-20}{"DB Type",-20}{"Query ID",-20}{"Rows",-8}{"Data size",-14}{"Uploads",-10}{"Updates",-10}{"Max buffers",-14}" );
			Console.WriteLine(separator);
			foreach (QueryData query in _queryController.QueryList)
			{
				var conSplit = (query.ConnectionString??"").Split(';');
				var host="";
				foreach (var sc in conSplit)
				{
					if (sc.IndexOf("server", StringComparison.OrdinalIgnoreCase)!=-1)
					{
						host = sc.Split('=')[1];
					}
					break;
				}
				Console.WriteLine(
					$"{host,-20}{query.DbType,-20}{query.QueryId,-20}{query.Rows,-8}{query.DataSize,-14}{query.Uploaded,-10}{query.Updated,-10}{query.MaxBufferIndex+1,-14}" );
			}
		}

		/// <summary>
		/// Push to reload all queries.
		/// </summary>
		public void ReloadQueries()
		{
			Console.WriteLine(Globals.NotImplementedError);
		}
		
		/// <summary>
		/// Push to reload all queries.
		/// </summary>
		public void DisplayTcpClients()
		{
			Console.WriteLine(Globals.NotImplementedError);
		}
	}
}