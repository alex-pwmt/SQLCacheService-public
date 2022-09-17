#define CONSOLE

using System.Net;
using System.Net.Sockets;
using System.Text;

namespace SqlCacheService
{
	/// <summary>
	/// State object to present client data in asynchronous calls. 
	/// </summary>
	internal class TcpClient
	{
		public const int BufferSize = 4096;
		public readonly byte[] InBuffer = new byte[BufferSize];
		public byte[] OutBuffer = new byte[BufferSize];
		public int OutBufferSize;
		public int BytesSent;
		public readonly StringBuilder? OutDataSb;
		public readonly Socket? WorkSocket;
		public MemoryStream? OutMemoryStream;
		public QueryData? QdQueryData;
		//
		public string? QueryId;
		public int BufferIndex = -1;
		public int QueryIndex = -1;

		public bool OnLoadJson;
		public int Id { get; set; }
		public TcpClient(Socket socket)
		{
			WorkSocket = socket;
			Id = WorkSocket.GetHashCode();
			OutDataSb = new StringBuilder(BufferSize);
		}
	}

	/// <summary>
	/// 
	/// </summary>
	internal class ClientController
	{
		public readonly List<TcpClient> ClientsList = new List<TcpClient>();

		public void AddClient( TcpClient client )
		{
			lock( ClientsList )
			{
				ClientsList.Add( client );
			}
		}

		public void Clear()
		{
			lock( ClientsList )
			{
				ClientsList.Clear();
			}
		}

		public void RemoveClient( int id )
		{
			lock( ClientsList )
			{
				try
				{
					ClientsList.RemoveAt( ClientsList.FindIndex( tcpClient => tcpClient.Id==id ) );
				}
				catch( ArgumentOutOfRangeException ) { }
			}
		}

		public void RemoveClient(Socket s)
		{
			lock( ClientsList )
			{
				try
				{
					ClientsList.RemoveAt( ClientsList.FindIndex( tcpClient => tcpClient.WorkSocket==s ) );
				}
				catch( ArgumentOutOfRangeException ) { }
				#if DEBUG && CONSOLE
					catch( Exception ex )
					{
						Console.WriteLine( ex.Message );
					}
				#endif
			}
		}
	}

	/// <summary>
	/// Encapsulate methods for socket communication with the client and diagnostic.
	/// </summary>
	internal class TcpSocketListener
	{
		//private Socket _listener;
		//readonly IPEndPoint _localEndPoint;
		//private IPHostEntry _ipHostInfo;
		readonly int _port;
		int _onExit;
		readonly IPAddress _ipAddress;
		TcpListener? _tcpListener;
		readonly ClientController _clientController = new ClientController();
		readonly SqlNetCacheData _queryData;
		readonly CancellationToken _cancellationToken;
		readonly ILogger _logger;
		
		public TcpSocketListener( int port, SqlNetCacheData queryData, CancellationToken cancellationToken, ILogger logger )
		{
			_logger = logger;
			_cancellationToken = cancellationToken;
			_port = port;
			_onExit = 0;
			//_ipHostInfo = Dns.GetHostEntry(Dns.GetHostName());
			//_ipAddress = _ipHostInfo.AddressList[0];
			_ipAddress = IPAddress.Any;
			//_localEndPoint = new IPEndPoint(_ipAddress, _port);
			_queryData = queryData;
		}

		public async void StartListening()
		{
			try
			{
				// Start an asynchronous socket to listen for connections.  
				#if DEBUG && CONSOLE && VERBOSE
					Console.WriteLine($"\nWaiting for a connection on {_localEndPoint.Address} port {_port}.");
				#endif
				
				_tcpListener = new TcpListener(_ipAddress, _port);
				_tcpListener.Start();
				while ( true )
				{
					TcpClient? state = null;
					try
					{
						state = new TcpClient( await _tcpListener.AcceptSocketAsync( _cancellationToken ) );
					}
					catch( Exception ex ) when( ex is ObjectDisposedException or SocketException )
					{
						#if DEBUG && CONSOLE && VERBOSE
							Console.WriteLine( "StartListening " + Globals.ObjectDisposedException + ex.Message);
						#endif
						_onExit++;
					}

					if( _onExit>0 )
					{
						// close all connections?
						#if DEBUG && CONSOLE && VERBOSE
						 	Console.Write( Globals.TextListenerClosed );
						#endif
						break;
					}
					
					#if CONSOLE && VERBOSE
						Console.WriteLine($"\nAccept connection: {state.WorkSocket!.RemoteEndPoint}");
					#endif

					if( state!=null )
					{
						_clientController.AddClient( state );
						state.WorkSocket!.BeginReceive( state.InBuffer, 0, TcpClient.BufferSize, 0, ReadCallback, state );
					}
				}
			}
			catch( Exception e )
			{
				#if DEBUG && CONSOLE
					Console.WriteLine( Globals.Exception + e.GetType() + " type:" + e.Message );
				#endif
				_logger.LogCritical(Globals.LoggerTemplate, DateTimeOffset.Now, e.Message );
			}
		}

		public void Exit()
		{
			_onExit++;
			_tcpListener!.Stop();
			
			foreach( TcpClient client in _clientController.ClientsList )
			{
				if( client.WorkSocket!=null )
				{
					client.WorkSocket.Shutdown( SocketShutdown.Both );
					client.WorkSocket.Close();
				}
				if( client.BufferIndex>=0 && client.QueryId!=null )
				{
					_queryData.StopReadQueryDataById( client.BufferIndex, client.QueryIndex );
					client.BufferIndex = -1;
					client.QueryId = null;
				}
			}

			_clientController.Clear();
		}

		void ReadCallback( IAsyncResult ar )
		{
			if( ar.AsyncState==null )
			{
				_logger.LogCritical( Globals.LoggerTemplate, DateTimeOffset.Now, Globals.LostContentError );
				throw new ObjectDisposedException( Globals.LostContentError );
			}

			// Retrieve the state object and the handler socket from the asynchronous state object.  
			TcpClient state = (TcpClient)ar.AsyncState;
			
			// here it is possible two issue: state.WorkSocket and async operation was canceled
			var bytesRead = -1;
			if( state.WorkSocket!=null )
			{
				try
				{
					// Read data from the client socket.
					bytesRead = state.WorkSocket!.EndReceive(ar);
				}
				catch( Exception ex ) when( ex is ObjectDisposedException or SocketException )
				{
					#if DEBUG && CONSOLE
						Console.WriteLine( "ReadCallback " + Globals.ObjectDisposedException + ex.Message);
					#endif
					bytesRead = -2;
				}
			}

			if( bytesRead>0 )
			{
				var part = Encoding.UTF8.GetString(state.InBuffer, 0, bytesRead)
					.Replace("\r", "").Replace("\n", "").Replace("\t", "");
				
				state.OutDataSb!.Append( part );
				var request = state.OutDataSb.ToString().Trim();

				if( part.Length>0 )
				{
					if( state.OnLoadJson || part[0]=='{' )
					{
						state.OnLoadJson = true;
						//Console.WriteLine($"OnLoadJson={state.OnLoadJson} request[0]={request[0]} request[end]={request[^1]}");
						if (request.EndsWith('}'))
						{
							state.OnLoadJson = false;
							// parse the request
							var jsQuery = _queryData.ParseJsonRequest(request);
							if( jsQuery.ok )
							{
								string errorStr;
								state.QueryId = jsQuery.queryId;
								(state.BufferIndex, state.QueryIndex, MemoryStream? mStream) = 
									_queryData.StartReadQueryDataById( jsQuery.queryId!, jsQuery.connectionString! );
								
								#if DEBUG && CONSOLE && VERBOSE
									Console.WriteLine($"\ncmd:{cmd} queryId={queryId} jsonResult={jsonResult} BufferIndex={state.BufferIndex}");
								#endif

								// cmd==10 add new query to list
								if( state.BufferIndex==-1 && jsQuery.cmd==10 && jsQuery.sql!=null && jsQuery.dbType!=null && (jsQuery.jsonResult is >= 0 and <= 2) )
								{
									#if DEBUG && CONSOLE && VERBOSE
										Console.WriteLine($"\nADD: cmd:{jsQuery.cmd} queryId={jsQuery.queryId} BufferIndex={state.BufferIndex} jsonResult={jsQuery.jsonResult} DbType={jsQuery.dbType}");
									#endif
									mStream ??= new MemoryStream();
									
									state.QdQueryData = new QueryData( jsQuery.connectionString??"" )
									{
										JsonResultType = jsQuery.jsonResult,
										Timer = jsQuery.timer,
										QueryId = jsQuery.queryId,
										Sql = jsQuery.sql,
										DbType = jsQuery.dbType,
										Updated = 1,
										Uploaded = 1,
										CurrentBufferIndex = 0,
										MaxBufferIndex = 0
									};

									var taskFetch = _queryData.FetchSqlData( 
										jsQuery.connectionString, jsQuery.sql, jsQuery.jsonResult, mStream, jsQuery.dbType, jsQuery.dtFormat );

									taskFetch.Wait( 5000, _cancellationToken );
									var rows = taskFetch.Result;

									if( taskFetch.IsCompletedSuccessfully && rows>0 )
									{
										#if DEBUG && CONSOLE && VERBOSE
											Console.WriteLine( $"FetchSqlData Completed Successfully rows={rows}" );
										#endif

										// -2 means buffer is not assigned to List
										state.BufferIndex = -2;
										state.QdQueryData.Rows = rows;
										state.QdQueryData.DataSize = (int)mStream.Length;
										state.QdQueryData.BufferList[0].OutMemoryStream = mStream;
										state.QdQueryData.BufferList[0].Rows = rows;
										state.QdQueryData.BufferList[0].OutBufferSize = (int)mStream.Length;
										state.QdQueryData.BufferList[0].Readers = 0;
										state.OutMemoryStream = mStream;
									}
									else
									{
										state.BufferIndex = -1;
										state.OutDataSb.Clear();
										if( taskFetch.Exception!=null )
										{
											errorStr = taskFetch.Exception.Message + " QueryId: "+ jsQuery.queryId + " DBType: "+ jsQuery.dbType;
											state.OutDataSb.Append( errorStr+Globals.TextLineEnd );
											_logger.LogError( Globals.LoggerTemplate, DateTimeOffset.Now, errorStr );
										}
										else
										{
											errorStr = Globals.FetchingDataError + " QueryId: "+ jsQuery.queryId + " DBType: "+ jsQuery.dbType;
											state.OutDataSb.Append( errorStr+Globals.TextLineEnd );
											_logger.LogError( Globals.LoggerTemplate, DateTimeOffset.Now, errorStr );
										}
									}
								}
								if( state.BufferIndex!=-1 && mStream!=null )
								{
									// does not matter cmd
									state.OutMemoryStream = mStream;
									state.OutDataSb!.Clear();
									state.BytesSent = 0;
									state.OutBufferSize = (int)mStream.Length;
									state.WorkSocket!.BeginSend( mStream.GetBuffer(), state.BytesSent, state.OutBufferSize, 0, SendCallback, state);
									return;
								}
								
								state.BufferIndex = -1;
								state.OutDataSb.Clear();
								errorStr = $"{Globals.UnrecognizedError}: QueryId={jsQuery.queryId} CMD:{jsQuery.cmd} JsonResult={jsQuery.jsonResult} DBType={jsQuery.dbType}";
								state.OutDataSb.Append( errorStr+Globals.TextLineEnd );
								_logger.LogError( Globals.LoggerTemplate, DateTimeOffset.Now, errorStr );
							}
							else
							{
								#if DEBUG && CONSOLE && VERBOSE
									Console.WriteLine( ConnectionString );
								#endif
								state.OutDataSb.Clear();
								state.OutDataSb.Append( jsQuery.connectionString );		// here is error message
							}
						}
						else
						{
							// Not all data received after {. Get more.
							state.WorkSocket?.BeginReceive(state.InBuffer, 0, TcpClient.BufferSize, 0, ReadCallback, state);
							return;
						}
					}
					else
					{
						state.OutDataSb.Clear();
						state.OutDataSb.Append( Globals.QueryNotFoundError + Globals.TextLineEnd );
					}
					
					// send respond from OutDataSb
					state.OutBuffer = Encoding.UTF8.GetBytes( state.OutDataSb.ToString() );
					state.OutBufferSize = state.OutBuffer.Length;
					state.OutDataSb.Clear();

					if( state.OutBufferSize>0 )
					{
						state.QueryId = "";
						state.BufferIndex = -1;
						state.BytesSent = 0;
						state.WorkSocket!.BeginSend( state.OutBuffer, state.BytesSent, state.OutBufferSize, 0, SendCallback, state );
					}
					else
					{
						// inform about error and wait for other request
						#if DEBUG && CONSOLE
							Console.WriteLine( $"OutBuffer=NULL {state.QueryId ?? "<empty>"} QueryIndex={state.QueryIndex,-15} OutBufferSize={state.OutBufferSize}");
						#endif
						state.WorkSocket?.Shutdown( SocketShutdown.Both );
						state.WorkSocket?.Close();
						_clientController.RemoveClient( state.Id );
					}
				}
				else
				{
					// Not all data received. Get more.  
					state.WorkSocket?.BeginReceive( state.InBuffer, 0, TcpClient.BufferSize, 0, ReadCallback, state );
				}
				return;
			}
			
			if( bytesRead!=-2 )
			{
				state.WorkSocket!.Shutdown( SocketShutdown.Both );
				state.WorkSocket.Close();
			}
			#if DEBUG && VERBOSE && CONSOLE
				Console.WriteLine( Globals.TextSocketClosed );
			#endif
			_clientController.RemoveClient( state.Id );
		}

		void SendCallback( IAsyncResult ar )
		{
			if( ar.AsyncState==null )
			{
				#if DEBUG && CONSOLE
					Console.WriteLine( Globals.LostContentError );
				#endif
				_logger.LogCritical(Globals.LoggerTemplate, DateTimeOffset.Now, Globals.LostContentError );
				throw new ObjectDisposedException( Globals.LostContentError );
			}

			TcpClient state = (TcpClient)ar.AsyncState!;
			int bytesSent = -2;
			
			if( state.WorkSocket!=null )
			{
				try
				{
					bytesSent = state.WorkSocket!.EndSend( ar );
				}
				catch(Exception ex) when( ex is SocketException or ObjectDisposedException )
				{
					#if DEBUG && CONSOLE
						Console.WriteLine( "\nSendCallback " + Globals.SocketExceptionError + ex.Message );
					#endif
					bytesSent = -1;
				}
			}

			if( bytesSent>=0 )
			{
				state.BytesSent += bytesSent;
				if( state.WorkSocket!.Connected )
				{
					// still connected
					if (bytesSent < state.OutBufferSize)
					{
						// not all data were sent
						state.OutBufferSize -= bytesSent;
						state.WorkSocket!.BeginSend( state.OutBuffer, state.BytesSent, state.OutBufferSize, 0, SendCallback, state);
					}
					else
					{
						// continue or close
						state.OutDataSb!.Clear();
						if( state.BufferIndex>=0 )
						{
							_queryData.StopReadQueryDataById( state.BufferIndex, state.QueryIndex );
							state.BufferIndex = -1;
							#if DEBUG && VERBOSE && CONSOLE
								Console.WriteLine($"Uploaded {state.BytesSent} bytes of " + state.QueryId);
							#endif
							state.QueryId = null;
						}
						else if( state.BufferIndex==-2 )
						{
							var ret = _queryData.AddNewQuery( state.QdQueryData );
							string message;
							if( ret==0 )
							{
								message = Globals.TextQueryAdded + $" {state.QueryId}.";
								_logger.LogInformation( Globals.LoggerTemplate, DateTimeOffset.Now, message );
							}
							else
							{
								message = Globals.FailedAddQueryError + $" {state.QueryId}.";
								_logger.LogInformation( Globals.LoggerTemplate, DateTimeOffset.Now, message );
							}
							state.BufferIndex = -1;
							state.QueryId = null;
						}

						state.OnLoadJson = false;
						state.WorkSocket.BeginReceive( state.InBuffer, 0, TcpClient.BufferSize, 0, ReadCallback, state );
					}
					return;
				}
			}
			
			if( bytesSent!=-2 )
			{
				state.WorkSocket!.Shutdown( SocketShutdown.Both );
				state.WorkSocket.Close();
			}
			_clientController.RemoveClient( state.Id );
			#if DEBUG && CONSOLE && VERBOSE 
				Console.WriteLine( Globals.TextSocketClosed );
			#endif
		}
	}
}