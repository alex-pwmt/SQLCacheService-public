//#define CONSOLE

using System.Text;

#if CONSOLE
using Microsoft.Extensions.Hosting.Systemd;
#endif

#if DEBUG
// 	#define CONSOLE
// 	#define VERBOSE
#endif

namespace SqlCacheService
{
	public class Worker : BackgroundService
	{
		private readonly ILogger<Worker> _logger;
		private readonly IConfiguration _configuration;
		private readonly IHostApplicationLifetime _appLifetime;
		SqlNetCache? _sqlNetCache;
		Task? _serviceTask;
		
		#if CONSOLE
			Task? _consoleTask;
		#endif			

		public Worker( IConfiguration configuration, ILogger<Worker> logger, IHostApplicationLifetime appLifetime )
		{
			_logger = logger;
			_appLifetime = appLifetime;
			_configuration = configuration;
		}

		protected override Task ExecuteAsync( CancellationToken stoppingToken )
		{
			_sqlNetCache = new SqlNetCache( stoppingToken, _logger, _configuration );
			_serviceTask = _sqlNetCache.StartService();

			if( _serviceTask.Status is TaskStatus.Faulted or TaskStatus.Canceled or TaskStatus.Created ) return _serviceTask;

			#if CONSOLE
				if( !SystemdHelpers.IsSystemdService() )
				{
					_consoleTask = Task.Run(() => ConsoleLoop(stoppingToken), stoppingToken);
				}
			#endif
			
			return _serviceTask;
		}
		
		public override Task StopAsync( CancellationToken cancellationToken )
		{
			#if CONSOLE
				_consoleTask?.WaitAsync( cancellationToken);
			#endif
			_sqlNetCache?.Exit();
			_serviceTask?.Wait( cancellationToken );
			return base.StopAsync( cancellationToken );
		}

#if CONSOLE
		private Task ConsoleLoop( CancellationToken stoppingToken )
		{
			try
			{
				StringBuilder output = new StringBuilder();
				Console.Write("\nPress E - exit console, Q - saved queries, C - client list, R - force to reload, Ctrl-C to exit service.");
				var exit = false;

				while( !exit && !stoppingToken.IsCancellationRequested )
				{
					ConsoleKeyInfo key = Console.ReadKey(true);
					switch (key.Key)
					{
						case ConsoleKey.Enter or ConsoleKey.Q:
							// display the list of stored queries
							_sqlNetCache?.DisplayStoredQueries( output );
							break;

						case ConsoleKey.C:
							// display the list of tcp clients
							_sqlNetCache?.DisplayTcpClients( output );
							break;

						case ConsoleKey.R:
							// push to reload all queries
							_sqlNetCache?.ReloadQueries( output );
							break;

						case ConsoleKey.E:
							exit = true;
							break;
					}
					Console.WriteLine( output.ToString() );
					output.Clear();
				}

				Console.WriteLine("Console finished here!");
			}
			catch (InvalidOperationException)
			{
				// Probably console is absent
			}
			catch (Exception ex)
			{
				_logger.LogError( Globals.LoggerTemplate, DateTimeOffset.Now, ex.Message );
			}
			return Task.CompletedTask;
		}
#endif
	}
}