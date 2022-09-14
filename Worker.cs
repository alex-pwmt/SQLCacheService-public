#define CONSOLE

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
			//_logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
			_sqlNetCache = new SqlNetCache( stoppingToken, _logger, _configuration );
			_serviceTask = _sqlNetCache.StartService();

			if( _serviceTask.Status is TaskStatus.Faulted or TaskStatus.Canceled or TaskStatus.Created) return _serviceTask;
			_logger.LogInformation("{Time} SqlCacheService running...", DateTimeOffset.Now);
			
			#if CONSOLE
				_consoleTask = Task.Run( () => ConsoleLoop(stoppingToken), stoppingToken );
			#endif
			
			return _serviceTask;
		}
		
		public override Task StopAsync(CancellationToken cancellationToken)
		{
			#if CONSOLE
				_consoleTask?.WaitAsync(cancellationToken);
			#endif
			_sqlNetCache?.Exit();
			_serviceTask?.Wait(cancellationToken);
			return base.StopAsync(cancellationToken);
		}

#if CONSOLE
		private Task ConsoleLoop( CancellationToken stoppingToken )
		{
			Console.Write("\nPress E - exit console, Q - saved queries, C - client list, R - force to reload, Ctrl-C to exit service.");

			bool exit = false;
			ConsoleKeyInfo key;

			while( !exit && !stoppingToken.IsCancellationRequested )
			{
				try
				{
					key = Console.ReadKey(true);
					switch (key.Key)
					{
						case ConsoleKey.E:
							exit = true;
							break;
						case ConsoleKey.Enter or ConsoleKey.Q:
							// display the list of stored queries
							_sqlNetCache?.DisplayStoredQueries();
							break;
						case ConsoleKey.C:
							// display the list of tcp clients
							_sqlNetCache?.DisplayTcpClients();
							break;
						case ConsoleKey.R:
							// push to reload all queries
							_sqlNetCache?.ReloadQueries();
							break;
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
				}
			}
			Console.WriteLine("Console finished here!");
			return Task.CompletedTask;
		}
#endif
	}
}