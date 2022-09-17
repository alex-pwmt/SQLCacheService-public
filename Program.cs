namespace SqlCacheService
{
	public class Program
	{
		public static void Main(string[] args)
		{
			IHost host = Host.CreateDefaultBuilder(args)
				.ConfigureHostConfiguration(configHost =>
				{
					configHost.SetBasePath(Directory.GetCurrentDirectory());
					//configHost.AddEnvironmentVariables();
					configHost.AddCommandLine(args);
					//configHost.AddJsonFile("configfile.json", optional: true);
				})
				.ConfigureServices((hostContext, services) =>
				{
					services.AddSingleton(hostContext.Configuration);
					services.AddHostedService<Worker>();
				})
				.Build();
			host.RunAsync().Wait();
		}
	}
}