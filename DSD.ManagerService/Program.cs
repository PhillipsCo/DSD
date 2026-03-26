using DSD.ManagerService;


using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;

class Program
{
    static void Main(string[] args)
    {
        var config = new ConfigurationBuilder()
            .SetBasePath(AppContext.BaseDirectory)
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        // Build log file path from rootFilepath
        string rootFilePath = config.GetConnectionString("rootFilepath");
        string logFilePath = System.IO.Path.Combine(rootFilePath, "Managerlog-.txt");

        // 👇 Write the path to the console
        //Console.WriteLine($"Serilog will write logs to: {logFilePath}");

        Log.Logger = new LoggerConfiguration()
        .ReadFrom.Configuration(config)
        .WriteTo.File(logFilePath, rollingInterval: RollingInterval.Day)
        .CreateLogger();
        // Configure Serilog
        //Log.Logger = new LoggerConfiguration()
        //    .ReadFrom.Configuration(config)
        //    .CreateLogger();

        try
        {
            Log.Information("Starting up the service...");
            CreateHostBuilder(args).Build().Run();
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Application start-up failed");
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }

    static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .UseSerilog()
            .ConfigureServices((hostContext, services) =>
            {
                services.AddHostedService<Worker>();
            })
            .UseWindowsService();
}


