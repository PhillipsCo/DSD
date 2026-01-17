
using DSD.Common.Services;
using DSD.Scheduler.Runners;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Extensions.Hosting;
using Serilog.Settings.Configuration;
using System.IO;

public class Program
{
    public static async Task Main()
    { 

        // Load configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        //// Sanitize customer code for filename
        //var safeCustomerCode = string.Concat(customerCode.Split(Path.GetInvalidFileNameChars()));

        // Build dynamic log file path
        var logPath = configuration["logPath"] ?? "C:\\Logs\\";
        Directory.CreateDirectory(logPath);
        var dynamicLogFile = Path.Combine(logPath, $"Scheduler-log-.txt");

        // Override Serilog file path dynamically
        configuration["Serilog:WriteTo:1:Args:path"] = dynamicLogFile;

        // Configure Serilog
        Log.Logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger();

        try
        {
            Log.Information("Application starting...");

            var host = Host.CreateDefaultBuilder()
                .ConfigureAppConfiguration((context, config) =>
                {
                    config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                })
                .UseSerilog()
                .ConfigureServices((context, services) =>
                {
                     
                    services.AddSingleton<IConfiguration>(configuration);
                     
                     
                    services.AddTransient<SqlService>();
                    services.AddTransient<SchedulerAppRunner>();

                })
                .Build();


            using var scope = host.Services.CreateScope();
            var appRunner = scope.ServiceProvider.GetRequiredService<SchedulerAppRunner>();
            await appRunner.RunAsync();


            Log.Information("Jobs Scheduled for the next 3 days.");
        }
        catch (Exception ex)
        {
            Log.Fatal(ex, "Application terminated unexpectedly");
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }
}


