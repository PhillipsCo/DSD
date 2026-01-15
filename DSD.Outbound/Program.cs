
using DSD.Common.Services;
using DSD.Outbound.Runners;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using System.IO;

public class Program
{
    public static async Task Main(string[] args)
    {
        var customerCode = args.Length > 0 ? args[0] : "DEMO";

        // Load configuration
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        // Sanitize customer code for filename
        var safeCustomerCode = string.Concat(customerCode.Split(Path.GetInvalidFileNameChars()));

        // Build dynamic log file path
        var logPath = configuration["logPath"] ?? "C:\\Logs\\";
        Directory.CreateDirectory(logPath);
        var dynamicLogFile = Path.Combine(logPath, $"outbound-{safeCustomerCode}-log-.txt");

        // Override Serilog file path dynamically
        configuration["Serilog:WriteTo:1:Args:path"] = dynamicLogFile;

        // Configure Serilog
        Log.Logger = new LoggerConfiguration()
            .ReadFrom.Configuration(configuration)
            .CreateLogger();

        try
        {
            Log.Information("Application starting for customer {CustomerCode}", customerCode);

            var host = Host.CreateDefaultBuilder(args)
                .ConfigureAppConfiguration((context, config) =>
                {
                    config.AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);
                })
                .UseSerilog()
                .ConfigureServices((context, services) =>
                {
                    services.AddHttpClient("ApiClient");
                    services.AddSingleton<IConfiguration>(configuration);

                    // Register custom services
                    //services.AddTransient<ApiService>();
                    services.AddTransient<EmailService>();
                    services.AddTransient<FtpService>();
                    services.AddTransient<SqlService>();
                    services.AddTransient<ApiExecutorService>();
                    services.AddTransient<CsvExportService>();
                    services.AddTransient<OutboundAppRunner>();
                })
                .Build();


            using var scope = host.Services.CreateScope();
            var appRunner = scope.ServiceProvider.GetRequiredService<OutboundAppRunner>();
            await appRunner.RunAsync(args);


            Log.Information("DSD Outbound application completed successfully.");
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

