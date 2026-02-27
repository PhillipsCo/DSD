using Microsoft.Extensions.Hosting;
using Serilog;

namespace DSD.Common.Logging;

public static class SerilogHostBuilderExtensions
{
    public static IHostBuilder UseDsdSerilog(
        this IHostBuilder hostBuilder,
        string applicationName)
    {
        return hostBuilder.UseSerilog((context, services, loggerConfig) =>
        {
            var logPath = context.Configuration["logPath"] ?? @"C:\Logs";
            Directory.CreateDirectory(logPath);

            var logFile = Path.Combine(logPath, $"{applicationName}-log-.txt");

            loggerConfig
                .ReadFrom.Configuration(context.Configuration)
                .Enrich.FromLogContext()
                .Enrich.WithProperty("Application", applicationName)
                .WriteTo.Console()
             .WriteTo.File(
                    path: logFile,
                    rollingInterval: RollingInterval.Day,
                    retainedFileCountLimit: 30,        // ✅ centralized
                    fileSizeLimitBytes: 10_485_760,    // ✅ centralized
                    rollOnFileSizeLimit: true,
                    outputTemplate:
                        "[{Timestamp:yyyy-MM-dd HH:mm:ss} {Level:u3}] {Message:lj}{NewLine}{Exception}"
                );
             
        });
    }
}