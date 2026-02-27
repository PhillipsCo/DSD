using DSD.Common.Logging;          // <-- your shared extension lives here
using DSD.Common.Services;
using DSD.Eod.Runners;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;

public class Program
{
    public static async Task Main(string[] args)
    {
        try
        {
            var customerCode = args.Length > 0 ? args[0] : "DEMO";
            var host = Host.CreateDefaultBuilder(args)
                // Shared Serilog setup from DSD.Common (reads DSD.Eod appsettings.json via context.Configuration)
                .UseDsdSerilog("Eod")
                .ConfigureServices((context, services) =>
                {
                    // App-specific registrations
                    services.AddTransient<EmailService>();
                    services.AddTransient<SqlService>();
                    services.AddTransient<EodAppRunner>();
                })
                .Build();
         
            using var scope = host.Services.CreateScope();
            var runner = scope.ServiceProvider.GetRequiredService<EodAppRunner>();
            await runner.RunAsync(args);
        }
        catch (Exception ex)
        {
            // Captures startup + runtime fatal errors
            Log.Fatal(ex, "DSD.Eod terminated unexpectedly");
        }
        finally
        {
            Log.CloseAndFlush();
        }
    }
}