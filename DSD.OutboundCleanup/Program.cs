using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace CisOutboundCleanup
{
    class Program
    {
        static void Main(string[] args)
        {
            // Load configuration
            IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(AppContext.BaseDirectory)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .Build();

            // Configure Serilog
            Log.Logger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .CreateLogger();

            //var logPath = Path.GetFullPath(
            //    Path.Combine(AppContext.BaseDirectory, "Logs", $"cleanup-{DateTime.Now:yyyyMMdd}.log")
            //);

            //Console.WriteLine($"Serilog log file path: {logPath}");


            try
            {
                Log.Information("CIS OutBound cleanup started");

                string rootPath = configuration["CleanupSettings:RootPath"];
                int daysOld = int.Parse(configuration["CleanupSettings:DaysOld"]);

                if (!Directory.Exists(rootPath))
                {
                    Log.Error("Root path does not exist: {RootPath}", rootPath);
                    return;
                }

                DateTime cutoffDate = DateTime.Now.AddDays(-daysOld);

                Log.Information(
                    "Scanning root path {RootPath} for OutBound folders older than {DaysOld} days",
                    rootPath,
                    daysOld);

                var outboundDirs = Directory.GetDirectories(
                    rootPath,
                    "OutBound",
                    SearchOption.AllDirectories
                );

                foreach (var outboundDir in outboundDirs)
                {
                    Log.Information("Scanning OutBound directory: {OutBoundDir}", outboundDir);

                    foreach (var folder in Directory.GetDirectories(outboundDir))
                    {
                        try
                        {
                            DateTime lastWrite = Directory.GetLastWriteTime(folder);

                            if (lastWrite < cutoffDate)
                            {
                                Log.Warning(
                                    "Deleting folder {Folder} (LastWrite: {LastWrite})",
                                    folder,
                                    lastWrite);

                                Directory.Delete(folder, true);
                            }
                        }
                        catch (Exception ex)
                        {
                            Log.Error(
                                ex,
                                "Failed to delete folder {Folder}",
                                folder);
                        }
                    }
                }

                Log.Information("CIS OutBound cleanup completed successfully");
            }
            catch (Exception ex)
            {
                Log.Fatal(ex, "Fatal error during cleanup");
            }
            finally
            {
                Log.CloseAndFlush();
            }
        }
    }
}