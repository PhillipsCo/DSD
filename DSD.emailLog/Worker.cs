using DSD.Common.Models;
using DSD.Common.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

namespace DSD.emailLog
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly SqlService _sqlService;
        private readonly EmailService _emailService;
        private readonly IConfiguration _configuration;
        private static readonly TimeSpan Interval = TimeSpan.FromHours(3);
        private static readonly TimeSpan StartTime = new(6, 0, 0);   // 6:00 AM
        private static readonly TimeSpan EndTime = new(24, 0, 0);  // Midnight (end exclusive)
        public Worker(
            ILogger<Worker> logger,
            SqlService sqlService,
            EmailService emailService,
            IConfiguration configuration)
        {
            _logger = logger;
            _sqlService = sqlService;
            _emailService = emailService;
            _configuration = configuration;
        }
        private async Task RunEmailJobAsync(CancellationToken stoppingToken)
        {
            try
            {
                _logger.LogInformation("📨 Sending email at {Time}", DateTime.Now);

                // Your “business work” goes here
                AccessInfo accessInfo = null;
                accessInfo = await _sqlService.GetAccessInfoAsync("DEMO");
                Log.Information("AccessInfo retrieved for customer {CustomerCode}", "DEMO");
                _logger.LogCritical("✅ Worker ExecuteAsync entered");

                    var logDirectory = _configuration["logPath"] ?? "C:\\Logs\\";
                    var searchPattern = $"Managerlog-*.txt";
                    var logFiles = Directory.GetFiles(logDirectory, searchPattern);
                    if (logFiles.Length > 0)
                    {
                        var latestLogFile = logFiles.OrderByDescending(File.GetLastWriteTime).First();
                        var emailLogFile = Path.Combine(logDirectory, "EmailtheLog.txt");
                        File.Copy(latestLogFile, emailLogFile, true); // Overwrite if exists   
                        var status = "FYI";
                        var subject = $"DSD Manager Log- {DateTime.Now:yyyy-MM-dd}";

                        await _emailService.SendEmailAsync(
                                   accessInfo,
                                   subject: subject,
                                   content: "See Log",
                                   attachmentPaths: new List<string> { emailLogFile }
                                   , false
                               );
                    }

                    _logger.LogInformation("✅ Email sent successfully at {Time}", DateTime.Now);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                // graceful shutdown - don’t treat as error
                _logger.LogInformation("🛑 Email send canceled due to shutdown");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "❌ Error sending email");
            }
        }

        private static DateTime GetNextRunTime(DateTime now)
        {
            var todayStart = now.Date.Add(StartTime);
            var todayEnd = now.Date.Add(EndTime);

            // Before 6 AM → first run at 6 AM
            if (now < todayStart)
                return todayStart;

            // Within window → next 3-hour boundary strictly after now
            for (var run = todayStart; run < todayEnd; run += Interval)
            {
                if (run > now)
                    return run;
            }

            // After (or at) midnight window → next day 6 AM
            return now.Date.AddDays(1).Add(StartTime);
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)


        {
#if DEBUG
            while (!System.Diagnostics.Debugger.IsAttached)
                await Task.Delay(250, stoppingToken);
#endif

            _logger.LogInformation("✅ Email Worker started");

            while (!stoppingToken.IsCancellationRequested)
            {
                var now = DateTime.Now;                 // local time
                var nextRun = GetNextRunTime(now);

                _logger.LogInformation("⏳ Next scheduled run at {NextRun}", nextRun);

                var delay = nextRun - DateTime.Now;     // recalc in case clock moved a bit
                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, stoppingToken);
                }

                if (stoppingToken.IsCancellationRequested)
                    break;

                await RunEmailJobAsync(stoppingToken);
            }

            _logger.LogInformation("🛑 Email Worker stopping");
        }
    }
}