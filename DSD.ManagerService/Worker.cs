using Microsoft.Extensions.Configuration;
using Microsoft.Data.SqlClient;
using System.Linq;
using System.Runtime.InteropServices;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DSD.ManagerService
{
    public class Worker : BackgroundService
    {


        private readonly ILogger<Worker> _logger;
        //List<int> _runMinutes = new List<int>();
        private readonly IConfiguration _configuration;
        private readonly TimeSpan _interval;
        private readonly string _targetcomputer;
        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            int intervalSeconds = _configuration.GetValue<int>("WorkerSettings:IntervalSeconds");
            _interval = TimeSpan.FromSeconds(intervalSeconds);
            _targetcomputer = _configuration.GetValue<string>("TargetComputer");

        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Run the job
                    await RunScheduledTaskAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error executing scheduled task");
                }

                // Calculate delay until next 5?minute boundary
                var delay = GetDelayUntilNextFiveMinuteMark();

                _logger.LogInformation(
                    "Next run in {Minutes} minutes and {Seconds} seconds",
                    delay.Minutes,
                    delay.Seconds);

                await Task.Delay(delay, stoppingToken);
            }
        }

        private static TimeSpan GetDelayUntilNextFiveMinuteMark()
        {
            var now = DateTime.Now; // local time

            int minutesToAdd = 5 - (now.Minute % 5);
            if (minutesToAdd == 0)
                minutesToAdd = 5;

            var nextRun = new DateTime(
                now.Year,
                now.Month,
                now.Day,
                now.Hour,
                now.Minute,
                0
            ).AddMinutes(minutesToAdd);

            return nextRun - now;
        }


        //protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        //{


        //    string connectionString = _configuration.GetConnectionString("DefaultConnection");
        //    string logPath = _configuration.GetConnectionString("rootFilepath");




        //    var timer = new PeriodicTimer(_interval);
        //    while (!stoppingToken.IsCancellationRequested)
        //    {


        //        await timer.WaitForNextTickAsync(stoppingToken);

        //        _logger.LogInformation("Get Job List for : {time}", DateTimeOffset.Now);

        //        await RunScheduledTaskAsync();

        //    }
        //}





        private async Task RunScheduledTaskAsync()
        {
            try
            {
                string connectionString = _configuration.GetConnectionString("DefaultConnection");
                _logger.LogInformation("Checking for an APP to run");

                using (var conn = new SqlConnection(connectionString))
                {
                    await conn.OpenAsync();

                    // Prefer to keep the SQL text in code or a dedicated setting, not a "ConnectionString" entry.
                    // Ensure your stored SQL text uses these parameter names: @ScheduleDate, @ScheduleTime
                    string sqlQuery = @"
                                        SELECT TOP (1)
                                            l.logid,
                                            e.jobid,
                                            e.cust,
                                            e.Job,
                                            e.RUNGROUP,
                                            e.SendCIS,
                                            e.ScheduleTime
                                        FROM DSD_Job_Log AS l
                                        JOIN DSD_Job_Executables AS e
                                        ON e.JobId = l.JobId
                                        WHERE l.ScheduleDate = @ScheduleDate
                                        AND e.ScheduleTime <= @ScheduleTime
                                        AND l.enddate IS NULL
                                        AND e.TargetComputer = @TargetComputer
                                        ORDER BY l.logid;";

                    using (var cmd = new SqlCommand(sqlQuery, conn))
                    {
                        // Compute current local date/time in Pacific Time (DST-aware)
                        var pacificTz = TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time");
                        var pacificNow = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, pacificTz);

                        var scheduleDate = pacificNow.Date;        // DATE (no time)
                        var scheduleTime = pacificNow.TimeOfDay;   // TIME (time-of-day only)

                        cmd.Parameters.Add("@ScheduleDate", SqlDbType.Date).Value = scheduleDate;
                        cmd.Parameters.Add("@ScheduleTime", SqlDbType.Time).Value = scheduleTime;
                        cmd.Parameters.Add("@TargetComputer", SqlDbType.VarChar).Value = _targetcomputer;
                        using (var rdr = await cmd.ExecuteReaderAsync())
                        {
                            if (await rdr.ReadAsync())
                            {
                                // Read job details
                                var jobPath = rdr.GetString(rdr.GetOrdinal("Job"));       // likely full exe path
                                var cust = rdr.GetString(rdr.GetOrdinal("cust"));
                                var id = rdr.GetInt32(rdr.GetOrdinal("logid"));
                                var runGroup = rdr.GetString(rdr.GetOrdinal("RUNGROUP"));
                                var sendCIS = rdr.GetString(rdr.GetOrdinal("SendCIS"));



                                // Determine base directory from the job path
                                string appBaseDirectory = Path.GetDirectoryName(jobPath) ?? AppContext.BaseDirectory;
                                _logger.LogInformation("App base directory resolved to: {appBaseDirectory}", appBaseDirectory);

                                // Build configuration for that app dynamically
                                var appConfiguration = new ConfigurationBuilder()
                                    .SetBasePath(appBaseDirectory)
                                    .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                                    .Build();

                                // Run the app and capture start/end times
                                (DateTime start, DateTime end, string status) = await RunApp(jobPath, cust, runGroup, sendCIS);

                                // Close reader before issuing UPDATE against the same connection
                                rdr.Close();

                                // Update the job log
                                string updateSql = @"
                                                    UPDATE DSD_Job_Log
                                                    SET StartDate = @StartTime,
                                                        EndDate   = @EndTime,
                                                        Status    = @Status
                                                    WHERE LogId   = @Id;";

                                using (var updateCmd = new SqlCommand(updateSql, conn))
                                {
                                    updateCmd.Parameters.Add("@StartTime", SqlDbType.DateTime).Value = start;
                                    updateCmd.Parameters.Add("@EndTime", SqlDbType.DateTime).Value = end;
                                    updateCmd.Parameters.Add("@Status", SqlDbType.VarChar, 50).Value = status ?? (object)DBNull.Value;
                                    updateCmd.Parameters.Add("@Id", SqlDbType.Int).Value = id;

                                    await updateCmd.ExecuteNonQueryAsync();
                                }

                                _logger.LogInformation("Updated row {id} with StartTime={start}, EndTime={end}, Status={status}", id, start, end, status);
                            }
                            else
                            {
                                _logger.LogInformation("No eligible jobs found for {date} at {time}", scheduleDate, scheduleTime);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Problem getting data");
            }
        }



        private async Task<(DateTime StartTime, DateTime ExitTime, string status)> RunApp(string app, string cust, string p1, string p2)
        {
            try
            {

                var process = new Process
                {
                    StartInfo = new ProcessStartInfo
                    {
                        FileName = app,
                        Arguments = $"{cust} {p1} {p2}",
                        WorkingDirectory = Path.GetDirectoryName(app),
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        UseShellExecute = false,
                        CreateNoWindow = true,
                        //WorkingDirectory = Path.GetDirectoryName(app)
                    }
                };

                process.Start();

                string output = await process.StandardOutput.ReadToEndAsync();
                string error = await process.StandardError.ReadToEndAsync();
                string status = "Success";
                await Task.Run(() => process.WaitForExit(60000)); // 60s timeout
                if (_configuration.GetValue<string>("WorkerSettings:LogOutput") == "Y")
                    _logger.LogInformation("Output: {output}", output);
                if (!string.IsNullOrEmpty(error))
                {
                    status = "Fail";
                    _logger.LogError("Error: {error}", error);
                }

                return (process.StartTime, process.ExitTime, status);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to run {app} with args {cust} {p1} {p2}", app, cust, p1, p2);

                return (DateTime.MinValue, DateTime.MinValue, "Fail"); // fallback
            }
        }

    }
}
