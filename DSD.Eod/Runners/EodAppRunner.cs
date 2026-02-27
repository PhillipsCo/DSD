using DSD.Common.Models;
using DSD.Common.Services;
using Microsoft.Extensions.Configuration;
//using Microsoft.Extensions.Configuration.Json;

//using Microsoft.Graph.Models.ExternalConnectors;
using Serilog;
using System.Runtime.ConstrainedExecution;
using System.Text.RegularExpressions;

namespace DSD.Eod.Runners
{
    //***EodAppRunner***:
    //1.Load AccessInfo using supplied argument
    //2.Load a JSON “job list” (SQL delete statements + metadata)
    //3.Execute each job independently(try/catch per job)
    //4.Capture pass/fail, rows affected, duration, error message
    //5.Send an email summary at the end
    public class EodAppRunner
    {
        private readonly SqlService _sqlService;              // Handles SQL database operations
        private readonly IConfiguration _configuration;       // Provides access to appsettings.json configuration
        private readonly EmailService _emailService;          // Sends email notifications
        public EodAppRunner(SqlService sqlService,
                            EmailService emailService,
                            IConfiguration configuration)
        {
            _sqlService = sqlService;
            _emailService = emailService;
            _configuration = configuration;
        }
            
            public async Task RunAsync(string[] args)
            {
                var customerCode = args.Length > 0 ? args[0] : "DEMO"; // Default customer code if not provided

                Log.Information("Starting EOD purge for customer {CustomerCode}", customerCode);
            //STEP 1
                AccessInfo accessInfo = null; // Holds database and FTP credentials for the customer
                Log.Information("Attempting to get accessInfo for {CustomerCode}", customerCode);
                accessInfo = await _sqlService.GetAccessInfoAsync(customerCode);
            //Step2
            var purgeSection = _configuration.GetSection("PurgeJobs");

            if (!purgeSection.Exists())
            {
                Log.Error("Missing configuration section 'PurgeJobs' in appsettings.json.");
                return;
            }

            var purgeConfig = purgeSection.Get<PurgeConfig>();
            if (purgeConfig is null)
            {
                Log.Error("Failed to bind 'PurgeJobs' to PurgeConfig. Check JSON structure.");
                return;
            }
                purgeConfig = _configuration
               .GetSection("PurgeJobs")
               .Get<PurgeConfig>();

            //Step3


            var jobsToRun = purgeConfig.Jobs
                .Where(j => j.Enabled)
                .OrderBy(j => j.Order)
                .ToList();

            foreach (var job in jobsToRun)
            {
               var timeout = job.CommandTimeoutSeconds ?? 60;
               Log.Information("Running purge job {Order}: {Name}", job.Order, job.Name);


               var result = await _sqlService.ExecuteDeleteAsync(
                        catalog: accessInfo.InitialCatalog ?? "DEMO",
                        sql: job.Sql,
                        parameters: job.Parameters,
                        commandTimeoutSeconds: timeout
                    );

                if (result.Success)
                {
                    Log.Information(job.PassMessage);
                    Log.Information("PASS: {JobName} | RowsAffected={Rows}", job.Name, result.RowsAffected);
                }
                else
                {
                    Log.Error("FAIL: {JobName} | Error={Error}", job.Name, result.ErrorMessage);
                    await _emailService.SendEmailAsync(
                            accessInfo,
                            subject: "EOD delete Failure",
                            content: $"{job.Name} failed Error = {result.ErrorMessage}",
                            attachmentPaths: null,
                            true
                        );
                }

            }

        }

        }
}
