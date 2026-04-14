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
            //STEP 1 Get AccessInfo for Customer
                AccessInfo accessInfo = null; // Holds database and FTP credentials for the customer
                Log.Information("Attempting to get accessInfo for {CustomerCode}", customerCode);
                accessInfo = await _sqlService.GetAccessInfoAsync(customerCode);
            //Step2 Load EOD model for this customer from DSD_EOD table
            
            var purgeConfig = new PurgeConfig();
            var jobs = await _sqlService.GetPurgeJobsAsync(accessInfo.InitialCatalog);
            purgeConfig.Jobs = jobs;
            
            //Step3 Iterate through list of jobs

            foreach (var job in jobs)
            {
              
               Log.Information("Running purge job : {Name}",  job.jobName);


               var result = await _sqlService.ExecuteDeleteAsync(
                        catalog: accessInfo.InitialCatalog ?? "DEMO",
                        sql: job.jobSql,
                         job.jobRetentionDays,
                        job.jobTimeout
                    );

                if (result.Success)
                {
                    
                    Log.Information("PASS: {JobName} | RowsAffected={Rows}", job.jobName, result.RowsAffected);
                }
                else
                {
                    Log.Error("FAIL: {JobName} | Error={Error}", job.jobName, result.ErrorMessage);
                    await _emailService.SendEmailAsync(
                            accessInfo,
                            subject: "EOD delete Failure",
                            content: $"{job.jobName} failed Error = {result.ErrorMessage}",
                            attachmentPaths: null,
                            true
                        );
                }

            }

        }

        }
}
