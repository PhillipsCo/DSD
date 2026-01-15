
using DSD.Common.Models;
using DSD.Common.Services;
 
using Microsoft.Extensions.Configuration;
using Serilog;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

/// <summary>
/// AppRunner orchestrates the outbound process:
/// Steps:
/// 1. Parse input arguments
/// 2. Retrieve AccessInfo from DB
/// 3. Conditionally get API list (skip if configured)
/// 4. Conditionally delete old records (skip if configured)
/// 5. Execute APIs (only if API list was retrieved)
/// 6. Conditionally export CSV files
/// 7. Conditionally upload files to FTP
/// 8. Conditionally email daily log file with SUCCESS or FAILURE in subject
/// </summary>
/// 

namespace DSD.Outbound.Runners
{

    public class OutboundAppRunner
    {
        private readonly SqlService _sqlService;
        private readonly ApiExecutorService _apiExecutorService;
        private readonly CsvExportService _csvExportService;
        private readonly FtpService _ftpService;
        private readonly EmailService _emailService;
        private readonly IConfiguration _configuration;

        public OutboundAppRunner(SqlService sqlService,
             ApiExecutorService apiExecutorService,
             CsvExportService csvExportService, 
             FtpService ftpService, 
             EmailService emailService,
             IConfiguration configuration)
        {
            _sqlService = sqlService;
            _apiExecutorService = apiExecutorService;
            _csvExportService = csvExportService;
            _ftpService = ftpService;
            _emailService = emailService;
            _configuration = configuration;
        }

        public async Task RunAsync(string[] args)
        {
            bool processFailed = false; // Track success/failure for email subject
            var customerCode = args.Length > 0 ? args[0] : "DEMO";
            var group = args.Length > 1 ? args[1] : "ALL";
            var sendCIS = args.Length > 2 ? args[2] : "N";

            Log.Information("Starting AppRunner Outbound for customer {CustomerCode}", customerCode);

            AccessInfo accessInfo = null;

            try
            {
                // STEP 2: Retrieve AccessInfo
                accessInfo = await _sqlService.GetAccessInfoAsync(customerCode);
                Log.Information("AccessInfo retrieved for customer {CustomerCode}", customerCode);

                // Read skip flags
                bool skipApiList = _configuration.GetValue<bool>("SkipSteps:SkipApiList");
                bool skipDeleteRecords = _configuration.GetValue<bool>("SkipSteps:SkipDeleteRecords");
                bool skipCsvExport = _configuration.GetValue<bool>("SkipSteps:SkipCsvExport");
                bool skipFtpUpload = _configuration.GetValue<bool>("SkipSteps:SkipFtpUpload");
                bool skipEmail = _configuration.GetValue<bool>("SkipSteps:SkipEmail");

                // STEP 3: Get API list

                var apiList = new List<TableApiName>();
                if (!skipApiList)
                {
                    apiList = await _sqlService.GetApiListAsync(accessInfo.InitialCatalog, group,"Outbound");
                    Log.Information("Retrieved {Count} APIs for execution", apiList.Count);

                    if (apiList.Count == 0)
                    {
                        Log.Warning("No APIs found for customer {CustomerCode}. Exiting process.", customerCode);
                        return;
                    }
                }
                else
                {
                    Log.Information("Skipping API list retrieval based on configuration.");
                }

                // STEP 4: Delete old records
                if (!skipDeleteRecords)
                {
                    if (group.StartsWith("HFS", StringComparison.OrdinalIgnoreCase))
                        await _sqlService.DeleteSingleTableAsync(accessInfo.InitialCatalog, group);
                    else
                        await _sqlService.DeleteTablesWithPrefixAsync(accessInfo.InitialCatalog, "HFS", "Outbound", group);
                }
                else
                {
                    Log.Information("Skipping record deletion based on configuration.");
                }

                // STEP 5: Execute APIs
                if (!skipApiList)
                {
                    await _apiExecutorService.ExecuteApisAndInsertAsync(apiList, accessInfo);
                    Log.Information("API execution completed for customer {CustomerCode}", customerCode);
                }

                // STEP 6: Export CSV
                if (!skipCsvExport && !string.Equals(sendCIS, "N", StringComparison.OrdinalIgnoreCase))
                {
                    await _csvExportService.GenerateCsvFilesAsync(accessInfo.InitialCatalog, accessInfo.ftpLocalFilePath);
                    Log.Information("CSV export completed.");
                }

                // STEP 7: FTP Upload
                if (!skipFtpUpload && !string.Equals(sendCIS, "N", StringComparison.OrdinalIgnoreCase))
                {
                    _ftpService.ProcessUploadFiles(accessInfo.ftpHost, accessInfo.ftpUser, accessInfo.ftpPass, accessInfo.ftpRemoteFilePath, accessInfo.ftpLocalFilePath);
                    Log.Information("FTP upload completed.");
                }
            }
            catch (Exception ex)
            {
                processFailed = true; // Mark failure
                Log.Error(ex, "Error occurred during outbound process");
            }
            finally
            {
                // STEP 8: Email log file
                bool skipEmail = _configuration.GetValue<bool>("SkipSteps:SkipEmail");
                if (!skipEmail && accessInfo != null)
                {

                    var logDirectory = _configuration["logPath"] ?? "C:\\Logs\\";
                    var safeCustomerCode = string.Concat(customerCode.Split(Path.GetInvalidFileNameChars()));
                    var searchPattern = $"outbound-{safeCustomerCode}-log-*.txt";
                    var logFiles = Directory.GetFiles(logDirectory, searchPattern);

                    if (logFiles.Length > 0)
                    {
                        var latestLogFile = logFiles.OrderByDescending(File.GetLastWriteTime).First();

                        // ✅ Copy to a fixed file name that gets overwritten each run
                        var emailLogFile = Path.Combine(logDirectory, "EmailLog.txt");
                        File.Copy(latestLogFile, emailLogFile, true); // Overwrite if exists               
                        var status = processFailed ? "FAILURE" : "SUCCESS";
                        var subject = $"Outbound Process {status} - {customerCode} - {DateTime.Now:yyyy-MM-dd}";

                        await _emailService.SendEmailAsync(
                            accessInfo,
                            subject: subject,
                            content: processFailed
                                ? "The outbound process encountered errors. Please review the attached log."
                                : "The outbound process completed successfully. Please review the attached log.",
                            attachmentPaths: new List<string> { emailLogFile }
                            , processFailed
                        );

                        Log.Information("Email sent with subject: {Subject}", subject);
                    }
                    else
                    {
                        Log.Warning("No log file found in {LogDirectory}. Email not sent.", logDirectory);
                    }
                }
            }
        }
    }
}

