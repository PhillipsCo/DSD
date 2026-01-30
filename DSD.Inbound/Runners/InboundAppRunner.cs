
using DSD.Common.Models;
using DSD.Common.Services;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace DSD.Inbound.Runners
{
    /// <summary>
    /// InboundAppRunner orchestrates the inbound data processing workflow.
    /// It uses multiple services for database operations, API execution, FTP file handling,
    /// CSV processing, and email notifications.
    /// </summary>
    public class InboundAppRunner
    {
        // Dependencies injected via constructor
        private readonly SqlService _sqlService;              // Handles SQL database operations
        private readonly IConfiguration _configuration;       // Provides access to appsettings.json configuration
        private readonly ApiExecutorService _apiExecutorService; // Executes APIs and inserts data
        private readonly FtpService _ftpService;              // Handles FTP file transfers
        private readonly CsvHelperService _csvHelperService;  // Processes CSV files (move, purge, etc.)
        private readonly EmailService _emailService;          // Sends email notifications

        /// <summary>
        /// Constructor initializes all required services via Dependency Injection.
        /// </summary>
        public InboundAppRunner(SqlService sqlService,
                                ApiExecutorService apiExecutorService,
                                CsvHelperService csvHelperService,
                                FtpService ftpService,
                                EmailService emailService,
                                IConfiguration configuration)
        {
            _sqlService = sqlService;
            _apiExecutorService = apiExecutorService;
            _csvHelperService = csvHelperService;
            _ftpService = ftpService;
            _emailService = emailService;
            _configuration = configuration;
        }

        /// <summary>
        /// Main entry point for running the inbound process.
        /// Accepts command-line arguments for customer code, group, and CIS flag.
        /// </summary>
        public async Task RunAsync(string[] args)
        {
            bool processFailed = false; // Tracks whether the process failed for email subject
            var customerCode = args.Length > 0 ? args[0] : "DEMO"; // Default customer code if not provided
            var group = args.Length > 1 ? args[1] : "ALL";         // Default group if not provided
            var sendCIS = args.Length > 2 ? args[2] : "N";         // Flag to control CIS sending
            Log.Information("Starting AppRunner Inbound for customer {CustomerCode}", customerCode);

            AccessInfo accessInfo = null; // Holds database and FTP credentials for the customer

            try
            {
                // STEP 1: Retrieve AccessInfo from database for the given customer
                Log.Information("Attempting to get accessInfo for {CustomerCode}", customerCode);
                accessInfo = await _sqlService.GetAccessInfoAsync(customerCode);

                // STEP 2: Read configuration flags to determine which steps to skip
                bool skipApiList = _configuration.GetValue<bool>("SkipSteps:SkipApiList");
                bool skipDeleteRecords = _configuration.GetValue<bool>("SkipSteps:SkipDeleteRecords");
                bool skipCsvExport = _configuration.GetValue<bool>("SkipSteps:SkipCsvExport");
                bool skipFtpUpload = _configuration.GetValue<bool>("SkipSteps:SkipFtpUpload");
                bool skipEmail = _configuration.GetValue<bool>("SkipSteps:SkipEmail");

                // STEP 3: Retrieve API list if not skipped
                var apiList = new List<TableApiName>();
                if (!skipApiList)
                {
                    apiList = await _sqlService.GetApiListAsync(accessInfo.InitialCatalog, group, "Inbound");
                    Log.Information("Retrieved {Count} APIs for execution", apiList.Count);

                    // If no APIs found, exit early
                    if (apiList.Count == 0)
                    {
                        Log.Warning("No APIs found for customer {CustomerCode}. Exiting process.", customerCode);
                        return;
                    }
                }

                // STEP 4: Delete old records if not skipped
                if (!skipDeleteRecords)
                {
                    if (group.StartsWith("HFS", StringComparison.OrdinalIgnoreCase))
                        await _sqlService.DeleteSingleTableAsync(accessInfo.InitialCatalog, group);
                    else
                        await _sqlService.DeleteTablesWithPrefixAsync(accessInfo.InitialCatalog, "HFS", "Inbound", group);
                }
                else
                {
                    Log.Information("Skipping record deletion based on configuration.");
                }

                // STEP 5: Execute APIs if not skipped
                if (!skipApiList)
                {
                    await _apiExecutorService.ExecuteApisAndInsertAsync(apiList, accessInfo);
                    Log.Information("API execution completed for customer {CustomerCode}", customerCode);
                }

                // STEP 6: FTP Download if not skipped and CIS flag is set
                if (!skipFtpUpload && !string.Equals(sendCIS, "N", StringComparison.OrdinalIgnoreCase))
                {
                    var remotePaths = new[]
                    {
                        accessInfo.ftpRemoteFilePath + "Outbound/",
                        accessInfo.ftpRemoteFilePath + "Outbound/RouteSettlements/"
                    };
                    var localPath = accessInfo.ftpLocalFilePath + "Inbound\\";
                    foreach (var remotePath in remotePaths)
                    {
                        var ftpResult = _ftpService.ProcessDownloadFiles(accessInfo.ftpHost,
                                                         accessInfo.ftpUser,
                                                         accessInfo.ftpPass,
                                                         remotePath,
                                                         localPath);
                        if (!ftpResult)
                        {
                            processFailed = true; // ✅ Mark failure for email
                            Log.Error("FTP download failed due to handshake timeout or other error.");
                        }
                        else
                        {
                            Log.Information("FTP download completed successfully.");
                        }
                    }
                    Log.Information("FTP download completed.");
                }

                // STEP 7: Process CSV files if FTP upload is not skipped
                if (!skipFtpUpload)
                {
                    string sourceFolder = Path.Combine(accessInfo.ftpLocalFilePath, "Inbound");
                    string destinationFolder = Path.Combine(sourceFolder, "Archive");

                    // Insert CSV data into database
                    _sqlService.InsertCSV(accessInfo.InitialCatalog, sourceFolder);

                    // Move processed CSV files to archive
                    _csvHelperService.MoveCSVfiles(sourceFolder, destinationFolder);

                    // Purge old CSV files from archive (older than 30 days)
                    _csvHelperService.PurgeOldCsv(destinationFolder, 30);
                }
            }
            catch (Exception ex)
            {
                processFailed = true; // Mark failure for email notification
                Log.Error(ex, "Error occurred during inbound process");
            }
            finally
            {
                // STEP 8: Send email notification with log file if not skipped
                bool skipEmail = _configuration.GetValue<bool>("SkipSteps:SkipEmail");
                if (!skipEmail && accessInfo != null)
                {
                    var logDirectory = _configuration["logPath"] ?? "C:\\Logs\\";
                    var safeCustomerCode = string.Concat(customerCode.Split(Path.GetInvalidFileNameChars()));
                    var searchPattern = $"outbound-{safeCustomerCode}-log-*.txt";
                    var logFiles = Directory.GetFiles(logDirectory, searchPattern);

                    if (logFiles.Length > 0)
                    {
                        // Get the latest log file
                        var latestLogFile = logFiles.OrderByDescending(File.GetLastWriteTime).First();

                        // Copy to a fixed file name for email attachment
                        var emailLogFile = Path.Combine(logDirectory, "EmailLog.txt");
                        File.Copy(latestLogFile, emailLogFile, true); // Overwrite if exists

                        var status = processFailed ? "FAILURE" : "SUCCESS";
                        var subject = $"Inbound Process {status} - {customerCode} - {DateTime.Now:yyyy-MM-dd}";

                        // Send email with log file attached
                        await _emailService.SendEmailAsync(
                            accessInfo,
                            subject: subject,
                            content: processFailed
                                ? "The inbound process encountered errors. Please review the attached log."
                                : "The inbound process completed successfully. Please review the attached log.",
                            attachmentPaths: new List<string> { emailLogFile },
                            processFailed
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

