using DSD.Common.Models;
using DSD.Common.Services;

using Microsoft.Extensions.Configuration;
using Serilog;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace DSD.LoadCsv.Runners
{
    
    public class LoadCsvAppRunner
    {
        private readonly SqlService _sqlService;
        //private readonly ApiExecutorService _apiExecutorService;
        private readonly CsvExportService _csvExportService;
        private readonly FtpService _ftpService;
        //private readonly EmailService _emailService;
        private readonly IConfiguration _configuration;

        public LoadCsvAppRunner(SqlService sqlService,
             ApiExecutorService apiExecutorService,
             CsvExportService csvExportService,
             FtpService ftpService,
             EmailService emailService,
             IConfiguration configuration)
        {
            _sqlService = sqlService;
            //_apiExecutorService = apiExecutorService;
            _csvExportService = csvExportService;
            _ftpService = ftpService;
            //_emailService = emailService;
            _configuration = configuration;
        }
        public async Task RunAsync(string[] args)
        {
            var customerCode = args.Length > 0 ? args[0] : "DEMO";
            var view = args.Length > 1 ? args[1] : "ALL";
            var sendCIS = args.Length > 2 ? args[2] : "N";

            Log.Information("Sending data to CIS from {view} for {customerCode}",view, customerCode);

            AccessInfo accessInfo = null;
            try
            {
                //Step 1 Get Access Info
                accessInfo = await _sqlService.GetAccessInfoAsync(customerCode);
                Log.Information("AccessInfo retrieved for customer {CustomerCode}", customerCode);
                //Step 2 Export Csv File

                //Step 3 FTP Csv File

                //Step 4 Remove Csv File


            }
            catch (Exception ex)
            {
                //processFailed = true; // Mark failure
                Log.Error(ex, "Error occurred during outbound process");
            }
        }
}
