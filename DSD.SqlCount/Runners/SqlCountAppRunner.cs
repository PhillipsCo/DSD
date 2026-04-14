using DSD.Common.Models;
using DSD.Common.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Graph.Models.ExternalConnectors;
using Serilog;

namespace DSD.SqlCount.Runners
{
    /// <summary>
    /// SqlCountAppRunner orchestrates a simple workflow:
    ///  1) Determine the customer and database context.
    ///  2) Retrieve AccessInfo (credentials / DB catalog info) for the customer.
    ///  3) Count records in the given table or view.
    ///  4) Send an email containing the record count (and optionally logs).
    ///
    /// The "runner" pattern is common for console apps: it is the coordinator,
    /// while services (SqlService, EmailService) do the real work.
    /// </summary>
    public class SqlCountAppRunner
    {
        // Services injected via DI (Dependency Injection)
        private readonly SqlService _sqlService;
        private readonly IConfiguration _configuration;
        private readonly EmailService _emailService;

        /// <summary>
        /// DI constructor: the host creates this class and provides its dependencies.
        /// </summary>
        public SqlCountAppRunner(
            SqlService sqlService,
            EmailService emailService,
            IConfiguration configuration)
        {
            _sqlService = sqlService;
            _emailService = emailService;
            _configuration = configuration;
        }

        /// <summary>
        /// Main entry point invoked by Program.cs.
        /// Command-line args:
        ///   args[0] = customerCode (default "DEMO")
        ///   args[1] = objectName (table or view name, default "RouteSalesOrderStaging")
        ///   args[2] = unused placeholder (your original pattern; kept for compatibility)
        /// </summary>
        public async Task RunAsync(string[] args)
        {
            // Indicates overall success/failure of the run.
            // This affects email subject/body wording.
            bool processFailed = false;

            // Read command-line arguments with safe defaults
            var customerCode = args.Length > 0 ? args[0] : "DEMO";
            var objectName = args.Length > 1 ? args[1] : "RouteSalesOrderStaging";
            var notUsed = args.Length > 2 ? args[2] : "N"; // retained for compatibility

            Log.Information(
                "Starting SqlCount for customer {CustomerCode} on object {ObjectName}",
                customerCode, objectName);

            // AccessInfo is required to send email, so it must be visible outside try/catch
            AccessInfo? accessInfo = null;

            // Nullable so we can safely reference it even if counting fails
            int? recordCount = null;

            // Optional: capture exception details for email/logging
            string? errorDetails = null;

            try
            {
                // STEP 1: Retrieve database and email configuration for this customer
                Log.Information("Retrieving AccessInfo for customer {CustomerCode}", customerCode);
                accessInfo = await _sqlService.GetAccessInfoAsync(customerCode);

                // STEP 2: Count rows in the target table or view
                Log.Information(
                    "Counting records in {ObjectName} using database {Database}",
                    objectName, accessInfo.InitialCatalog);

                recordCount = await _sqlService.SqlCountAsync(
                    accessInfo.InitialCatalog,
                    objectName);

                Log.Information(
                    "Record count for {ObjectName}: {RecordCount}",
                    objectName, recordCount);
            }
            catch (Exception ex)
            {
                // Any exception means the process failed
                processFailed = true;

                // Capture details for logging and optional email inclusion
                errorDetails = ex.ToString();

                Log.Error(
                    ex,
                    "SqlCount failed for customer {CustomerCode} on object {ObjectName}",
                    customerCode, objectName);

                // IMPORTANT:
                // Do NOT rethrow here if you still want the email to be sent.
                // If you rethrow, Program.cs will likely terminate the process.
            }

            // ✅ NO FINALLY BLOCK IS REQUIRED
            // All disposable resources are handled inside SqlCountAsync via "await using".
            // We now decide whether we can send an email.

            if (accessInfo == null)
            {
                // Without AccessInfo, EmailService cannot function
                Log.Warning("AccessInfo is null; email will not be sent.");
                return;
            }


            // Convert record count to friendly display text
            var countText = $"Daily Order Summary – {recordCount} Orders";


            // Determine success/failure label
            var status = processFailed ? "FAILURE" : "SUCCESS";

            // Email subject includes the record count
            var subject = countText;
            // Build email body with clear, structured information




            var body =
                "Hello,\r\n\r\n" +
                "We’ve successfully received your order data.\r\n\r\n" +
                "Summary:\r\n" +
                $"- Total records available: {countText}\r\n" +
                $"- Date processed: {DateTime.Now:MMMM dd, yyyy}\r\n\r\n" +
                "No action is required on your part.\r\n\r\n" +
                "If you have any questions, please contact our support team.\r\n\r\n" +
                "Thank you.";



            // If there was an error, append details (optional but useful for ops teams)
            if (processFailed && !string.IsNullOrWhiteSpace(errorDetails))
            {
                body += $@"

Error Details:
{errorDetails}";
            }

            //// Optional attachments (empty for now)
            //var attachments = new List<string>();

            // STEP 3: Send the email
            var emailto = _configuration.GetValue<string>("emailTo");
            //Console.WriteLine(emailto);
            accessInfo.email_recipient = emailto;
            //Console.WriteLine(accessInfo.email_recipient);
            await _emailService.SendEmailAsync(
                accessInfo,
                subject: subject,
                content: body,
                attachmentPaths: new List<string>(),
                processFailed: processFailed);

            Log.Information("Email sent with subject: {Subject}", subject);
        }
    }
}