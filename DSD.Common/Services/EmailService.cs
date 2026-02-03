using Azure.Identity;
using DSD.Common.Models;

using Microsoft.Graph;
using Microsoft.Graph.Models;
using Serilog;
using System.IO;
using System.Net.Mail;
using System.Text;
using System.Text.RegularExpressions;

namespace DSD.Common.Services
{
    /// <summary>
    /// EmailService handles sending emails using Microsoft Graph API.
    /// It uses configuration stored in AccessInfo (email_* properties).
    /// Supports sending HTML content and file attachments.
    /// </summary>
    public class EmailService
    {
        // --- Configurable limits for log scanning ---
        private const long MaxTailBytesPerLog = 2 * 1024 * 1024; // scan last 2 MB per log to avoid huge reads
        private const int MaxLinesPerEmail = 2000;            // cap injected lines to keep email readable
        private static readonly string[] LogExtensions = new[] { ".log", ".txt" };

        // Entry header like: [2026-02-01 19:30:00 INF] Message...
        // Captures <level> = INF/WRN/ERR/DBG/etc. (3–8 letters)
        private static readonly Regex EntryStartRegex = new(
            pattern: @"^\[(?<ts>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+(?<level>[A-Za-z]{3,8})\]\s",
            options: RegexOptions.Compiled | RegexOptions.CultureInvariant);

        /// <summary>
        /// Sends an email with optional attachments using Microsoft Graph.
        /// Authentication and email details are sourced from AccessInfo.
        /// </summary>
        /// <param name="accessInfo">Contains email_* properties for authentication and recipients.</param>
        /// <param name="subject">Subject line of the email.</param>
        /// <param name="content">HTML body content of the email.</param>
        /// <param name="attachmentPaths">List of file paths to attach to the email.</param>
        public async Task SendEmailAsync(AccessInfo accessInfo, string subject, string content, List<string> attachmentPaths, bool processFailed)
        {
            try
            {
                // ------------------------------------------------------------
                // STEP 1: Authenticate with Microsoft Graph using Azure AD app credentials
                // ------------------------------------------------------------
                var clientSecretCredential = new ClientSecretCredential(
                    accessInfo.email_tenantId,      // Azure AD Tenant ID
                    accessInfo.email_clientId,      // App Registration Client ID
                    accessInfo.email_secret         // App Registration Client Secret
                );

                var graphClient = new GraphServiceClient(clientSecretCredential);

                // ------------------------------------------------------------
                // STEP 2: Build recipient list from AccessInfo.email_recipient
                // ------------------------------------------------------------
                var toRecipients = accessInfo.email_recipient
                    .Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries) // support ; or ,
                    .Select(email => new Recipient
                    {
                        EmailAddress = new EmailAddress { Address = email.Trim() } // Trim spaces
                    })
                    .ToList();

                // ------------------------------------------------------------
                // STEP 3: Scan attached logs and collect non-INF entry headers
                // ------------------------------------------------------------
                var nonInfHeaders = new List<string>();

                if (attachmentPaths != null && attachmentPaths.Any())
                {
                    foreach (var path in attachmentPaths)
                    {
                        if (!File.Exists(path))
                        {
                            Log.Warning("Attachment file not found: {Path}", path);
                            continue;
                        }

                        var ext = Path.GetExtension(path).ToLowerInvariant();
                        if (!LogExtensions.Contains(ext))
                            continue;

                        try
                        {
                            foreach (var hdr in ReadTailAndYieldNonInfEntryHeaders(path, MaxTailBytesPerLog, MaxLinesPerEmail))
                            {
                                nonInfHeaders.Add(hdr);
                                if (nonInfHeaders.Count >= MaxLinesPerEmail) break;
                            }
                        }
                        catch (Exception ex)
                        {
                            Log.Warning(ex, "Failed to read/parse log file: {Path}", path);
                        }
                    }
                }

                // ------------------------------------------------------------
                // STEP 4: Create the email message object (prepend log highlights)
                // ------------------------------------------------------------
                var bodyBuilder = new StringBuilder();

                if (nonInfHeaders.Count > 0)
                {
                    subject = subject.Replace("SUCCESS","WARNING");
                    content = content.Replace("successfully", "with warnings");
                    bodyBuilder.AppendLine(
                        @"<div style=""font-family:Segoe UI, Arial, sans-serif; font-size:13px"">" +
                        @"<h3 style=""margin:0 0 8px 0"">Log Highlights (non-INF)</h3>" +
                        //@"<p style=""margin:0 0 8px 0; color:#555"">First line of each non-INF log entry (from attached log file(s)):</p>" +
                        @"<pre style=""background:#f5f7fa;border:1px solid #e1e6ee;padding:12px;white-space:pre-wrap;word-wrap:break-word;max-height:400px;overflow:auto"">");

                    foreach (var line in nonInfHeaders)
                    {
                        bodyBuilder.AppendLine(System.Net.WebUtility.HtmlEncode(line));
                    }

                    bodyBuilder.AppendLine("</pre>");

                    if (nonInfHeaders.Count >= MaxLinesPerEmail)
                    {
                        bodyBuilder.AppendLine(@"<p style=""color:#888;margin-top:6px"">…output truncated for brevity…</p>");
                    }

                    bodyBuilder.AppendLine(@"<hr style=""border:none;border-top:1px solid #e1e6ee;margin:16px 0""/>");
                    bodyBuilder.AppendLine("</div>");
                }

                // Append your original provided HTML content
                bodyBuilder.Append(content);
                var finalHtmlBody = bodyBuilder.ToString();

                var message = new Microsoft.Graph.Models.Message
                {
                     
                    Subject = subject,
                    Body = new ItemBody
                    {
                        ContentType = BodyType.Html, // HTML content for better formatting
                        Content = finalHtmlBody
                    },
                    ToRecipients = toRecipients,
                    Attachments = new List<Microsoft.Graph.Models.Attachment>(), // Initialize empty attachment list
                    Importance = processFailed ? Importance.High : Importance.Normal // ✅ Set priority
                };

                // ------------------------------------------------------------
                // STEP 5: Add file attachments if provided (unchanged)
                // ------------------------------------------------------------
                if (attachmentPaths != null && attachmentPaths.Any())
                {
                    foreach (var path in attachmentPaths)
                    {
                        if (File.Exists(path))
                        {
                            var fileBytes = await File.ReadAllBytesAsync(path);
                            message.Attachments.Add(new FileAttachment
                            {
                                OdataType = "#microsoft.graph.fileAttachment",
                                Name = Path.GetFileName(path), // Use file name for attachment
                                ContentBytes = fileBytes       // File content in bytes
                            });
                        }
                        else
                        {
                            Log.Warning("Attachment file not found: {Path}", path);
                        }
                    }
                }

                // ------------------------------------------------------------
                // STEP 6: Send the email using Microsoft Graph API (unchanged)
                // ------------------------------------------------------------
                await graphClient.Users[accessInfo.email_sender]
                    .SendMail
                    .PostAsync(new Microsoft.Graph.Users.Item.SendMail.SendMailPostRequestBody
                    {
                        Message = message,
                        SaveToSentItems = true // Save a copy in Sent Items
                    });

                Log.Information("Email sent successfully with subject: {Subject}", subject);
            }
            catch (Exception ex)
            {
                Log.Error(ex, "Error sending email");
            }
        }

        // ----------------- Helpers: log tail scanning -----------------

        /// <summary>
        /// Reads the tail of the file and yields ONLY the first line of each entry
        /// whose level != "INF". Continuation lines are ignored.
        /// </summary>
        private static IEnumerable<string> ReadTailAndYieldNonInfEntryHeaders(string path, long maxTailBytes, int maxLines)
        {
            var fi = new FileInfo(path);
            long start = fi.Length > maxTailBytes ? fi.Length - maxTailBytes : 0;

            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            fs.Seek(start, SeekOrigin.Begin);

            // If starting mid-line, discard the partial line
            if (start > 0) _ = ReadLine(fs);

            int yielded = 0;
            bool insideEntry = false; // we saw an entry start; we're in its (skipped) body

            string? line;
            while ((line = ReadLine(fs)) is not null)
            {
                if (TryGetEntryLevel(line, out var level))
                {
                    insideEntry = true;

                    // Include header ONLY if level != INF
                    if (!string.Equals(level, "INF", StringComparison.OrdinalIgnoreCase))
                    {
                        yield return line;
                        yielded++;
                        if (yielded >= maxLines) yield break;
                    }

                    // continuation lines for this entry are skipped by design
                    continue;
                }

                // Not an entry header: treat as continuation/noise and skip
                // (Change here if you ever want to include standalone lines)
            }
        }

        /// <summary>
        /// Determines if a line starts a new entry and (if so) outputs the level.
        /// </summary>
        private static bool TryGetEntryLevel(string line, out string? level)
        {
            var m = EntryStartRegex.Match(line);
            if (m.Success)
            {
                level = m.Groups["level"].Value;
                return true;
            }
            level = null;
            return false;
        }

        /// <summary>
        /// Efficient line reader for FileStream without StreamReader allocations.
        /// </summary>
        private static string? ReadLine(FileStream fs)
        {
            var sb = new StringBuilder(256);
            int b;
            while ((b = fs.ReadByte()) != -1)
            {
                if (b == '\n') break;
                if (b != '\r') sb.Append((char)b);
            }
            if (b == -1 && sb.Length == 0) return null;
            return sb.ToString();
        }
    }
}
//using Azure.Identity;
//using DSD.Common.Models;

//using Microsoft.Graph;
//using Microsoft.Graph.Models;
//using Org.BouncyCastle.Cms;
//using Renci.SshNet.Messages;
//using Serilog;
//using System.IO;
//using System.Net.Mail;

//namespace DSD.Common.Services
//{
//    /// <summary>
//    /// EmailService handles sending emails using Microsoft Graph API.
//    /// It uses configuration stored in AccessInfo (email_* properties).
//    /// Supports sending HTML content and file attachments.
//    /// </summary>
//    public class EmailService
//    {
//        /// <summary>
//        /// Sends an email with optional attachments using Microsoft Graph.
//        /// Authentication and email details are sourced from AccessInfo.
//        /// </summary>
//        /// <param name="accessInfo">Contains email_* properties for authentication and recipients.</param>
//        /// <param name="subject">Subject line of the email.</param>
//        /// <param name="content">HTML body content of the email.</param>
//        /// <param name="attachmentPaths">List of file paths to attach to the email.</param>
//        public async Task SendEmailAsync(AccessInfo accessInfo, string subject, string content, List<string> attachmentPaths, bool processFailed)
//        {
//            try
//            {
//                // ------------------------------------------------------------
//                // STEP 1: Authenticate with Microsoft Graph using Azure AD app credentials
//                // ------------------------------------------------------------
//                var clientSecretCredential = new ClientSecretCredential(
//                    accessInfo.email_tenantId,      // Azure AD Tenant ID
//                    accessInfo.email_clientId,      // App Registration Client ID
//                    accessInfo.email_secret   // App Registration Client Secret
//                );

//                var graphClient = new GraphServiceClient(clientSecretCredential);

//                // ------------------------------------------------------------
//                // STEP 2: Build recipient list from AccessInfo.email_Recipients
//                // ------------------------------------------------------------

//                var toRecipients = accessInfo.email_recipient
//                    .Split(';', StringSplitOptions.RemoveEmptyEntries) // Split by semicolon
//                    .Select(email => new Recipient
//                    {
//                        EmailAddress = new EmailAddress { Address = email.Trim() } // Trim spaces
//                    })
//                    .ToList();

//                // ------------------------------------------------------------
//                // STEP 3: Create the email message object
//                // ------------------------------------------------------------
//                var message = new Microsoft.Graph.Models.Message
//                {
//                    Subject = subject,
//                    Body = new ItemBody
//                    {
//                        ContentType = BodyType.Html, // HTML content for better formatting
//                        Content = content
//                    },
//                    ToRecipients = toRecipients,
//                    Attachments = new List<Microsoft.Graph.Models.Attachment>(), // Initialize empty attachment list
//                    Importance = processFailed ? Importance.High : Importance.Normal // ✅ Set priority
//                };

//                // ------------------------------------------------------------
//                // STEP 4: Add file attachments if provided
//                // ------------------------------------------------------------
//                if (attachmentPaths != null && attachmentPaths.Any())
//                {
//                    foreach (var path in attachmentPaths)
//                    {
//                        if (File.Exists(path))
//                        {
//                            var fileBytes = await File.ReadAllBytesAsync(path);
//                            message.Attachments.Add(new FileAttachment
//                            {
//                                OdataType = "#microsoft.graph.fileAttachment",
//                                Name = Path.GetFileName(path), // Use file name for attachment
//                                ContentBytes = fileBytes       // File content in bytes
//                            });
//                        }
//                        else
//                        {
//                            Log.Warning("Attachment file not found: {Path}", path);
//                        }
//                    }
//                }

//                // ------------------------------------------------------------
//                // STEP 5: Send the email using Microsoft Graph API
//                // ------------------------------------------------------------
//                await graphClient.Users[accessInfo.email_sender]
//                    .SendMail
//                    .PostAsync(new Microsoft.Graph.Users.Item.SendMail.SendMailPostRequestBody
//                    {
//                        Message = message,
//                        SaveToSentItems = true // Save a copy in Sent Items
//                    });

//                Log.Information("Email sent successfully with subject: {Subject}", subject);
//            }
//            catch (Exception ex)
//            {
//                Log.Error(ex, "Error sending email");
//            }
//        }
//    }
//}


