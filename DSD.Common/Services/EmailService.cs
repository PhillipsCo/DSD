
using Azure.Identity;
using DSD.Common.Models;
 
using Microsoft.Graph;
using Microsoft.Graph.Models;
using Org.BouncyCastle.Cms;
using Renci.SshNet.Messages;
using Serilog;
using System.IO;
using System.Net.Mail;

namespace DSD.Common.Services
{
    /// <summary>
    /// EmailService handles sending emails using Microsoft Graph API.
    /// It uses configuration stored in AccessInfo (email_* properties).
    /// Supports sending HTML content and file attachments.
    /// </summary>
    public class EmailService
    {
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
                    accessInfo.email_secret   // App Registration Client Secret
                );

                var graphClient = new GraphServiceClient(clientSecretCredential);

                // ------------------------------------------------------------
                // STEP 2: Build recipient list from AccessInfo.email_Recipients
                // ------------------------------------------------------------

                var toRecipients = accessInfo.email_recipient
                    .Split(';', StringSplitOptions.RemoveEmptyEntries) // Split by semicolon
                    .Select(email => new Recipient
                    {
                        EmailAddress = new EmailAddress { Address = email.Trim() } // Trim spaces
                    })
                    .ToList();

                // ------------------------------------------------------------
                // STEP 3: Create the email message object
                // ------------------------------------------------------------
                var message = new Microsoft.Graph.Models.Message
                {
                    Subject = subject,
                    Body = new ItemBody
                    {
                        ContentType = BodyType.Html, // HTML content for better formatting
                        Content = content
                    },
                    ToRecipients = toRecipients,
                    Attachments = new List<Microsoft.Graph.Models.Attachment>(), // Initialize empty attachment list
                    Importance = processFailed ? Importance.High : Importance.Normal // ✅ Set priority
                };

                // ------------------------------------------------------------
                // STEP 4: Add file attachments if provided
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
                // STEP 5: Send the email using Microsoft Graph API
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
    }
}


