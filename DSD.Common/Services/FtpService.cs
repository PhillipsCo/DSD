
using Renci.SshNet; // SSH.NET library for SFTP operations
using Serilog;      // Serilog for structured logging
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace DSD.Common.Services
{
    /// <summary>
    /// FtpService provides functionality to upload, download, and orchestrate file transfers via SFTP.
    /// Features:
    /// - Centralized connection handling
    /// - Retry logic for robustness
    /// - Handshake signaling for process coordination
    /// - Structured logging for observability
    /// </summary>
    public class FtpService
    {
        private const int MaxRetries = 3; // Maximum retry attempts for upload/download
        private const int RetryDelaySeconds = 5; // Delay between retries in seconds

        /// <summary>
        /// Establishes an SFTP connection using provided credentials.
        /// </summary>
        private SftpClient Connect(string ftpHost, string ftpUser, string ftpPass)
        {
            var client = new SftpClient(ftpHost, ftpUser, ftpPass);
            try
            {
                client.Connect();
                Log.Information("Connected to FTP server at {FtpHost}", ftpHost);
                return client;
            }
            catch (Exception ex)
            {
                Log.Error(ex, "FTP connection failed. Cannot proceed.");
                throw; // Rethrow to allow caller to handle failure
            }
        }

        /// <summary>
        /// Full download process with handshake:
        /// 1. Wait for ReadyERP file to appear (remote system ready).
        /// 2. Upload WaitCIS handshake file to signal start.
        /// 3. Download all CSV files from remote directory.
        /// 4. Remove WaitCIS and upload ReadyCIS to signal completion.
        /// </summary>
        public void ProcessDownloadFiles(string ftpHost, string ftpUser, string ftpPass, string ftpRemoteFilePath, string ftpLocalFilePath)
        {
            using (var client = Connect(ftpHost, ftpUser, ftpPass))
            {
                try
                {
                    // STEP 1: Wait for ReadyERP file
                    Log.Information("Waiting for ReadyERP file...");
                    WaitForFilePresence(client, ftpRemoteFilePath + "ReadyERP", 600);

                    // STEP 2: Upload WaitCIS
                    Log.Information("Uploading WaitCIS handshake file...");
                    UploadWithRetry(client, "c:/CIS/WaitCIS", ftpRemoteFilePath + "WaitCIS");

                    // STEP 3: Download CSV files
                    Log.Information("Downloading files...");
                    Directory.CreateDirectory(ftpLocalFilePath);
                    var files = client.ListDirectory(ftpRemoteFilePath)
                                      .Where(f => !f.IsDirectory && f.Name.EndsWith(".csv"));
                    foreach (var file in files)
                    {
                        var localPath = Path.Combine(ftpLocalFilePath, file.Name);
                        DownloadWithRetry(client, ftpRemoteFilePath + file.Name, localPath);
                        Log.Information("{FileName} downloaded successfully.", file.Name);
                    }

                    // STEP 4: Remove WaitCIS and upload ReadyCIS
                    Log.Information("Finalizing process: removing WaitCIS and uploading ReadyCIS...");
                    DeleteIfExists(client, ftpRemoteFilePath + "WaitCIS");
                    UploadWithRetry(client, "c:/CIS/ReadyCIS", ftpRemoteFilePath + "ReadyCIS");

                    Log.Information("Download process completed successfully.");
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error occurred during FTP download process.");
                }
            }
        }

        /// <summary>
        /// Full upload process with handshake:
        /// 1. Wait for ReadyERP file to appear.
        /// 2. Upload WaitCIS handshake file.
        /// 3. Upload all CSV files from local directory.
        /// 4. Remove WaitCIS and upload ReadyCIS.
        /// </summary>
        public void ProcessUploadFiles(string ftpHost, string ftpUser, string ftpPass, string ftpRemoteFilePath, string ftpLocalFilePath)
        {
            using (var client = Connect(ftpHost, ftpUser, ftpPass))
            {
                try
                {
                    // STEP 1: Wait for ReadyERP file
                    Log.Information("Waiting for ReadyERP file before upload...");
                    WaitForFilePresence(client, ftpRemoteFilePath + "ReadyERP", 600);

                    // STEP 2: Upload WaitCIS
                    Log.Information("Uploading WaitCIS handshake file...");
                    UploadWithRetry(client, "c:/CIS/WaitCIS", ftpRemoteFilePath + "WaitCIS");

                    // STEP 3: Upload CSV files
                    Log.Information("Uploading files...");
                    var files = Directory.EnumerateFiles(ftpLocalFilePath + "\\Outbound\\" + DateTime.Now.ToString("yyyyMMdd")).Where(f => f.EndsWith(".csv"));
                 
                    foreach (var file in files)
                    {   var ftpPath = ftpRemoteFilePath + "Inbound/MasterData/";
                        var fileName = Path.GetFileName(file);
                        if (fileName.StartsWith("ORD"))
                            ftpPath = ftpPath.Replace("MasterData", "Orders");
                        var remoteFile = ftpPath + Path.GetFileName(file);
                        UploadWithRetry(client, file, remoteFile);
                        Log.Information("{FileName} uploaded successfully.", Path.GetFileName(file));
                    }

                    // STEP 4: Remove WaitCIS and upload ReadyCIS
                    Log.Information("Finalizing upload: removing WaitCIS and uploading ReadyCIS...");
                    DeleteIfExists(client, ftpRemoteFilePath + "WaitCIS");
                    UploadWithRetry(client, "c:/CIS/ReadyCIS", ftpRemoteFilePath + "ReadyCIS");

                    Log.Information("Upload process completed successfully.");
                }
                catch (Exception ex)
                {
                    Log.Error(ex, "Error occurred during FTP upload process.");
                }
            }
        }

        /// <summary>
        /// Uploads a file with retry logic.
        /// </summary>
        private void UploadWithRetry(SftpClient client, string localFilePath, string remoteFilePath)
        {
            int attempt = 0;
            while (attempt < MaxRetries)
            {
                try
                {
                    using (var fs = new FileStream(localFilePath, FileMode.Open))
                    {
                        client.UploadFile(fs, remoteFilePath);
                    }
                    Log.Debug("Uploaded {LocalFile} to {RemoteFile} on attempt {Attempt}.", localFilePath, remoteFilePath, attempt + 1);
                    return;
                }
                catch (Exception ex)
                {
                    attempt++;
                    Log.Warning("Upload failed for {File}. Attempt {Attempt}/{MaxRetries}. Error: {Error}", localFilePath, attempt, MaxRetries, ex.Message);
                    if (attempt >= MaxRetries) throw;
                    Thread.Sleep(RetryDelaySeconds * 1000);
                }
            }
        }

        /// <summary>
        /// Downloads a file with retry logic.
        /// </summary>
        private void DownloadWithRetry(SftpClient client, string remoteFilePath, string localFilePath)
        {
            int attempt = 0;
            while (attempt < MaxRetries)
            {
                try
                {
                    using (var fs = new FileStream(localFilePath, FileMode.Create))
                    {
                        client.DownloadFile(remoteFilePath, fs);
                        client.DeleteFile(remoteFilePath);
                    }
                    Log.Debug("Downloaded {RemoteFile} to {LocalFile} on attempt {Attempt}.", remoteFilePath, localFilePath, attempt + 1);
                    return;
                }
                catch (Exception ex)
                {
                    attempt++;
                    Log.Warning("Download failed for {File}. Attempt {Attempt}/{MaxRetries}. Error: {Error}", remoteFilePath, attempt, MaxRetries, ex.Message);
                    if (attempt >= MaxRetries) throw;
                    Thread.Sleep(RetryDelaySeconds * 1000);
                }
            }
        }

        /// <summary>
        /// Waits for a file to appear on the remote server (used for ReadyERP).
        /// </summary>
        private void WaitForFilePresence(SftpClient client, string remoteFilePath, int timeoutSeconds)
        {
            Stopwatch sw = Stopwatch.StartNew();
            while (sw.Elapsed.TotalSeconds < timeoutSeconds)
            {
                if (client.Exists(remoteFilePath)) return; // File found
                Thread.Sleep(2000); // Check every 2 seconds
            }
            Log.Warning("Timeout waiting for {File} to appear.", remoteFilePath);
            throw new TimeoutException($"File {remoteFilePath} did not appear within {timeoutSeconds} seconds.");
        }

        /// <summary>
        /// Deletes a file from the remote server if it exists.
        /// </summary>
        private void DeleteIfExists(SftpClient client, string remoteFilePath)
        {
            if (client.Exists(remoteFilePath))
            {
                client.Delete(remoteFilePath);
                Log.Debug("Deleted {RemoteFile}.", remoteFilePath);
            }
        }
    }
}
