using Serilog;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSD.Common.Services
{
    public class CsvHelperService
    {
        public void MoveCSVfiles(string sourceFolder, string destinationFolder)
        {
            // Ensure destination folder exists
            if (!Directory.Exists(destinationFolder))
            {
                Directory.CreateDirectory(destinationFolder);
            }
            // Get all .csv files in the source folder
            string[] csvFiles = Directory.GetFiles(sourceFolder, "*.csv");

            foreach (string filePath in csvFiles)
            {
                string fileName = Path.GetFileName(filePath);
                string destPath = Path.Combine(destinationFolder, fileName);

                // Move the file

                if (File.Exists(destPath))
                    File.Replace(filePath, destPath, null); // Atomic overwrite
                else
                    File.Move(filePath, destPath); // Move if destination doesn't exist

                Log.Information($"Moved: {fileName}");
            }

            Log.Information("All .csv files have been moved.");
        }

        public void PurgeOldCsv(string folderPath ,int daysOld)
        {

            try
            {
                if (!Directory.Exists(folderPath))
                {
                    Log.Information("Error: The specified folder does not exist.");
                    return;
                }

                string[] csvFiles = Directory.GetFiles(folderPath, "*.csv");

                foreach (string filePath in csvFiles)
                {
                    try
                    {
                        DateTime lastWriteTime = File.GetLastWriteTime(filePath);

                        if (lastWriteTime < DateTime.Now.AddDays(-daysOld))
                        {
                            File.Delete(filePath);
                            Log.Information($"Deleted: {Path.GetFileName(filePath)}");
                        }
                    }
                    catch (UnauthorizedAccessException ex)
                    {
                        Log.Information($"Access denied for file: {filePath}. Details: {ex.Message}");
                    }
                    catch (IOException ex)
                    {
                        Log.Information($"IO error while deleting file: {filePath}. Details: {ex.Message}");
                    }
                    catch (Exception ex)
                    {
                        Log.Information($"Unexpected error for file: {filePath}. Details: {ex.Message}");
                    }
                }

                Log.Information("Cleanup complete.");
            }
            catch (Exception ex)
            {
                Log.Information($"Critical error: {ex.Message}");
            }
        }


    }
}

