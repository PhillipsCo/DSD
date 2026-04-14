using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSD.Common.Models
{
    public class PurgeConfig
    {
        public PurgeSettings Settings { get; set; } = new();
        public PurgeEmail Email { get; set; } = new();
        public List<PurgeJob> Jobs { get; set; } = new();
    }

    public class PurgeSettings
    {
        public bool Enabled { get; set; } = true;
        public bool ContinueOnError { get; set; } = true;
        public int DefaultCommandTimeoutSeconds { get; set; } = 300;
        public string ConnectionStringName { get; set; } = "CustomerConnectionDB";
    }

    public class PurgeEmail
    {
        public bool Enabled { get; set; } = true;
        public string Subject { get; set; } = "Staging purge results";
        public List<string> To { get; set; } = new();
        public List<string> Cc { get; set; } = new();
        public bool IncludeErrorDetails { get; set; } = true;
    }

    public class PurgeJob
    {
        public string jobKey { get; set; } = "";
        public string jobName { get; set; } = "";
        public bool jobEnabled { get; set; } = true;
        public int jobTimeout { get; set; } = 120;

        public string jobSql { get; set; } = "";
        public int jobRetentionDays { get; set; } = 30;



    }
}
