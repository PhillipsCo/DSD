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
        public string Name { get; set; } = "";
        public bool Enabled { get; set; } = true;
        public int Order { get; set; } = 0;

        public string Sql { get; set; } = "";

        public Dictionary<string, object> Parameters { get; set; } = new();

        public int? CommandTimeoutSeconds { get; set; }

        public string PassMessage { get; set; } = "PASS";
        public string FailMessage { get; set; } = "FAIL";
    }
}
