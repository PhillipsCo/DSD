using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace DSD.Common.Models
{
    public class AccessInfo
    {
        public string? Url { get; set; }
        public string? Grant_Type { get; set; }
        //public String? Token_URL { get; set; }

        public string? Client_ID { get; set; }

        public string? Client_Secret { get; set; }

        public string? Scope { get; set; }
        public string? RootUrl { get; set; }

        public string? path { get; set; }
        public string? ftpHost { get; set; }
        public string? ftpUser { get; set; }
        public string? ftpPass { get; set; }

        public string? ftpRemoteFilePath { get; set; }
        public string? ftpLocalFilePath { get; set; }

        public string? DataSource { get; set; }
        public string? InitialCatalog { get; set; }

        public string? UserID { get; set; }
        public string? Password { get; set; }

        public string? DayOffset { get; set; }
        public string? email_tenantId { get; set; }
        public string? email_clientId { get; set; }
        public string? email_secret { get; set; }
        public string? email_sender { get; set; }
        public string? email_recipient { get; set; }
    }
}

