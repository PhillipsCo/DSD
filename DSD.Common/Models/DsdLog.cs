using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSD.Common.Models
{
    public class DsdLog
    {
        public string? Cust { get; set; }
        public string? Job { get; set; }
        public string? TargetComputer { get; set; }
        public DateOnly? ScheduleDate { get; set; }
        public TimeOnly? ScheduleTime { get; set; }
        public string? job { get; set; }
        public DateTime? StartDate { get; set; }
        public DateTime? EndDate { get; set; }
        public string? Status { get; set; }
    }
}
