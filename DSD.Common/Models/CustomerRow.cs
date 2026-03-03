using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSD.Common.Models
{

    public sealed class CustomerRow
    {
        public int Id { get; init; }
        public string Customer { get; init; } = "";

        public string InitialCatalog { get; init; } = ""; // ✅ NEW
    }

}
