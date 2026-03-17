using Microsoft.Data.SqlClient;
using DSD.UI.Models;

public static class CustomerConnectionBuilder
{
    public static string Build(CustomerInfoRow customer)
    {
        if (customer == null)
            throw new ArgumentNullException(nameof(customer));

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = customer.DataSource,
            UserID = customer.UserID,
            Password = customer.Password,
            InitialCatalog = customer.InitialCatalog,

            // sensible defaults
            Encrypt = true,
            TrustServerCertificate = true,
            MultipleActiveResultSets = true
        };

        return builder.ConnectionString;
    }
}