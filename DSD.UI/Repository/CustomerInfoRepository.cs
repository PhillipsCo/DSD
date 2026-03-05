using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

public class CustomerInfoRepository
{
    private readonly IConfiguration _config;
    private const string ConnectionStringName = "CustomerConnectionDB";

    public CustomerInfoRepository(IConfiguration config) => _config = config;

    private string GetConnectionString() =>
        _config.GetConnectionString(ConnectionStringName)
        ?? throw new InvalidOperationException($"Missing connection string '{ConnectionStringName}'.");

    public async Task<CustomerInfoRow?> GetByCustomerAsync(string customer)
    {
        var sql = @"
SELECT
    Customer, Url, Grant_Type, Client_ID, Client_Secret, Scope, RootUrl,
    ftpHost, ftpUser, ftpPass, ftpRemoteFilePath, ftpLocalFilePath,
    DataSource, UserID, Password, InitialCatalog, DayOffset,
    email_tenantId, email_clientId, email_secret, email_sender, email_recipient,
    PROD, HISTORY_DAYS
FROM dbo.DSD_CustomerInfo
WHERE Customer = @Customer;";

        using var conn = new SqlConnection(GetConnectionString());
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@Customer", customer);

        await conn.OpenAsync();
        using var rdr = await cmd.ExecuteReaderAsync();

        if (!await rdr.ReadAsync()) return null;

        return new CustomerInfoRow
        {
            Customer = rdr["Customer"] as string,
            Url = rdr["Url"] as string,
            Grant_Type = rdr["Grant_Type"] as string,
            Client_ID = rdr["Client_ID"] as string,
            Client_Secret = rdr["Client_Secret"] as string,
            Scope = rdr["Scope"] as string,
            RootUrl = rdr["RootUrl"] as string,
            ftpHost = rdr["ftpHost"] as string,
            ftpUser = rdr["ftpUser"] as string,
            ftpPass = rdr["ftpPass"] as string,
            ftpRemoteFilePath = rdr["ftpRemoteFilePath"] as string,
            ftpLocalFilePath = rdr["ftpLocalFilePath"] as string,
            DataSource = rdr["DataSource"] as string,
            UserID = rdr["UserID"] as string,
            Password = rdr["Password"] as string,
            InitialCatalog = rdr["InitialCatalog"] as string,
            DayOffset = rdr["DayOffset"] == DBNull.Value ? null : (int?)Convert.ToInt32(rdr["DayOffset"]),
            email_tenantId = rdr["email_tenantId"] as string,
            email_clientId = rdr["email_clientId"] as string,
            email_secret = rdr["email_secret"] as string,
            email_sender = rdr["email_sender"] as string,
            email_recipient = rdr["email_recipient"] as string,
            PROD = rdr["PROD"] as string,
            HISTORY_DAYS = rdr["HISTORY_DAYS"] == DBNull.Value ? null : (int?)Convert.ToInt32(rdr["HISTORY_DAYS"]),
        };
    }

    public async Task UpdateAsync(CustomerInfoRow row)
    {
        var sql = @"
UPDATE dbo.DSD_CustomerInfo
SET
    Url=@Url, Grant_Type=@Grant_Type, Client_ID=@Client_ID, Client_Secret=@Client_Secret,
    Scope=@Scope, RootUrl=@RootUrl,
    ftpHost=@ftpHost, ftpUser=@ftpUser, ftpPass=@ftpPass,
    ftpRemoteFilePath=@ftpRemoteFilePath, ftpLocalFilePath=@ftpLocalFilePath,
    DataSource=@DataSource, UserID=@UserID, Password=@Password, InitialCatalog=@InitialCatalog,
    DayOffset=@DayOffset,
    email_tenantId=@email_tenantId, email_clientId=@email_clientId, email_secret=@email_secret,
    email_sender=@email_sender, email_recipient=@email_recipient,
    PROD=@PROD, HISTORY_DAYS=@HISTORY_DAYS
WHERE Customer=@Customer;";

        using var conn = new SqlConnection(GetConnectionString());
        using var cmd = new SqlCommand(sql, conn);

        cmd.Parameters.AddWithValue("@Customer", row.Customer ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Url", row.Url ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Grant_Type", row.Grant_Type ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Client_ID", row.Client_ID ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Client_Secret", row.Client_Secret ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Scope", row.Scope ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@RootUrl", row.RootUrl ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@ftpHost", row.ftpHost ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@ftpUser", row.ftpUser ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@ftpPass", row.ftpPass ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@ftpRemoteFilePath", row.ftpRemoteFilePath ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@ftpLocalFilePath", row.ftpLocalFilePath ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@DataSource", row.DataSource ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@UserID", row.UserID ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Password", row.Password ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@InitialCatalog", row.InitialCatalog ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@DayOffset", row.DayOffset ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@email_tenantId", row.email_tenantId ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@email_clientId", row.email_clientId ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@email_secret", row.email_secret ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@email_sender", row.email_sender ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@email_recipient", row.email_recipient ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@PROD", row.PROD ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@HISTORY_DAYS", row.HISTORY_DAYS ?? (object)DBNull.Value);

        await conn.OpenAsync();
        await cmd.ExecuteNonQueryAsync();
    }
}