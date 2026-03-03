using DSD.Common.Models;

namespace DSD.Common.Services;

public interface ISqlService
{
    Task<List<CustomerRow>> GetCustomersAsync();
    Task<List<string>> GetOutboundTableNamesAsync(string catalog);
    // Add more as you need them later:
    // Task<AccessInfo> GetAccessInfoAsync(string customerCode);
    // Task<List<TableApiName>> GetApiListAsync(string db, string group, string dir);
}