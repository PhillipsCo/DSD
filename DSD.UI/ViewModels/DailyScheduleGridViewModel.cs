using DSD.Common.Models;
using DSD.UI.Models;
using DSD.UI.Repositories;

using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DSD.UI.ViewModels;

/// <summary>
/// DailyScheduleGridViewModel
/// =========================
///
/// Owns everything related to the "Admin - Daily Schedule" grid:
///  - Grid data (Items / SelectedItem)
///  - Copy / Update / Delete commands
///  - SQL for this grid only
///
/// MainViewModel supplies customer context only.
/// </summary>
public class DailyScheduleGridViewModel : INotifyPropertyChanged
{
    // =========================================================
    // 0) Database constants
    // =========================================================
    private const string TableName = "dbo.DSD_Job_Executables";
    private const string PrimaryKeyColumn = "jobId";
    private const string ConnectionStringName = "CustomerConnectionDB";

    // =========================================================
    // 1) Dependencies
    // =========================================================
    private readonly IConfiguration _config;
    private readonly DailyScheduleRepository _repo;

    // =========================================================
    // 2) UI State
    // =========================================================
    private bool _isBusy;
    public bool IsBusy
    {
        get => _isBusy;
        private set
        {
            if (_isBusy == value) return;
            _isBusy = value;
            OnPropertyChanged();
            RaiseCommandCanExecuteChanged();
        }
    }

    private CustomerRow? _customer;

    // =========================================================
    // 3) Grid data
    // =========================================================
    public ObservableCollection<DailyScheduleRow> Items { get; } = new();

    private DailyScheduleRow? _selectedItem;
    public DailyScheduleRow? SelectedItem
    {
        get => _selectedItem;
        set
        {
            if (ReferenceEquals(_selectedItem, value)) return;
            _selectedItem = value;
            OnPropertyChanged();
            RaiseCommandCanExecuteChanged();
        }
    }

    // =========================================================
    // 4) Commands
    // =========================================================
    public ICommand CopyCommand { get; }
    public ICommand UpdateCommand { get; }
    public ICommand DeleteCommand { get; }

    // =========================================================
    // 5) Constructor  ✅ THIS IS WHERE INITIALIZATION BELONGS
    // =========================================================
    public DailyScheduleGridViewModel(IConfiguration config)
    {
        _config = config;

        // ✅ Correct: initialize repository AFTER config is assigned
        _repo = new DailyScheduleRepository(_config);

        CopyCommand = new DSD.UI.AsyncRelayCommand(CopyAsync, CanModify);
        UpdateCommand = new DSD.UI.AsyncRelayCommand(UpdateAsync, CanModify);
        DeleteCommand = new DSD.UI.AsyncRelayCommand(DeleteAsync, CanModify);
    }

    // =========================================================
    // 6) Public API (called by MainViewModel)
    // =========================================================
    public async void SetCustomer(CustomerRow? customer)
    {
        _customer = customer;
        await LoadAsync();
    }

    public async Task LoadAsync()
    {
        Items.Clear();
        SelectedItem = null;

        if (_customer == null || string.IsNullOrWhiteSpace(_customer.Customer))
            return;

        var rows = await _repo.GetByCustomerAsync(_customer.Customer);

        foreach (var row in rows)
            Items.Add(row);

        RaiseCommandCanExecuteChanged();
    }

    // =========================================================
    // 7) CanExecute
    // =========================================================
    private bool CanModify() =>
        !IsBusy && _customer != null && SelectedItem != null;

    // =========================================================
    // 8) COPY
    // =========================================================
    private async Task CopyAsync()
    {
        if (SelectedItem == null) return;

        if (MessageBox.Show("Do you want to copy this record?",
                            "Copy Record",
                            MessageBoxButton.YesNo,
                            MessageBoxImage.Question)
            != MessageBoxResult.Yes)
            return;

        try
        {
            IsBusy = true;
            int newId = await CopyRowInDatabaseAsync(SelectedItem);
            await ReloadAndReselectAsync(newId);
        }
        finally
        {
            IsBusy = false;
        }
    }

    // =========================================================
    // 9) UPDATE
    // =========================================================
    private async Task UpdateAsync()
    {
        if (SelectedItem == null) return;

        if (MessageBox.Show("Save changes to this record?",
                            "Confirm Update",
                            MessageBoxButton.YesNo,
                            MessageBoxImage.Question)
            != MessageBoxResult.Yes)
            return;

        try
        {
            IsBusy = true;
            int id = SelectedItem.jobId;
            await UpdateRowInDatabaseAsync(SelectedItem);
            await ReloadAndReselectAsync(id);
        }
        finally
        {
            IsBusy = false;
        }
    }

    // =========================================================
    // 10) DELETE
    // =========================================================
    private async Task DeleteAsync()
    {
        if (SelectedItem == null) return;

        if (MessageBox.Show("Are you sure you want to delete this record?",
                            "Confirm Delete",
                            MessageBoxButton.YesNo,
                            MessageBoxImage.Warning)
            != MessageBoxResult.Yes)
            return;

        try
        {
            IsBusy = true;
            int index = Items.IndexOf(SelectedItem);
            await DeleteRowInDatabaseAsync(SelectedItem);
            await LoadAsync();

            if (Items.Count > 0)
                SelectedItem = Items[Math.Min(index, Items.Count - 1)];
        }
        finally
        {
            IsBusy = false;
        }
    }

    // =========================================================
    // 11) Database helpers
    // =========================================================
    private string GetConnectionString() =>
        _config.GetConnectionString(ConnectionStringName)
        ?? throw new InvalidOperationException($"Missing connection string '{ConnectionStringName}'.");

    private async Task<int> CopyRowInDatabaseAsync(DailyScheduleRow row)
    {
        string sql = $@"
INSERT INTO {TableName}
(
    Cust, Job, TargetComputer, ScheduleTime,
    ExecuteWeekDays, IsActive, RUNGROUP, SendCIS
)
SELECT
    Cust, Job, TargetComputer, ScheduleTime,
    ExecuteWeekDays, IsActive, RUNGROUP, SendCIS
FROM {TableName}
WHERE {PrimaryKeyColumn} = @Id;

SELECT CAST(SCOPE_IDENTITY() AS int);";

        using var conn = new SqlConnection(GetConnectionString());
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@Id", row.jobId);

        await conn.OpenAsync();
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private async Task UpdateRowInDatabaseAsync(DailyScheduleRow row)
    {
        string sql = $@"
UPDATE {TableName}
SET
    Cust=@Cust, Job=@Job, TargetComputer=@TargetComputer,
    ScheduleTime=@ScheduleTime, ExecuteWeekDays=@ExecuteWeekDays,
    IsActive=@IsActive, RUNGROUP=@RUNGROUP, SendCIS=@SendCIS
WHERE {PrimaryKeyColumn}=@Id;";

        using var conn = new SqlConnection(GetConnectionString());
        using var cmd = new SqlCommand(sql, conn);

        cmd.Parameters.AddWithValue("@Id", row.jobId);
        cmd.Parameters.AddWithValue("@Cust", row.Cust);
        cmd.Parameters.AddWithValue("@Job", row.Job);
        cmd.Parameters.AddWithValue("@TargetComputer", row.TargetComputer);
        cmd.Parameters.AddWithValue("@ScheduleTime", row.ScheduleTime);
        cmd.Parameters.AddWithValue("@ExecuteWeekDays", row.ExecuteWeekDays);
        cmd.Parameters.AddWithValue("@IsActive", row.IsActive);
        cmd.Parameters.AddWithValue("@RUNGROUP", row.RUNGROUP);
        cmd.Parameters.AddWithValue("@SendCIS", row.SendCIS);

        await conn.OpenAsync();
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task DeleteRowInDatabaseAsync(DailyScheduleRow row)
    {
        string sql = $@"
DELETE FROM {TableName}
WHERE {PrimaryKeyColumn} = @Id;";

        using var conn = new SqlConnection(GetConnectionString());
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@Id", row.jobId);

        await conn.OpenAsync();
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task ReloadAndReselectAsync(int id)
    {
        await LoadAsync();
        SelectedItem = Items.FirstOrDefault(x => x.jobId == id);
    }

    // =========================================================
    // 12) Command refresh
    // =========================================================
    private void RaiseCommandCanExecuteChanged()
    {
        if (CopyCommand is DSD.UI.AsyncRelayCommand c) c.RaiseCanExecuteChanged();
        if (UpdateCommand is DSD.UI.AsyncRelayCommand u) u.RaiseCanExecuteChanged();
        if (DeleteCommand is DSD.UI.AsyncRelayCommand d) d.RaiseCanExecuteChanged();
    }

    // =========================================================
    // 13) INotifyPropertyChanged
    // =========================================================
    public event PropertyChangedEventHandler? PropertyChanged;

    private void OnPropertyChanged([CallerMemberName] string? name = null) =>
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}