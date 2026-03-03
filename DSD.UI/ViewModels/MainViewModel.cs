using DSD.Common.Models;
using DSD.Common.Services;
using DSD.UI;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
namespace DSD.UI.ViewModels;

public class MainViewModel : INotifyPropertyChanged
{

    private readonly IConfiguration _config;

    //public MainViewModel(IConfiguration config)
    //{
    //    _config = config;

    //    // TEMP DIAGNOSTIC:
    //    MessageBox.Show(
    //        $"VM got config. Inbound={_config["InboundPath"] ?? "<null>"}\n" +
    //        $"Outbound={_config["OutboundPath"] ?? "<null>"}",
    //        "Config in VM");

    //}

    private readonly ISqlService _sqlService;
    public ICommand RunCommand { get; }
    public MainViewModel(ISqlService sqlService, IConfiguration config)
    {
        _sqlService = sqlService;
        _config = config;
        _selectedDirection = "Inbound";
        TableOptions.Add("ALL");
        SelectedTableOption = "ALL";

        RunCommand = new DSD.UI.RelayCommand(Run, CanRun);
    }
    private bool CanRun()
    {
        return SelectedCustomer != null &&
               !string.IsNullOrWhiteSpace(SelectedDirection) &&
               !string.IsNullOrWhiteSpace(SelectedTableOption);
    }

    private void Run()
    {
        var message =
    $@"You are about to run:

Direction: {SelectedDirection}
Customer: {SelectedCustomer?.Customer}
Group / Table: {SelectedTableOption}
Send to CIS: {(SendToCis ? "Yes" : "No")}

Do you want to continue?";

        var result = MessageBox.Show(
            message,
            "Confirm Run",
            MessageBoxButton.OKCancel,
            MessageBoxImage.Warning);

        if (result != MessageBoxResult.OK)
            return;

        ExecuteRun();   // ✅ THIS finally launches the EXE
    }
    //public MainViewModel(ISqlService sqlService)
    //{
    //    _sqlService = sqlService;

    //    // Defaults so the UI always has a stable state
    //    _selectedDirection = "Inbound";
    //    TableOptions.Add("ALL");
    //    SelectedTableOption = "ALL";
    //}

    // ----------------------------
    // Customers
    // ----------------------------
    public ObservableCollection<CustomerRow> Customers { get; } = new();

    private CustomerRow? _selectedCustomer;
    public CustomerRow? SelectedCustomer
    {
        get => _selectedCustomer;
        set
        {
            if (ReferenceEquals(_selectedCustomer, value)) return;
            _selectedCustomer = value;

            OnPropertyChanged();
            OnPropertyChanged(nameof(SelectedCustomerId));

            // ✅ Setter #1: refresh table list when customer changes
            _ = LoadTableOptionsAsync();
        }
    }

    public int? SelectedCustomerId => SelectedCustomer?.Id;

    // ----------------------------
    // Direction (Inbound/Outbound)
    // ----------------------------
    public ObservableCollection<string> Directions { get; } =
        new() { "Inbound", "Outbound" };

    private string _selectedDirection;
    public string SelectedDirection
    {
        get => _selectedDirection;
        set
        {
            if (string.Equals(_selectedDirection, value, StringComparison.Ordinal)) return;
            _selectedDirection = value;

            OnPropertyChanged();
            OnPropertyChanged(nameof(IsOutbound));

            // ✅ Setter #2: refresh table list when direction changes
            _ = LoadTableOptionsAsync();
        }
    }

    public bool IsOutbound => SelectedDirection == "Outbound";

    // ----------------------------
    // Outbound tables/groups
    // ----------------------------
    public ObservableCollection<string> TableOptions { get; } = new();

    private string? _selectedTableOption;
    public string? SelectedTableOption
    {
        get => _selectedTableOption;
        set
        {
            if (string.Equals(_selectedTableOption, value, StringComparison.Ordinal)) return;
            _selectedTableOption = value;
            OnPropertyChanged();
        }
    }

    // ----------------------------
    // Load customers (call from Window Loaded)
    // ----------------------------
    public async Task LoadCustomersAsync()
    {
        var rows = await _sqlService.GetCustomersAsync();

        Customers.Clear();
        foreach (var row in rows)
            Customers.Add(row);

        if (Customers.Count > 0)
            SelectedCustomer = Customers[0];   // triggers LoadTableOptionsAsync()

        // Ensure table options are consistent even if list is empty
        await LoadTableOptionsAsync();
    }
    private void ExecuteRun()
    {
        // Decide which exe to run
        var exeName = SelectedDirection == "Inbound"
            ? _config["InboundPath"]
    :         _config["OutboundPath"];


        // Build arguments
        var customer = SelectedCustomer?.Customer ?? "";
        var group = SelectedTableOption ?? "ALL";
        var sendToCis = SendToCis ? "Y" : "N";

        var arguments = $"{customer} {group} {sendToCis}";

        // Optional: path where the EXEs live (recommended)
        var exePath = Path.Combine(
            AppContext.BaseDirectory,
            exeName
        );

        if (!File.Exists(exePath))
        {
            MessageBox.Show(
                $"Could not find {exeName} at:\n{exePath}",
                "Executable Not Found",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
            return;
        }

        try
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = exePath,
                Arguments = arguments,
                UseShellExecute = true,
                WorkingDirectory = Path.GetDirectoryName(exePath)!,
                CreateNoWindow = false   // true if you want it hidden
            };
           

            Process.Start(startInfo);
        }
        catch (Exception ex)
        {
            MessageBox.Show(
                $"Failed to start process:\n{ex.Message}",
                "Execution Error",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
        }
    }
    // ----------------------------
    // Populate TableOptions:
    // - Always include "ALL"
    // - If Outbound AND selected customer has InitialCatalog, add DB table names
    // ----------------------------
    private bool _sendToCis = true; // default = Yes
    public bool SendToCis
    {
        get => _sendToCis;
        set
        {
            if (_sendToCis == value) return;
            _sendToCis = value;
            OnPropertyChanged();
        }
    }

    private async Task LoadTableOptionsAsync()
    {
        TableOptions.Clear();
        TableOptions.Add("ALL");

        if (IsOutbound &&
            SelectedCustomer is not null &&
            !string.IsNullOrWhiteSpace(SelectedCustomer.InitialCatalog))
        {
            var tables = await _sqlService.GetOutboundTableNamesAsync(SelectedCustomer.InitialCatalog);

            foreach (var t in tables)
            {
                if (string.IsNullOrWhiteSpace(t)) continue;
                if (string.Equals(t, "ALL", StringComparison.OrdinalIgnoreCase)) continue;
                if (!TableOptions.Contains(t)) TableOptions.Add(t);
            }
        }

        // Always reset to ALL when list changes
        SelectedTableOption = "ALL";
        OnPropertyChanged(nameof(TableOptions));
        OnPropertyChanged(nameof(SelectedTableOption));
    }

    // ----------------------------
    // INotifyPropertyChanged
    // ----------------------------
    public event PropertyChangedEventHandler? PropertyChanged;
    private void OnPropertyChanged([CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}