using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

using DSD.Common.Models;
using DSD.UI.Models;
using DSD.UI.Repositories;
using Microsoft.Extensions.Configuration;

namespace DSD.UI.ViewModels;

public sealed class ApiListGridViewModel : INotifyPropertyChanged
{
    private readonly ApiListRepository _repo;

    public ObservableCollection<ApiListRow> Items { get; } = new();

    private ApiListRow? _selectedItem;
    public ApiListRow? SelectedItem
    {
        get => _selectedItem;
        set
        {
            if (!Equals(_selectedItem, value))
            {
                _selectedItem = value;
                OnPropertyChanged();
                RaiseCommandCanExecuteChanged();
            }
        }
    }

    private string? _initialCatalog;
    private string? _tableOption;

    // -----------------------------------------------------
    // Commands
    // -----------------------------------------------------
    public ICommand UpdateCommand { get; }
    public ICommand CopyCommand { get; }
    public ICommand DeleteCommand { get; }

    public ApiListGridViewModel(IConfiguration config)
    {
        _repo = new ApiListRepository(config);

        UpdateCommand = new AsyncRelayCommand(UpdateAsync, () => SelectedItem != null);
        CopyCommand = new AsyncRelayCommand(CopyAsync, () => SelectedItem != null);
        DeleteCommand = new AsyncRelayCommand(DeleteAsync, () => SelectedItem != null);
    }

    // -----------------------------------------------------
    // Context setters (called by MainViewModel)
    // -----------------------------------------------------

    public void SetInitialCatalog(string? initialCatalog)
    {
        _initialCatalog = initialCatalog;
        _ = ReloadAsync();
    }

    public void SetTableOption(string? tableOption)
    {
        _tableOption = tableOption;
        _ = ReloadAsync();
    }

    // -----------------------------------------------------
    // Load
    // -----------------------------------------------------

    private async Task ReloadAsync()
    {
        if (string.IsNullOrWhiteSpace(_initialCatalog))
        {
            Items.Clear();
            return;
        }

        try
        {
            Debug.WriteLine($"[ApiList] Reloading for catalog={_initialCatalog}");

            var rows =
                string.IsNullOrWhiteSpace(_tableOption) || _tableOption == "ALL"
                    ? await _repo.GetAllAsync(_initialCatalog)
                    : await _repo.GetByTableNameAsync(_initialCatalog, _tableOption);

            Items.Clear();
            foreach (var r in rows)
                Items.Add(r);
        }
        catch (Exception ex)
        {
            MessageBox.Show(ex.ToString(), "API List Load Error");
        }
    }

    // -----------------------------------------------------
    // CRUD
    // -----------------------------------------------------

    private async Task UpdateAsync()
    {
        if (SelectedItem == null || string.IsNullOrWhiteSpace(_initialCatalog))
            return;

        var result = MessageBox.Show(
            "Save changes to this API entry?",
            "Confirm Update",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes)
            return;

        await _repo.UpdateAsync(_initialCatalog, SelectedItem);
    }

    private async Task CopyAsync()
    {
        if (SelectedItem == null || string.IsNullOrWhiteSpace(_initialCatalog))
            return;

        var result = MessageBox.Show(
            $"Create a copy of API '{SelectedItem.API_NAME}'?",
            "Confirm Copy",
            MessageBoxButton.YesNo,
            MessageBoxImage.Question);

        if (result != MessageBoxResult.Yes)
            return;

        var copy = new ApiListRow
        {
            TABLE_NAME = SelectedItem.TABLE_NAME,
            API_NAME = SelectedItem.API_NAME + "_COPY",
            FILTER = SelectedItem.FILTER,
            BATCHSIZE = SelectedItem.BATCHSIZE,
            DIR = SelectedItem.DIR,
            RUNGROUP = SelectedItem.RUNGROUP,
            ENDPOINT = SelectedItem.ENDPOINT
        };

        await _repo.InsertAsync(_initialCatalog, copy);

        Items.Add(copy);
        SelectedItem = copy;
    }

    private async Task DeleteAsync()
    {
        if (SelectedItem == null || string.IsNullOrWhiteSpace(_initialCatalog))
            return;

        var toDelete = SelectedItem;

        if (MessageBox.Show(
                $"Delete API '{toDelete.API_NAME}'?",
                "Confirm Delete",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning)
            != MessageBoxResult.Yes)
            return;

        await _repo.DeleteAsync(_initialCatalog, toDelete.TABLE_NAME, toDelete.API_NAME);

        Items.Remove(toDelete);
        SelectedItem = null;
    }

    // -----------------------------------------------------
    // Helpers
    // -----------------------------------------------------

    private void RaiseCommandCanExecuteChanged()
    {
        if (UpdateCommand is AsyncRelayCommand u) u.RaiseCanExecuteChanged();
        if (CopyCommand is AsyncRelayCommand c) c.RaiseCanExecuteChanged();
        if (DeleteCommand is AsyncRelayCommand d) d.RaiseCanExecuteChanged();
    }

    private CustomerRow? _customer;

    public void SetCustomer(CustomerRow? customer)
    {
        _customer = customer;

        // Optional: clear selection on customer change
        SelectedItem = null;

        // Reload will only succeed once InitialCatalog is provided via SetInitialCatalog(...)
        _ = ReloadAsync();
    }

    public event PropertyChangedEventHandler? PropertyChanged;
    private void OnPropertyChanged([CallerMemberName] string? name = null)
        => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}