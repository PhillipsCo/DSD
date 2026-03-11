using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

using DSD.UI.Models;
using DSD.UI.Repositories;

namespace DSD.UI.ViewModels
{
    public sealed class ApiListViewModel : INotifyPropertyChanged
    {
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
                    _saveCommand.RaiseCanExecuteChanged();
                    _copyCommand.RaiseCanExecuteChanged();
                    _deleteCommand.RaiseCanExecuteChanged();
                }
            }
        }

        private readonly ApiListRepository _repo;

        private readonly AsyncRelayCommand _loadCommand;
        private readonly AsyncRelayCommand _saveCommand;
        private readonly AsyncRelayCommand _copyCommand;
        private readonly AsyncRelayCommand _deleteCommand;

        public ICommand LoadCommand => _loadCommand;
        public ICommand SaveCommand => _saveCommand;
        public ICommand CopyCommand => _copyCommand;
        public ICommand DeleteCommand => _deleteCommand;

        public ApiListViewModel(ApiListRepository repo)
        {
            _repo = repo;

            _loadCommand = new AsyncRelayCommand(LoadAsync);
            _saveCommand = new AsyncRelayCommand(SaveSelectedAsync, () => SelectedItem != null);
            _copyCommand = new AsyncRelayCommand(CopySelectedAsync, () => SelectedItem != null);
            _deleteCommand = new AsyncRelayCommand(DeleteSelectedAsync, () => SelectedItem != null);
        }

        public async Task LoadAsync()
        {
            Items.Clear();
            var rows = await _repo.GetAllAsync();
            foreach (var r in rows) Items.Add(r);
        }

        private async Task SaveSelectedAsync()
        {
            if (SelectedItem == null) return;

            // Update existing row (based on TABLE_NAME + API_NAME key)
            int affected = await _repo.UpdateAsync(SelectedItem);
            if (affected == 0)
            {
                MessageBox.Show("No rows updated. The record may not exist (key mismatch).");
            }
        }

        private async Task CopySelectedAsync()
        {
            if (SelectedItem == null) return;

            var copy = new ApiListRow
            {
                TABLE_NAME = SelectedItem.TABLE_NAME,
                API_NAME = SelectedItem.API_NAME + "_COPY", // avoid immediate key collision
                FILTER = SelectedItem.FILTER,
                BATCHSIZE = SelectedItem.BATCHSIZE,
                DIR = SelectedItem.DIR,
                RUNGROUP = SelectedItem.RUNGROUP,
                ENDPOINT = SelectedItem.ENDPOINT
            };

            // Insert into DB first; if it succeeds, add to collection
            await _repo.InsertAsync(copy);

            Items.Add(copy);
            SelectedItem = copy;
        }

        private async Task DeleteSelectedAsync()
        {
            if (SelectedItem == null) return;

            var toRemove = SelectedItem;

            // Delete from DB first
            await _repo.DeleteAsync(toRemove.TABLE_NAME, toRemove.API_NAME);

            // Then remove from UI
            SelectedItem = null;
            Items.Remove(toRemove);
        }

        public event PropertyChangedEventHandler? PropertyChanged;
        private void OnPropertyChanged([CallerMemberName] string? name = null)
            => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}
