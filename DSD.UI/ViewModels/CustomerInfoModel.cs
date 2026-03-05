using DSD.Common.Models;
using Microsoft.Extensions.Configuration;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;            // ✅ REQUIRED (Task)
using System.Windows;
using System.Windows.Input;

// ✅ Make sure this matches your project's namespace
namespace DSD.UI.ViewModels
{
    /// <summary>
    /// CustomerInfoViewModel
    /// ---------------------
    /// Owns the "Admin - Customer Info" tab:
    ///  - Loads a single CustomerInfoRow for the selected customer
    ///  - Exposes Current (bindable to TextBoxes)
    ///  - Exposes Reload + Update (Save) commands
    /// </summary>
    public class CustomerInfoViewModel : INotifyPropertyChanged
    {
        private readonly CustomerInfoRepository _repo;

        // -----------------------------------------
        // Busy state (disables commands, optional UI)
        // -----------------------------------------
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

        // -----------------------------------------
        // Context from MainViewModel
        // -----------------------------------------
        private CustomerRow? _customer;

        // -----------------------------------------
        // The single editable row bound to the UI
        // -----------------------------------------
        private CustomerInfoRow? _current;
        public CustomerInfoRow? Current
        {
            get => _current;
            private set
            {
                if (ReferenceEquals(_current, value)) return;
                _current = value;
                OnPropertyChanged();
                RaiseCommandCanExecuteChanged();
            }
        }

        // -----------------------------------------
        // Commands exposed to the UI
        // -----------------------------------------
        public ICommand ReloadCommand { get; }

        // Your existing name:
        public ICommand SaveCommand { get; }

        // ✅ Alias so the button can say "Update" and bind cleanly:
        public ICommand UpdateCommand => SaveCommand;

        public CustomerInfoViewModel(IConfiguration config)
        {
            _repo = new CustomerInfoRepository(config);

            // Reload only needs a customer selected
            ReloadCommand = new DSD.UI.AsyncRelayCommand(LoadAsync, CanLoad);

            // Save/Update requires a loaded Current row
            SaveCommand = new DSD.UI.AsyncRelayCommand(SaveAsync, CanSave);
        }

        /// <summary>
        /// Called by MainViewModel whenever SelectedCustomer changes.
        /// Loads the CustomerInfoRow for that customer and sets Current.
        /// </summary>
        public async void SetCustomer(CustomerRow? customer)
        {
            _customer = customer;
            await LoadAsync();
        }

        /// <summary>
        /// Loads Current from the database (or creates a blank row if none exists).
        /// </summary>
        public async Task LoadAsync()
        {
            Current = null;

            if (_customer == null || string.IsNullOrWhiteSpace(_customer.Customer))
                return;

            try
            {
                IsBusy = true;

                // If not found, create a new row so the form still populates.
                Current = await _repo.GetByCustomerAsync(_customer.Customer)
                          ?? new CustomerInfoRow { Customer = _customer.Customer };
            }
            finally
            {
                IsBusy = false;
            }
        }

        /// <summary>
        /// Saves changes for Current back to the database.
        /// (Repo uses UPDATE; if you need INSERT for new customers, we can add that.)
        /// </summary>
        private async Task SaveAsync()
        {
            if (Current == null) return;

            if (MessageBox.Show("Save changes to Customer Info?",
                                "Confirm Update",
                                MessageBoxButton.YesNo,
                                MessageBoxImage.Question) != MessageBoxResult.Yes)
                return;

            try
            {
                IsBusy = true;
                await _repo.UpdateAsync(Current);
            }
            finally
            {
                IsBusy = false;
            }
        }

        // -----------------------------------------
        // CanExecute logic (kept separate for clarity)
        // -----------------------------------------
        private bool CanLoad() =>
            !IsBusy &&
            _customer != null &&
            !string.IsNullOrWhiteSpace(_customer.Customer);

        private bool CanSave() =>
            !IsBusy &&
            Current != null &&
            !string.IsNullOrWhiteSpace(Current.Customer);

        // -----------------------------------------
        // Notify commands when state changes
        // -----------------------------------------
        private void RaiseCommandCanExecuteChanged()
        {
            if (ReloadCommand is DSD.UI.AsyncRelayCommand r) r.RaiseCanExecuteChanged();
            if (SaveCommand is DSD.UI.AsyncRelayCommand s) s.RaiseCanExecuteChanged();
        }

        // -----------------------------------------
        // INotifyPropertyChanged
        // -----------------------------------------
        public event PropertyChangedEventHandler? PropertyChanged;

        private void OnPropertyChanged([CallerMemberName] string? name = null) =>
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}