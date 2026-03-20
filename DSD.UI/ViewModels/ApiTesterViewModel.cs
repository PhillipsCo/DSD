using DSD.Common.Models;
using DSD.Common.Services;
using System;
using System.ComponentModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Windows.Input;

namespace DSD.UI.ViewModels
{
    public class ApiTesterViewModel : INotifyPropertyChanged
    {
        private readonly ApiTesterRepository _repo;
        private readonly CustomerInfoViewModel _customerInfoVm;

        public ICommand GetTokenCommand { get; }

        private TokenInfo? _token;
        public TokenInfo? Token
        {
            get => _token;
            private set
            {
                _token = value;
                OnPropertyChanged();
                // No need to raise ExpiresAtDisplay here if ExpiresAt is set separately,
                // but it doesn't hurt.
                OnPropertyChanged(nameof(ExpiresAtDisplay));
            }
        }

        private DateTime? _expiresAt;
        public DateTime? ExpiresAt
        {
            get => _expiresAt;
            private set
            {
                _expiresAt = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(ExpiresAtDisplay)); // ✅ REQUIRED for computed display
            }
        }

        // ✅ Keep ONLY ONE ExpiresAtDisplay
        public string ExpiresAtDisplay =>
            ExpiresAt?.ToString("g") ?? string.Empty;

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

        public ApiTesterViewModel(ApiTesterRepository repo, CustomerInfoViewModel customerInfoVm)
        {
            _repo = repo;
            _customerInfoVm = customerInfoVm;

            GetTokenCommand = new DSD.UI.AsyncRelayCommand(GetTokenAsync, CanGetToken);
        }

        private bool CanGetToken() =>
            !IsBusy &&
            _customerInfoVm.Current != null &&
            !string.IsNullOrWhiteSpace(_customerInfoVm.Current.Url) &&
            !string.IsNullOrWhiteSpace(_customerInfoVm.Current.Client_ID) &&
            !string.IsNullOrWhiteSpace(_customerInfoVm.Current.Client_Secret) &&
            !string.IsNullOrWhiteSpace(_customerInfoVm.Current.Grant_Type);

        private async Task GetTokenAsync()
        {
            var row = _customerInfoVm.Current!;
            try
            {
                IsBusy = true;

                Token = await _repo.GetAccessTokenAsync(
                    row.Url,
                    row.Grant_Type,
                    row.Client_ID,
                    row.Client_Secret,
                    row.Scope
                );

                // ✅ Compute the actual expiration timestamp
                // Prefer expires_in, fall back to ext_expires_in
                var secondsText = Token?.expires_in;
                if (string.IsNullOrWhiteSpace(secondsText))
                    secondsText = Token?.ext_expires_in;

                if (int.TryParse(secondsText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds))
                {
                    ExpiresAt = DateTime.Now.AddSeconds(seconds);
                }
                else
                {
                    ExpiresAt = null;
                }
            }
            finally
            {
                IsBusy = false;
            }
        }

        private void RaiseCommandCanExecuteChanged()
        {
            if (GetTokenCommand is DSD.UI.AsyncRelayCommand cmd)
                cmd.RaiseCanExecuteChanged();
        }

        public event PropertyChangedEventHandler? PropertyChanged;

        private void OnPropertyChanged([CallerMemberName] string? name = null) =>
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}