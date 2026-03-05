using System;
using System.Threading.Tasks;
using System.Windows.Input;

namespace DSD.UI
{
    /// <summary>
    /// AsyncRelayCommand
    /// ================
    /// An ICommand implementation that runs asynchronous (Task-based) code.
    ///
    /// WHY YOU NEED THIS:
    ///  - Database calls should be awaited (async) so the UI doesn't freeze.
    ///  - ICommand.Execute is 'void', so without a wrapper you end up with "async void" methods
    ///    scattered around your ViewModel (harder to test and error-handle).
    ///
    /// FEATURES:
    ///  - Prevents double-execution while the task is running (_isExecuting).
    ///  - Supports an optional CanExecute() predicate.
    ///  - Provides RaiseCanExecuteChanged() so your ViewModel can refresh button enabled state.
    ///
    /// NOTE ABOUT WPF:
    ///  - Like your RelayCommand, this hooks CanExecuteChanged into CommandManager.RequerySuggested.
    ///    That means WPF will automatically requery sometimes (focus changes, etc.).
    ///  - RaiseCanExecuteChanged() forces an immediate requery.
    /// </summary>
    public sealed class AsyncRelayCommand : ICommand
    {
        // ---------------------------------------------------------
        // Delegates supplied by the ViewModel
        // ---------------------------------------------------------
        private readonly Func<Task> _executeAsync;
        private readonly Func<bool>? _canExecute;

        // ---------------------------------------------------------
        // Execution state - blocks reentrancy (double clicks)
        // ---------------------------------------------------------
        private bool _isExecuting;

        // ---------------------------------------------------------
        // Construction
        // ---------------------------------------------------------
        public AsyncRelayCommand(Func<Task> executeAsync, Func<bool>? canExecute = null)
        {
            _executeAsync = executeAsync ?? throw new ArgumentNullException(nameof(executeAsync));
            _canExecute = canExecute;
        }

        // ---------------------------------------------------------
        // ICommand
        // ---------------------------------------------------------
        public bool CanExecute(object? parameter)
        {
            // Block execution while already running to prevent duplicate inserts, etc.
            if (_isExecuting) return false;

            // If no predicate, executable; otherwise defer to predicate.
            return _canExecute?.Invoke() ?? true;
        }

        public async void Execute(object? parameter)
        {
            // WPF calls Execute only if CanExecute returns true, but we guard anyway.
            if (!CanExecute(parameter)) return;

            try
            {
                _isExecuting = true;
                RaiseCanExecuteChanged(); // disables the bound button immediately

                await _executeAsync().ConfigureAwait(true);
            }
            finally
            {
                _isExecuting = false;
                RaiseCanExecuteChanged(); // re-enables the bound button if allowed
            }
        }

        // ---------------------------------------------------------
        // CanExecuteChanged / WPF command requery integration
        // ---------------------------------------------------------
        public event EventHandler? CanExecuteChanged
        {
            add => CommandManager.RequerySuggested += value;
            remove => CommandManager.RequerySuggested -= value;
        }

        /// <summary>
        /// Forces WPF to re-evaluate CanExecute() NOW.
        /// Call this when selection/busy state changes.
        /// </summary>
        public void RaiseCanExecuteChanged() =>
            CommandManager.InvalidateRequerySuggested();
    }
}