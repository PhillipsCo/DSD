using System;
using System.Windows.Input;

namespace DSD.UI
{
    /// <summary>
    /// RelayCommand
    /// ===========
    ///
    /// A lightweight ICommand implementation for MVVM.
    ///
    /// WHY THIS EXISTS:
    ///  - WPF Buttons/MenuItems/etc. bind to ICommand
    ///  - ICommand provides Execute() (what to do) and CanExecute() (whether it's allowed)
    ///  - When CanExecute() changes, the UI must be notified so it can enable/disable controls
    ///
    /// IMPORTANT WPF DETAIL:
    ///  - WPF does NOT continuously call CanExecute().
    ///  - It calls CanExecute() when it believes something *might* have changed,
    ///    and it calls it again when ICommand raises CanExecuteChanged.
    ///
    /// This implementation uses CommandManager.RequerySuggested, which is WPF's global
    /// "something changed, requery commands now" event.
    /// That means:
    ///  - Your command automatically participates in WPF's requery cycle
    ///  - CanExecute() gets re-evaluated on focus changes, mouse input, etc.
    ///
    /// BUT:
    ///  - Sometimes you need the UI to refresh immediately (ex: selection changed in a DataGrid)
    ///  - That's why we add RaiseCanExecuteChanged(), which forces WPF to requery right now.
    /// </summary>
    public class RelayCommand : ICommand
    {
        // =========================================================
        // 1) Delegates supplied by the ViewModel
        // =========================================================

        /// <summary>
        /// The action to execute when the command is invoked (e.g., when user clicks the button).
        /// </summary>
        private readonly Action _execute;

        /// <summary>
        /// Optional guard that determines whether the command is currently allowed to run.
        /// If null, the command is always executable.
        /// </summary>
        private readonly Func<bool>? _canExecute;

        // =========================================================
        // 2) Construction
        // =========================================================

        /// <summary>
        /// Creates a new RelayCommand.
        /// </summary>
        /// <param name="execute">The action to run when Execute() is called.</param>
        /// <param name="canExecute">
        /// Optional predicate used by CanExecute().
        /// Return true to enable the command; false to disable it.
        /// </param>
        public RelayCommand(Action execute, Func<bool>? canExecute = null)
        {
            // Fail fast: a command without an execute delegate is meaningless.
            _execute = execute ?? throw new ArgumentNullException(nameof(execute));
            _canExecute = canExecute;
        }

        // =========================================================
        // 3) ICommand implementation
        // =========================================================

        /// <summary>
        /// WPF calls this to determine whether bound UI elements should be enabled.
        /// </summary>
        public bool CanExecute(object? parameter)
        {
            // If no guard was provided, allow execution.
            // Otherwise, defer to the ViewModel's guard logic.
            return _canExecute?.Invoke() ?? true;
        }

        /// <summary>
        /// Called when the user triggers the command (click, keyboard shortcut, etc.).
        /// WPF guarantees Execute() is only called when CanExecute() == true.
        /// </summary>
        public void Execute(object? parameter)
        {
            _execute();
        }

        /// <summary>
        /// Raised when something changes that affects whether the command can execute.
        ///
        /// This implementation delegates subscriptions to CommandManager.RequerySuggested.
        /// That means WPF will automatically re-check CanExecute() during its normal UI cycles.
        /// </summary>
        public event EventHandler? CanExecuteChanged
        {
            add => CommandManager.RequerySuggested += value;
            remove => CommandManager.RequerySuggested -= value;
        }

        // =========================================================
        // 4) Manual CanExecute refresh (the important addition)
        // =========================================================

        /// <summary>
        /// Forces WPF to requery CanExecute() for commands *immediately*.
        ///
        /// WHEN TO CALL THIS:
        ///  - When a ViewModel property changes that affects CanExecute()
        ///    (SelectedItem, IsBusy, form validation, etc.)
        ///
        /// WHY THIS WORKS:
        ///  - Since CanExecuteChanged is tied to CommandManager.RequerySuggested,
        ///    invalidating the requery causes WPF to refresh command enablement.
        ///
        /// NOTE:
        ///  - This triggers a global command refresh (all commands),
        ///    so avoid calling it in very tight loops.
        /// </summary>
        public void RaiseCanExecuteChanged()
        {
            CommandManager.InvalidateRequerySuggested();
        }
    }
}