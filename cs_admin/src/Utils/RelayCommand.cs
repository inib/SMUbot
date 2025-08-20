using System.Windows.Input;

namespace TwitchSongAdmin.Utils;

public class RelayCommand : ICommand
{
    private readonly Action<object?> _exec;
    private readonly Func<object?, bool>? _can;
    public RelayCommand(Action<object?> exec, Func<object?, bool>? can = null)
    { _exec = exec; _can = can; }
    public event EventHandler? CanExecuteChanged;
    public bool CanExecute(object? p) => _can?.Invoke(p) ?? true;
    public void Execute(object? p) => _exec(p);
    public void RaiseCanExecuteChanged() => CanExecuteChanged?.Invoke(this, EventArgs.Empty);
}