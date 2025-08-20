using System.Windows.Input;

namespace TwitchSongAdmin.Utils;

public class AsyncCommand : ICommand
{
    private readonly Func<object?, Task> _exec;
    private readonly Func<object?, bool>? _can;
    private bool _busy;
    public AsyncCommand(Func<object?, Task> exec, Func<object?, bool>? can = null)
    { _exec = exec; _can = can; }
    public event EventHandler? CanExecuteChanged;
    public bool CanExecute(object? p) => !_busy && (_can?.Invoke(p) ?? true);
    public async void Execute(object? p)
    {
        if (_busy) return; _busy = true; CanExecuteChanged?.Invoke(this, EventArgs.Empty);
        try { await _exec(p); }
        finally { _busy = false; CanExecuteChanged?.Invoke(this, EventArgs.Empty); }
    }
}