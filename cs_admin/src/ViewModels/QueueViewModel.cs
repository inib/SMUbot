using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows.Input;
using TwitchSongAdmin.Models;
using TwitchSongAdmin.Services;
using TwitchSongAdmin.Utils;
using System.Windows;
using System.Windows.Threading;

namespace TwitchSongAdmin.ViewModels;

public class QueueViewModel : INotifyPropertyChanged
{
    private readonly ApiClient _api;
    private readonly SseClient _sse;
    private int _channelId;
    private string _status = "Ready";
    private readonly Dispatcher _ui = Application.Current?.Dispatcher ?? Dispatcher.CurrentDispatcher;

    public ObservableCollection<QueueItem> Items { get; } = new();
    private QueueItem? _selected;
    public QueueItem? Selected
    {
        get => _selected;
        set { _selected = value; OnPropertyChanged(); }
    }

    private CancellationTokenSource? _sseCts;

    public int ChannelId
    {
        get => _channelId;
        set
        {
            if (_channelId != value)
            {
                _channelId = value;
                OnPropertyChanged();
                SubscribeToEvents();
            }
        }
    }

    public string Status
    {
        get => _status;
        set { if (_status != value) { _status = value; OnPropertyChanged(); } }
    }

    public ICommand RefreshCommand { get; }
    public ICommand MoveUpCommand { get; }
    public ICommand MoveDownCommand { get; }
    public ICommand SkipCommand { get; }
    public ICommand TogglePrioCommand { get; }
    public ICommand MarkPlayedCommand { get; }
    public ICommand YtdlCommand { get; }
    public ICommand LoadAbletonCommand { get; }

    public QueueViewModel(ApiClient api)
    {
        _api = api;
        _sse = new SseClient(api.BaseUrl);
        Status = "SEE created";
        _sse.MessageReceived += OnSseMessage;
        Status = "SEE listening";
        SubscribeToEvents();
        RefreshCommand = new RelayCommand(async _ => await RefreshAsync());
        MoveUpCommand = new RelayCommand(async o => await MoveAsync(o as QueueItem, "up"));
        MoveDownCommand = new RelayCommand(async o => await MoveAsync(o as QueueItem, "down"));
        SkipCommand = new RelayCommand(async o => await SkipAsync(o as QueueItem));
        TogglePrioCommand = new RelayCommand(async o => await TogglePrioAsync(o as QueueItem));
        MarkPlayedCommand = new RelayCommand(async o => await MarkPlayedAsync(o as QueueItem));
        YtdlCommand = new RelayCommand(o => StartYtdl(o as QueueItem));
        LoadAbletonCommand = new RelayCommand(o => StartAbleton(o as QueueItem));
    }

    private async void OnSseMessage(string evt, string data)
    {
        if (ChannelId > 0 && (evt == "queue" || string.IsNullOrEmpty(evt)))
            await RefreshAsync();
    }

    private void SubscribeToEvents()
    {
        _sseCts?.Cancel();
        if (ChannelId <= 0) return;
        _sseCts = new CancellationTokenSource();
        _sse.Start($"/channels/{ChannelId}/queue/stream", _sseCts.Token);
    }

    public async Task RefreshAsync()
    {
        if (ChannelId <= 0)
        {
            await _ui.InvokeAsync(() => Status = "No channel selected");
            return;
        }
        try
        {
            await _ui.InvokeAsync(() => Status = "Loading...");
            var list = await _api.GetQueueExpandedAsync(ChannelId).ConfigureAwait(false);

            await _ui.InvokeAsync(() =>
            {
                Items.Clear();
                foreach (var it in list) Items.Add(it);
                Status = $"Loaded {Items.Count} items";
            });
        }
        catch (Exception ex)
        {
            await _ui.InvokeAsync(() => Status = $"Error: {ex.Message}");
        }
    }

    private async Task MoveAsync(QueueItem? it, string dir)
    {
        if (it is null || ChannelId <= 0) return;
        try
        {
            Status = $"Moving {it.DisplayLine1} {dir}...";
            var ok = await _api.MoveRequestAsync(ChannelId, it.Id, dir);
            if (!ok) Status = "Move failed";
            await RefreshAsync();
        }
        catch (Exception ex) { await _ui.InvokeAsync(() => Status = $"Error: {ex.Message}"); }
    }

    private async Task SkipAsync(QueueItem? it)
    {
        if (it is null || ChannelId <= 0) return;
        try
        {
            Status = $"Skipping {it.DisplayLine1}...";
            var ok = await _api.SkipRequestAsync(ChannelId, it.Id);
            if (!ok) Status = "Skip failed";
            await RefreshAsync();
        }
        catch (Exception ex) { await _ui.InvokeAsync(() => Status = $"Error: {ex.Message}"); }
    }

    private async Task TogglePrioAsync(QueueItem? it)
    {
        if (it is null || ChannelId <= 0) return;
        try
        {
            var makePrio = !it.IsPriority;
            Status = makePrio ? $"Prioritizing {it.DisplayLine1}..." : $"De-prioritizing {it.DisplayLine1}...";
            var ok = await _api.SetPriorityAsync(ChannelId, it.Id, makePrio);
            if (!ok) Status = "Priority change failed";
            await RefreshAsync();
        }
        catch (Exception ex) { await _ui.InvokeAsync(() => Status = $"Error: {ex.Message}"); }
    }

    private async Task MarkPlayedAsync(QueueItem? it)
    {
        if (it is null || ChannelId <= 0) return;
        try
        {
            Status = $"Marking played {it.DisplayLine1}...";
            var ok = await _api.MarkPlayedAsync(ChannelId, it.Id);
            if (!ok) Status = "Mark played failed";
            await RefreshAsync();
        }
        catch (Exception ex) { await _ui.InvokeAsync(() => Status = $"Error: {ex.Message}"); }
    }

    private void StartYtdl(QueueItem? it)
    {
        if (it is null) return;
        // TODO: read script path from Admin settings; for now just stub
        Status = $"YT-DL trigger for {it.DisplayLine1}";
    }

    private void StartAbleton(QueueItem? it)
    {
        if (it is null) return;
        // TODO: locate and launch Ableton project; for now just stub
        Status = $"Load Ableton for {it.DisplayLine1}";
    }

    public event PropertyChangedEventHandler? PropertyChanged;
    private void OnPropertyChanged([CallerMemberName] string? n = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(n));
}