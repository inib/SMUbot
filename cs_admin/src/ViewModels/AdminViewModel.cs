using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows.Input;
using TwitchSongAdmin.Models;
using TwitchSongAdmin.Services;
using TwitchSongAdmin.Utils;

namespace TwitchSongAdmin.ViewModels;

public class AdminViewModel : INotifyPropertyChanged
{
    private readonly ApiClient _api;
    private readonly QueueViewModel _queue;

    private string _apiBaseUrl = "http://localhost:8000";
    private string _adminToken = string.Empty;
    private Channel? _selectedChannel;

    public string ApiBaseUrl { get => _apiBaseUrl; set { _apiBaseUrl = value; OnPropertyChanged(); } }
    public string AdminToken { get => _adminToken; set { _adminToken = value; OnPropertyChanged(); } }

    public ObservableCollection<Channel> Channels { get; } = new();
    public Channel? SelectedChannel
    {
        get => _selectedChannel;
        set { _selectedChannel = value; OnPropertyChanged(); if (value != null) _queue.ChannelId = value.Id; }
    }

    public int MaxEntries { get; set; } = 50;
    public int MaxPerUser { get; set; } = 3;
    public bool PrioOnly { get; set; } = false;
    public string DownloadScriptPath { get; set; } = string.Empty;

    public ICommand ApplySettingsCommand { get; }
    public ICommand LoadChannelsCommand { get; }

    public AdminViewModel(ApiClient api, QueueViewModel queue)
    {
        _api = api; _queue = queue;
        ApplySettingsCommand = new RelayCommand(_ => _api.Configure(ApiBaseUrl, AdminToken));
        LoadChannelsCommand = new RelayCommand(_ => LoadChannelsStub());
        // stub: add one channel so you can debug immediately
        Channels.Clear();
        Channels.Add(new Channel { Id = 1, Name = "Channel #1" });
        SelectedChannel = Channels[0];
    }

    private void LoadChannelsStub()
    {
        // TODO: GET /channels and fill Channels; keep stub for now
    }

    public event PropertyChangedEventHandler? PropertyChanged;
    private void OnPropertyChanged([CallerMemberName] string? n = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(n));
}