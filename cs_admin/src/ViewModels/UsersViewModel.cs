using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using System.Windows.Input;
using TwitchSongAdmin.Models;
using TwitchSongAdmin.Services;
using TwitchSongAdmin.Utils;

namespace TwitchSongAdmin.ViewModels;

public class UsersViewModel : INotifyPropertyChanged
{
    private readonly ApiClient _api;
    public ObservableCollection<UserRow> Users { get; } = new();
    public string Status { get; set; } = "Ready";
    public int ChannelId { get; set; } = 1;

    public ICommand RefreshCommand { get; }
    public ICommand SaveCommand { get; }

    public UsersViewModel(ApiClient api)
    {
        _api = api;
        RefreshCommand = new RelayCommand(async _ => await RefreshAsync());
        SaveCommand = new RelayCommand(async _ => await SaveAsync());
    }

    public async Task RefreshAsync()
    {
        try
        {
            Status = "Loading...";
            var list = await _api.GetUsersAsync(ChannelId);
            Users.Clear();
            foreach (var u in list) Users.Add(new UserRow(u));
            Status = $"Loaded {Users.Count}";
        }
        catch (Exception ex) { Status = $"Error: {ex.Message}"; }
        OnPropertyChanged(nameof(Status));
    }

    public async Task SaveAsync()
    {
        try
        {
            foreach (var u in Users) await _api.UpdateUserPointsAsync(ChannelId, u.Id, u.PrioPoints);
            Status = "Saved";
        }
        catch (Exception ex) { Status = $"Error: {ex.Message}"; }
        OnPropertyChanged(nameof(Status));
    }

    public event PropertyChangedEventHandler? PropertyChanged;
    private void OnPropertyChanged([CallerMemberName] string? n = null) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(n));

    public class UserRow
    {
        public int Id { get; }
        public string Username { get; }
        public string TwitchId { get; }
        public int PrioPoints { get; set; }

        public UserRow(User u)
        {
            Id = u.Id; Username = u.Username; TwitchId = u.TwitchId; PrioPoints = u.PrioPoints;
        }
    }
}
