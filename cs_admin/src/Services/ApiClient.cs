using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using TwitchSongAdmin.Models;
using System.Runtime.Intrinsics.X86;

namespace TwitchSongAdmin.Services;

public class ApiClient
{
    private readonly HttpClient _http;
    private string _base = "http://localhost:8000";
    private string _token = "defaultpw";

    public string BaseUrl { get; set; }

    public ApiClient(HttpClient? http = null)
    {
        _http = http ?? new HttpClient();
        _http.Timeout = TimeSpan.FromSeconds(10);
    }

    public void Configure(string baseUrl, string adminToken)
    {
        _base = baseUrl.TrimEnd('/');
        BaseUrl = _base;
        //_token = adminToken ?? string.Empty;
        _http.DefaultRequestHeaders.Clear();
        if (!string.IsNullOrEmpty(_token))
            _http.DefaultRequestHeaders.Add("X-Admin-Token", _token);
        _http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
    }

    private string U(string path) => _base + path;

    // ---- DTOs matching backend ----
    public record QueueDto(int id, int song_id, int user_id, int is_priority, int played);
    public record SongDto(int id, string? artist, string? title, string? youtube_link);
    public record UserDto(int id, string username, string twitch_id);

    public async Task<List<QueueDto>> GetQueueRawAsync(int channelId)
    {
        var res = await _http.GetFromJsonAsync<List<QueueDto>>(U($"/channels/{channelId}/queue"));
        return res ?? new List<QueueDto>();
    }

    public async Task<SongDto?> GetSongAsync(int channelId, int songId)
        => await _http.GetFromJsonAsync<SongDto>(U($"/channels/{channelId}/songs/{songId}"));

    public async Task<UserDto?> GetUserAsync(int channelId, int userId)
        => await _http.GetFromJsonAsync<UserDto>(U($"/channels/{channelId}/users/{userId}"));

    // Expand queue entries to UI model
    public async Task<List<QueueItem>> GetQueueExpandedAsync(int channelId)
    {
        var raw = await GetQueueRawAsync(channelId);
        var pending = raw.Where(q => q.played == 0).ToList();
        var tasks = pending.Select(async q =>
        {
            var song = await GetSongAsync(channelId, q.song_id) ?? new SongDto(q.song_id, "", "", null);
            var user = await GetUserAsync(channelId, q.user_id) ?? new UserDto(q.user_id, "", "");
            return new QueueItem
            {
                Id = q.id,
                SongId = q.song_id,
                UserId = q.user_id,
                IsPriority = q.is_priority == 1,
                Played = q.played == 1,
                Artist = song.artist ?? string.Empty,
                Title = song.title ?? string.Empty,
                Requester = user.username ?? string.Empty
            };
        });
        return (await Task.WhenAll(tasks)).ToList();
    }
    public async Task<bool> MoveRequestAsync(int channelId, int requestId, string direction)
    {
        // Adjust to your backend route. Example: POST /channels/{id}/queue/{req}/move?direction=up|down
        var resp = await _http.PostAsync(U($"/channels/{channelId}/queue/{requestId}/move?direction={direction}"), null);
        return resp.IsSuccessStatusCode;
    }

    public async Task<bool> SkipRequestAsync(int channelId, int requestId)
    {
        // Example: POST /channels/{id}/queue/{req}/skip  (server moves it to bottom)
        var resp = await _http.PostAsync(U($"/channels/{channelId}/queue/{requestId}/skip"), null);
        return resp.IsSuccessStatusCode;
    }

    public async Task<bool> SetPriorityAsync(int channelId, int requestId, bool makePriority)
    {
        // Example: POST /channels/{id}/queue/{req}/priority?enabled=true|false
        var resp = await _http.PostAsync(U($"/channels/{channelId}/queue/{requestId}/priority?enabled={(makePriority ? "true" : "false")}"), null);
        return resp.IsSuccessStatusCode;
    }

    public async Task<bool> MarkPlayedAsync(int channelId, int requestId)
    {
        // Example: POST /channels/{id}/queue/{req}/played
        var resp = await _http.PostAsync(U($"/channels/{channelId}/queue/{requestId}/played"), null);
        return resp.IsSuccessStatusCode;
    }
}