namespace TwitchSongAdmin.Models;

public class Settings
{
    public string ApiBaseUrl { get; set; } = "http://localhost:8000";
    public string AdminToken { get; set; } = string.Empty;
    public int ChannelId { get; set; }
    public string DownloadScriptPath { get; set; } = string.Empty;
}