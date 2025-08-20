namespace TwitchSongAdmin.Models;

public class Song
{
    public int Id { get; set; }
    public string Artist { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
    public string? YouTubeLink { get; set; }

    public string Display => string.IsNullOrWhiteSpace(Artist) ? Title : $"{Artist} - {Title}";
}