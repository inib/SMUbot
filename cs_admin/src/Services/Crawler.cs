namespace TwitchSongAdmin.Services;

public class Crawler
{
    public record Entry(string Artist, string Title, bool HasStems);

    public IEnumerable<Entry> Scan(string root)
    {
        // TODO: scan filesystem and yield entries
        yield break;
    }
}