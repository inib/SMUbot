using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Timers;

namespace TwitchSongAdmin.Services;

public class SseClient
{
    private readonly HttpClient _http = new() { Timeout = Timeout.InfiniteTimeSpan };
    public string BaseUrl { get; }
    public event Action<string, string>? MessageReceived;

    public SseClient(string baseUrl) => BaseUrl = baseUrl.TrimEnd('/');

    public void Start(string path, CancellationToken ct = default)
    {
        Task.Run(async () =>
        {
            var url = BaseUrl.TrimEnd('/') + path;
            while (!ct.IsCancellationRequested)
            {
                try
                {
                    using var req = new HttpRequestMessage(HttpMethod.Get, url);
                    req.Headers.Accept.ParseAdd("text/event-stream");
                    req.Headers.Add("X-Admin-Token", "defaultpw"); // if required

                    using var resp = await _http.SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct).ConfigureAwait(false);
                    System.Diagnostics.Debug.WriteLine($"SSE status: {(int)resp.StatusCode} {resp.ReasonPhrase}");
                    resp.EnsureSuccessStatusCode();

                    await using var stream = await resp.Content.ReadAsStreamAsync(ct).ConfigureAwait(false);
                    using var reader = new StreamReader(stream);

                    string? evt = null;
                    var dataBuf = new System.Text.StringBuilder();

                    while (!reader.EndOfStream && !ct.IsCancellationRequested)
                    {
                        var line = await reader.ReadLineAsync().ConfigureAwait(false);
                        if (line is null) break;

                        if (line.Length == 0)
                        {
                            // dispatch accumulated event
                            if (dataBuf.Length > 0)
                            {
                                var name = string.IsNullOrEmpty(evt) ? "queue" : evt;
                                MessageReceived?.Invoke(name, dataBuf.ToString());
                                dataBuf.Clear();
                                evt = null;
                            }
                            continue;
                        }
                        System.Diagnostics.Debug.WriteLine($"SSE status: {line}");
                        if (line.StartsWith("event:", StringComparison.OrdinalIgnoreCase))
                            evt = line[6..].Trim();
                        else if (line.StartsWith("data:", StringComparison.OrdinalIgnoreCase))
                        {
                            if (dataBuf.Length > 0) dataBuf.Append('\n');
                            dataBuf.Append(line[5..].Trim());
                        }
                        // ignore id:, retry:, comments
                    }
                }
                catch (Exception)
                {
                    // backoff and reconnect
                    await Task.Delay(1000, ct).ConfigureAwait(false);
                }
            }
        }, ct);
    }
}