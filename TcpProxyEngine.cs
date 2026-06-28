using System.Net;
using System.Net.Sockets;
using System.Text;

namespace RobustTrafficProxy;

public sealed class TcpProxyEngine
{
    private readonly ProxyConfig _config;
    private readonly MetricsService _metrics;
    private readonly FilterEngine _filter;
    private TcpListener? _listener;
    private CancellationTokenSource? _cts;

    public TcpProxyEngine(ProxyConfig config, MetricsService metrics, FilterEngine filter)
    {
        _config = config;
        _metrics = metrics;
        _filter = filter;
    }

    public async Task RunAsync()
    {
        _cts = new CancellationTokenSource();
        _listener = new TcpListener(_config.ListenEndpoint);
        _listener.Start();

        Console.WriteLine($"[TCP Proxy] Listening on {_config.ListenEndpoint}");
        Console.WriteLine($"[TCP Proxy] Forwarding to {_config.TargetEndpoint}");

        try
        {
            await AcceptLoopAsync(_cts.Token);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            Cleanup();
        }
    }

    public void Stop()
    {
        _cts?.Cancel();
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        var workerCount = Math.Max(1, Environment.ProcessorCount);
        var workers = new Task[workerCount];
        for (var i = 0; i < workerCount; i++)
        {
            workers[i] = AcceptWorkerAsync(ct);
        }
        await Task.WhenAll(workers);
    }

    private async Task AcceptWorkerAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var client = await _listener!.AcceptTcpClientAsync(ct);
                _ = HandleConnectionAsync(client, ct);
            }
            catch (OperationCanceledException) { break; }
            catch (ObjectDisposedException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"[TCP ERROR] Accept: {ex.Message}");
            }
        }
    }

    private async Task HandleConnectionAsync(TcpClient client, CancellationToken ct)
    {
        var remoteEp = client.Client.RemoteEndPoint;
        var clientIp = ((IPEndPoint)remoteEp!).Address;

        if (!_filter.IsClientAllowed(clientIp))
        {
            _metrics.FilterDenial();
            if (_config.Verbose)
                Console.WriteLine($"[TCP DROP] not allowed {remoteEp}");
            client.Dispose();
            return;
        }

        _metrics.TcpConnectionEstablished();
        Console.WriteLine($"[TCP CONN] {remoteEp}");

        try
        {
            using (client)
            using (var target = new TcpClient())
            {
                await target.ConnectAsync(_config.TargetEndpoint.Address, _config.TargetEndpoint.Port, ct);

                using var clientStream = client.GetStream();
                using var targetStream = target.GetStream();

                var c2s = RelayAsync(clientStream, targetStream, "c2s", ct);
                var s2c = RelayAsync(targetStream, clientStream, "s2c", ct);

                await Task.WhenAny(c2s, s2c);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            Console.WriteLine($"[TCP ERROR] {remoteEp}: {ex.Message}");
            _metrics.TcpConnectionError();
        }
        finally
        {
            _metrics.TcpConnectionClosed();
            Console.WriteLine($"[TCP DISC] {remoteEp}");
        }
    }

    private async Task RelayAsync(NetworkStream source, NetworkStream destination, string dir, CancellationToken ct)
    {
        var buffer = new byte[81920];
        var canRewrite = dir == "s2c";
        while (!ct.IsCancellationRequested)
        {
            var read = await source.ReadAsync(buffer, ct);
            if (read == 0) break;

            if (canRewrite)
            {
                canRewrite = false;
                if (TryRewriteConnectAddress(buffer.AsMemory(0, read), out var rewritten))
                {
                    await destination.WriteAsync(rewritten, ct);
                    _metrics.TcpBytesForwarded(dir, rewritten.Length);
                    continue;
                }
            }

            await destination.WriteAsync(buffer.AsMemory(0, read), ct);
            _metrics.TcpBytesForwarded(dir, read);
        }
    }

    private bool TryRewriteConnectAddress(ReadOnlyMemory<byte> data, out byte[] rewritten)
    {
        rewritten = [];
        var text = Encoding.UTF8.GetString(data.Span);

        if (!text.Contains("\"connect_address\"", StringComparison.Ordinal))
            return false;

        var headerEnd = text.IndexOf("\r\n\r\n", StringComparison.Ordinal);
        if (headerEnd < 0) return false;

        var headerStr = text.AsSpan(0, headerEnd);
        var contentLength = -1;
        foreach (var line in headerStr.ToString().Split("\r\n"))
        {
            if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(line["Content-Length:".Length..].Trim(), out var cl))
            {
                contentLength = cl;
                break;
            }
        }
        if (contentLength < 0) return false;

        var bodyStart = headerEnd + 4;
        if (bodyStart + contentLength > text.Length)
            return false;

        var body = text.AsSpan(bodyStart, contentLength);

        var addrKey = "\"connect_address\":\"";
        var addrIdx = body.IndexOf(addrKey, StringComparison.Ordinal);
        if (addrIdx < 0)
        {
            addrKey = "\"connect_address\": \"";
            addrIdx = body.IndexOf(addrKey, StringComparison.Ordinal);
            if (addrIdx < 0) return false;
        }

        var valStart = addrIdx + addrKey.Length;
        var valEnd = body[valStart..].IndexOf('\"');
        if (valEnd < 0) return false;

        var oldVal = body.Slice(valStart, valEnd).ToString();
        var addr = _config.AdvertisedAddress ?? _config.ListenEndpoint.ToString();
        var newVal = $"udp://{addr}";

        if (oldVal == newVal)
            return false;

        var newBody = string.Concat(
            body[..valStart].ToString(),
            newVal,
            body[(valStart + valEnd + 1)..].ToString()
        );
        var newBodyBytes = Encoding.UTF8.GetBytes(newBody);

        var newHeaders = new StringBuilder();
        foreach (var line in headerStr.ToString().Split("\r\n"))
        {
            if (line.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                newHeaders.Append("Content-Length: ").Append(newBodyBytes.Length);
            else
                newHeaders.Append(line);
            newHeaders.Append("\r\n");
        }
        newHeaders.Append("\r\n");

        var headerBytes = Encoding.ASCII.GetBytes(newHeaders.ToString());
        rewritten = [.. headerBytes, .. newBodyBytes];
        return true;
    }

    private void Cleanup()
    {
        try { _listener?.Stop(); } catch { }
    }
}