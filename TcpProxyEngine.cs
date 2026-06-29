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

    private sealed class RelayState
    {
        public bool NeedRewrite;
        public bool ResponseBuffered;
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

                var state = new RelayState();
                var c2s = RelayAsync(clientStream, targetStream, "c2s", state, ct);
                var s2c = RelayAsync(targetStream, clientStream, "s2c", state, ct);

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

    private async Task RelayAsync(NetworkStream source, NetworkStream destination, string dir, RelayState state, CancellationToken ct)
    {
        var buffer = new byte[81920];

        while (!ct.IsCancellationRequested)
        {
            var read = await source.ReadAsync(buffer, ct);
            if (read == 0)
                break;

            if (dir == "c2s" && !state.NeedRewrite && IsInfoRequest(buffer, read))
                state.NeedRewrite = true;

            if (dir == "s2c" && state.NeedRewrite && !state.ResponseBuffered)
            {
                var response = await BufferCompleteResponseAsync(source, buffer, read, ct);
                TryRewriteConnectAddress(ref response);
                await destination.WriteAsync(response, ct);
                _metrics.TcpBytesForwarded(dir, response.Length);
                state.ResponseBuffered = true;
                continue;
            }

            await destination.WriteAsync(buffer.AsMemory(0, read), ct);
            _metrics.TcpBytesForwarded(dir, read);
        }
    }
    
    private static bool IsInfoRequest(byte[] buffer, int read)
    {
        var span = buffer.AsSpan(0, read);

        if (span.Length < 9)
            return false;

        if (span[0] != (byte)'G' || span[1] != (byte)'E' || span[2] != (byte)'T' ||
            span[3] != (byte)' ' || span[4] != (byte)'/' || span[5] != (byte)'i' ||
            span[6] != (byte)'n' || span[7] != (byte)'f' || span[8] != (byte)'o')
            return false;

        if (span.Length == 9)
            return true;

        var c = span[9];
        if (c == (byte)' ' || c == (byte)'?' || c == (byte)'\r' || c == (byte)'\n')
            return true;

        if (c == (byte)'.')
        {
            if (span.Length < 14)
                return false;

            if (span[10] != (byte)'j' || span[11] != (byte)'s' || span[12] != (byte)'o' || span[13] != (byte)'n')
                return false;

            if (span.Length == 14)
                return true;

            var c1 = span[14];
            return c1 == (byte)' ' || c1 == (byte)'?' || c1 == (byte)'\r' || c1 == (byte)'\n';
        }

        return false;
    }

    private static async Task<byte[]> BufferCompleteResponseAsync(
        NetworkStream source, byte[] initialBuffer, int initialRead, CancellationToken ct)
    {
        using var accumulator = new MemoryStream();
        accumulator.Write(initialBuffer, 0, initialRead);
        var headerBuf = new byte[81920];

        while (true)
        {
            var data = accumulator.GetBuffer();
            var length = (int)accumulator.Length;

            var headerEnd = IndexOfSequence(data, length, "\r\n\r\n"u8);
            if (headerEnd < 0)
            {
                var r = await source.ReadAsync(headerBuf, ct);
                if (r == 0) break;
                accumulator.Write(headerBuf, 0, r);
                continue;
            }

            var headerBytes = data.AsSpan(0, headerEnd);
            var headerText = Encoding.ASCII.GetString(headerBytes);

            var contentLength = TryParseContentLength(headerText);
            if (contentLength >= 0)
            {
                var bodyStart = headerEnd + 4;
                var expectedTotal = bodyStart + contentLength;

                while (accumulator.Length < expectedTotal)
                {
                    var r = await source.ReadAsync(headerBuf, ct);
                    if (r == 0) break;
                    accumulator.Write(headerBuf, 0, r);
                }
                break;
            }

            if (headerText.Contains("Transfer-Encoding", StringComparison.OrdinalIgnoreCase))
            {
                while (true)
                {
                    var totalData = accumulator.GetBuffer().AsSpan(0, (int)accumulator.Length);
                    if (EndsWithChunkedTerminator(totalData))
                        break;

                    var r = await source.ReadAsync(headerBuf, ct);
                    if (r == 0) break;
                    accumulator.Write(headerBuf, 0, r);
                }
                break;
            }

            break;
        }

        return accumulator.ToArray();
    }

    private static int IndexOfSequence(byte[] data, int length, ReadOnlySpan<byte> pattern)
    {
        return data.AsSpan(0, length).IndexOf(pattern);
    }

    private static bool EndsWithChunkedTerminator(ReadOnlySpan<byte> data)
    {
        if (data.Length < 5) return false;

        return data[^5] == (byte)'0' && data[^4] == (byte)'\r' &&
               data[^3] == (byte)'\n' && data[^2] == (byte)'\r' && data[^1] == (byte)'\n';
    }

    private static int TryParseContentLength(string headerText)
    {
        var lower = headerText.ToLowerInvariant().AsSpan();
        ReadOnlySpan<char> searchKey = "\ncontent-length:";

        var clIdx = lower.IndexOf(searchKey);
        if (clIdx < 0)
        {
            searchKey = "content-length:";
            clIdx = lower.IndexOf(searchKey);
            if (clIdx < 0)
                return -1;
        }

        var valStart = clIdx + searchKey.Length;
        while (valStart < lower.Length && lower[valStart] == ' ') valStart++;

        var valEnd = valStart;
        while (valEnd < lower.Length && lower[valEnd] >= '0' && lower[valEnd] <= '9') valEnd++;

        if (valEnd <= valStart)
            return -1;

        var numSpan = lower[valStart..valEnd];
        var result = 0;
        for (var i = 0; i < numSpan.Length; i++)
        {
            result = result * 10 + (numSpan[i] - '0');
        }

        return result;
    }

    private bool TryRewriteConnectAddress(ref byte[] data)
    {
        var key = "\"connect_address\""u8; 
        Span<byte> span = data; 

        var keyIdx = span.IndexOf(key);
        if (keyIdx == -1)
            return false;

        var idx = keyIdx + key.Length;
        
        while (idx < span.Length && span[idx] == (byte)' ') idx++;
        if (idx >= span.Length || span[idx] != (byte)':') return false;
        idx++;
        
        while (idx < span.Length && span[idx] == (byte)' ') idx++;
        if (idx >= span.Length || span[idx] != (byte)'"') return false;
        idx++; 

        var endIdx = idx;
        
        while (idx < span.Length)
        {
            var current = span[idx];
            span[idx] = (byte)' ';
            idx++;
        
            if (current == (byte)'"')
                break;
        }
        
        if (endIdx < span.Length)
        {
            span[endIdx] = (byte)'"';
        }

        return true;
    }

    private void Cleanup()
    {
        try { _listener?.Stop(); } catch { }
    }
}