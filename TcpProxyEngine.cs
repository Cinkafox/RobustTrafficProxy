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
                TryRewriteConnectAddress(ref buffer);
            }
            
            await destination.WriteAsync(buffer.AsMemory(0, read), ct);
            _metrics.TcpBytesForwarded(dir, read);
        }
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