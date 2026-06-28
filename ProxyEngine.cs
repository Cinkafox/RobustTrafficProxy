using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

public sealed class ProxyEngine
{
    private readonly ProxyConfig _config;
    private readonly FilterEngine _filter;
    public readonly MetricsService Metrics;
    private readonly ConcurrentDictionary<EndPoint, ClientSession> _sessions = new();
    private readonly ConcurrentDictionary<int, ClientSession> _upstreamByPort = new();
    private Socket? _listenSocket;
    private CancellationTokenSource? _cts;

    private static readonly byte[] SospMsg = "You are blocked by proxy filter"u8.ToArray();

    public ProxyEngine(ProxyConfig config)
    {
        _config = config;
        _filter = new FilterEngine(config);
        Metrics = new MetricsService(config.MetricsPort);
    }

    public async Task RunAsync()
    {
        _cts = new CancellationTokenSource();
        _listenSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        _listenSocket.Bind(_config.ListenEndpoint);

        var listenEndPoint = (IPEndPoint)_listenSocket.LocalEndPoint!;
        Console.WriteLine($"[Proxy] Listening on {listenEndPoint}");
        Console.WriteLine($"[Proxy] Forwarding to {_config.TargetEndpoint}");
        Console.WriteLine($"[Proxy] Max clients: {_config.MaxClients}, Rate limit: {_config.RateLimitPerClient}/s");

        if (_config.AllowedIPs.Count > 0)
            Console.WriteLine($"[Proxy] Allowlist: {_config.AllowedIPs.Count} rule(s)");
        if (_config.DeniedIPs.Count > 0)
            Console.WriteLine($"[Proxy] Denylist: {_config.DeniedIPs.Count} rule(s)");
        if (_config.MetricsPort > 0)
            Console.WriteLine($"[Proxy] Metrics enabled on port {_config.MetricsPort}");

        _ = CleanupLoopAsync(_cts.Token);
        var statsTask = StatsLoopAsync(_cts.Token);

        try
        {
            await ListenLoopAsync(_cts.Token);
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            await statsTask;
            Cleanup();
            Metrics.Dispose();
        }
    }

    public void Stop()
    {
        _cts?.Cancel();
    }

    private async Task ListenLoopAsync(CancellationToken ct)
    {
        var buffer = GC.AllocateUninitializedArray<byte>(ushort.MaxValue);
        var remoteEp = new IPEndPoint(IPAddress.Any, 0) as EndPoint;
        var recvFromArgs = new SocketAsyncEventArgs { RemoteEndPoint = remoteEp };

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var (data, length, sourceEp) = await ReceiveFromAsync(_listenSocket!, buffer, remoteEp, ct);

                var sourceIp = ((IPEndPoint)sourceEp).Address;

                if (_sessions.TryGetValue(sourceEp, out var existing))
                {
                    existing.LastActivity = Stopwatch.GetTimestamp();
                    if (!_filter.CheckRateLimit(sourceIp))
                    {
                        Metrics.PacketDropped("c2s");
                        if (_config.Verbose)
                            Console.WriteLine($"[DROP] rate limit {sourceEp}");
                        continue;
                    }

                    await existing.UpstreamSocket.SendToAsync(
                        data.AsMemory(0, length), SocketFlags.None, _config.TargetEndpoint, ct);

                    Metrics.PacketForwarded("c2s", length);
                    if (_config.Verbose)
                        Console.WriteLine($"[FWD] {sourceEp} -> server ({length} bytes)");
                }
                else
                {
                    await HandleNewClientAsync(sourceEp, data, firstLength: length, ct);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
            {
                // Ignore ICMP unreachable from upstream
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Listen loop: {ex.Message}");
            }
        }
    }

    private async Task HandleNewClientAsync(
        EndPoint clientEp, byte[] firstPacket, int firstLength, CancellationToken ct)
    {
        var clientIp = ((IPEndPoint)clientEp).Address;

        if (_sessions.Count >= _config.MaxClients)
        {
            Metrics.PacketDropped("c2s");
            Metrics.ConnectionDropped("max_clients");
            if (_config.Verbose)
                Console.WriteLine($"[DROP] max clients {clientEp}");
            return;
        }

        if (!_filter.IsClientAllowed(clientIp))
        {
            Metrics.PacketDropped("c2s");
            Metrics.FilterDenial();
            try
            {
                await _listenSocket!.SendToAsync(
                    SospMsg.AsMemory(), SocketFlags.None, clientEp, ct);
            }
            catch { }
            if (_config.Verbose)
                Console.WriteLine($"[DROP] not allowed {clientEp}");
            return;
        }

        if (!_filter.CheckRateLimit(clientIp))
        {
            Metrics.PacketDropped("c2s");
            if (_config.Verbose)
                Console.WriteLine($"[DROP] rate limit {clientEp}");
            return;
        }

        // Create upstream socket to server
        var upstream = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        try
        {
            upstream.Bind(new IPEndPoint(IPAddress.Any, 0));
        }
        catch
        {
            upstream.Dispose();
            Metrics.PacketDropped("c2s");
            return;
        }

        var localPort = ((IPEndPoint)upstream.LocalEndPoint!).Port;
        var session = new ClientSession
        {
            ClientEndpoint = (IPEndPoint)clientEp,
            UpstreamSocket = upstream,
            LocalPort = localPort,
            LastActivity = Stopwatch.GetTimestamp(),
        };

        if (!_sessions.TryAdd(clientEp, session))
        {
            upstream.Dispose();
            return;
        }
        _upstreamByPort[localPort] = session;

        Metrics.ConnectionEstablished();
        Console.WriteLine($"[CONN] {clientEp} -> upstream port {localPort}");

        // Start upstream receive loop (fire-and-forget but tracked)
        _ = UpstreamReceiveLoopAsync(session, ct);

        // Forward the first packet
        await upstream.SendToAsync(
            firstPacket.AsMemory(0, firstLength), SocketFlags.None, _config.TargetEndpoint, ct);

        Metrics.PacketForwarded("c2s", firstLength);
        if (_config.Verbose)
            Console.WriteLine($"[FWD] {clientEp} -> server ({firstLength} bytes)");
    }

    private async Task UpstreamReceiveLoopAsync(ClientSession session, CancellationToken ct)
    {
        var buffer = GC.AllocateUninitializedArray<byte>(ushort.MaxValue);
        var remoteEp = new IPEndPoint(IPAddress.Any, 0) as EndPoint;

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var (data, length, _) = await ReceiveFromAsync(session.UpstreamSocket, buffer, remoteEp, ct);
                session.LastActivity = Stopwatch.GetTimestamp();

                await _listenSocket!.SendToAsync(
                    data.AsMemory(0, length), SocketFlags.None, session.ClientEndpoint, ct);

                Metrics.PacketForwarded("s2c", length);
                if (_config.Verbose)
                    Console.WriteLine($"[FWD] server -> {session.ClientEndpoint} ({length} bytes)");
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException ex) when (ex.SocketErrorCode == SocketError.ConnectionReset)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] Upstream {session.LocalPort}: {ex.Message}");
                break;
            }
        }
    }

    private async Task CleanupLoopAsync(CancellationToken ct)
    {
        var period = TimeSpan.FromSeconds(Math.Min(_config.SessionTimeoutSeconds, 10));
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(period, ct);
                CleanupStaleSessions();
            }
            catch (OperationCanceledException) { break; }
        }
    }

    private void CleanupStaleSessions()
    {
        var now = Stopwatch.GetTimestamp();
        var timeoutTicks = (long)(Stopwatch.Frequency * (double)_config.SessionTimeoutSeconds);
        var stale = new List<EndPoint>();

        foreach (var (ep, session) in _sessions)
        {
            if (now - session.LastActivity > timeoutTicks)
                stale.Add(ep);
        }

        foreach (var ep in stale)
        {
            if (_sessions.TryRemove(ep, out var session))
            {
                _upstreamByPort.TryRemove(session.LocalPort, out _);
                try { session.UpstreamSocket.Close(); } catch { }
                try { session.UpstreamSocket.Dispose(); } catch { }
                Metrics.ConnectionDropped("timeout");
                Console.WriteLine($"[DISC] {ep} (timeout)");
            }
        }
    }

    private async Task StatsLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(10_000, ct);
            var forwarded = Metrics.PacketsTotal.WithLabels("c2s", "forwarded").Value +
                            Metrics.PacketsTotal.WithLabels("s2c", "forwarded").Value;
            var dropped = Metrics.PacketsTotal.WithLabels("c2s", "dropped").Value +
                          Metrics.PacketsTotal.WithLabels("s2c", "dropped").Value;
            Console.WriteLine(
                $"[STATS] sessions={_sessions.Count} packets={forwarded} dropped={dropped}");
        }
    }

    private void Cleanup()
    {
        foreach (var (_, session) in _sessions)
        {
            try { session.UpstreamSocket.Close(); } catch { }
            try { session.UpstreamSocket.Dispose(); } catch { }
        }
        _sessions.Clear();
        _upstreamByPort.Clear();
        _listenSocket?.Close();
        _listenSocket?.Dispose();
    }

    private static async ValueTask<(byte[] data, int length, EndPoint remoteEp)> ReceiveFromAsync(
        Socket socket, byte[] buffer, EndPoint remoteEp, CancellationToken ct)
    {
        var args = new SocketAsyncEventArgs
        {
            RemoteEndPoint = remoteEp,
            SocketFlags = SocketFlags.None,
        };
        args.SetBuffer(buffer, 0, buffer.Length);

        var tcs = new TaskCompletionSource<SocketError>();
        args.Completed += (_, a) => tcs.TrySetResult(a.SocketError);

        if (!socket.ReceiveFromAsync(args))
            tcs.TrySetResult(args.SocketError);

        var error = await tcs.Task.WaitAsync(ct);
        if (error != SocketError.Success)
            throw new SocketException((int)error);

        return (buffer, args.BytesTransferred, args.RemoteEndPoint!);
    }

    private sealed class ClientSession
    {
        public required IPEndPoint ClientEndpoint { get; init; }
        public required Socket UpstreamSocket { get; init; }
        public required int LocalPort { get; init; }
        public long LastActivity { get; set; }
    }
}
