using System.Net.Sockets;
using System.Text;
using Prometheus;

namespace RobustTrafficProxy;

public sealed class MetricsService : IDisposable
{
    private readonly TcpListener? _listener;
    private CancellationTokenSource? _cts;

    public readonly Counter PacketsTotal = Metrics.CreateCounter(
        "proxy_packets_total",
        "Total packets processed",
        new CounterConfiguration { LabelNames = ["dir", "status"] });

    public readonly Counter BytesTotal = Metrics.CreateCounter(
        "proxy_bytes_total",
        "Total bytes forwarded",
        new CounterConfiguration { LabelNames = ["dir"] });

    public readonly Counter ConnectionsTotal = Metrics.CreateCounter(
        "proxy_connections_total",
        "Total client connections established");

    public readonly Counter ConnectionsDroppedTotal = Metrics.CreateCounter(
        "proxy_connections_dropped_total",
        "Total client connections dropped",
        new CounterConfiguration { LabelNames = ["reason"] });

    public readonly Gauge ActiveSessions = Metrics.CreateGauge(
        "proxy_active_sessions",
        "Current number of active client sessions");

    public readonly Counter FilterDenialsTotal = Metrics.CreateCounter(
        "proxy_filter_denials_total",
        "Total connections denied by IP filter");

    public readonly Counter TcpConnectionsTotal = Metrics.CreateCounter(
        "proxy_tcp_connections_total",
        "Total TCP connections established");

    public readonly Gauge TcpConnectionsActive = Metrics.CreateGauge(
        "proxy_tcp_connections_active",
        "Current active TCP connections");

    public readonly Counter TcpBytesTotal = Metrics.CreateCounter(
        "proxy_tcp_bytes_total",
        "Total TCP bytes relayed",
        new CounterConfiguration { LabelNames = ["dir"] });

    public readonly Counter TcpErrorsTotal = Metrics.CreateCounter(
        "proxy_tcp_errors_total",
        "Total TCP proxy errors");

    public MetricsService(int port)
    {
        if (port <= 0)
            return;

        _listener = new TcpListener(System.Net.IPAddress.Loopback, port);
        _listener.Start();
        _cts = new CancellationTokenSource();
        Console.WriteLine($"[Metrics] Prometheus endpoint on http://localhost:{port}/metrics");
        _ = AcceptLoopAsync(_cts.Token);
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
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
            catch { }
        }
    }

    private async Task HandleConnectionAsync(TcpClient client, CancellationToken ct)
    {
        using (client)
        {
            try
            {
                var stream = client.GetStream();
                var buffer = new byte[4096];
                var read = await stream.ReadAsync(buffer, ct);

                var request = Encoding.ASCII.GetString(buffer, 0, read);
                if (!request.StartsWith("GET /metrics ", StringComparison.Ordinal))
                {
                    var notFound = "HTTP/1.1 404 Not Found\r\nContent-Length: 2\r\n\r\nOK"u8.ToArray();
                    await stream.WriteAsync(notFound.AsMemory(), ct);
                    return;
                }

                var body = CollectMetricsText();
                var bodyBytes = Encoding.UTF8.GetBytes(body);
                var header = $"HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {bodyBytes.Length}\r\nConnection: close\r\n\r\n";
                var headerBytes = Encoding.ASCII.GetBytes(header);

                await stream.WriteAsync(headerBytes, ct);
                await stream.WriteAsync(bodyBytes, ct);
            }
            catch { }
        }
    }

    private string CollectMetricsText()
    {
        var sb = new StringBuilder();

        AppendCounter2(sb, PacketsTotal, "proxy_packets_total", "Total packets processed",
            [("c2s", "forwarded"), ("c2s", "dropped"), ("s2c", "forwarded"), ("s2c", "dropped")], "dir", "status");
        AppendCounter1(sb, BytesTotal, "proxy_bytes_total", "Total bytes forwarded",
            ["c2s", "s2c"], "dir");
        AppendCounter0(sb, ConnectionsTotal, "proxy_connections_total", "Total client connections established");
        AppendCounter1(sb, ConnectionsDroppedTotal, "proxy_connections_dropped_total", "Total client connections dropped",
            ["timeout", "max_clients"], "reason");
        AppendGauge0(sb, ActiveSessions, "proxy_active_sessions", "Current number of active client sessions");
        AppendCounter0(sb, FilterDenialsTotal, "proxy_filter_denials_total", "Total connections denied by IP filter");
        AppendCounter0(sb, TcpConnectionsTotal, "proxy_tcp_connections_total", "Total TCP connections established");
        AppendGauge0(sb, TcpConnectionsActive, "proxy_tcp_connections_active", "Current active TCP connections");
        AppendCounter1(sb, TcpBytesTotal, "proxy_tcp_bytes_total", "Total TCP bytes relayed",
            ["c2s", "s2c"], "dir");
        AppendCounter0(sb, TcpErrorsTotal, "proxy_tcp_errors_total", "Total TCP proxy errors");

        return sb.ToString();
    }

    private static void AppendCounter0(StringBuilder sb, Counter counter, string name, string help)
    {
        sb.Append("# HELP ").Append(name).Append(' ').AppendLine(help);
        sb.Append("# TYPE ").Append(name).AppendLine(" counter");
        sb.Append(name).Append(' ').AppendLine(counter.Value.ToString("F0"));
    }

    private static void AppendCounter1(StringBuilder sb, Counter counter, string name, string help, string[] values, string label)
    {
        sb.Append("# HELP ").Append(name).Append(' ').AppendLine(help);
        sb.Append("# TYPE ").Append(name).AppendLine(" counter");
        foreach (var v in values)
        {
            sb.Append(name).Append('{').Append(label).Append("=\"").Append(v).Append("\"} ");
            sb.AppendLine(counter.WithLabels(v).Value.ToString("F0"));
        }
    }

    private static void AppendCounter2(StringBuilder sb, Counter counter, string name, string help, (string v1, string v2)[] values, string l1, string l2)
    {
        sb.Append("# HELP ").Append(name).Append(' ').AppendLine(help);
        sb.Append("# TYPE ").Append(name).AppendLine(" counter");
        foreach (var (v1, v2) in values)
        {
            sb.Append(name).Append('{').Append(l1).Append("=\"").Append(v1).Append("\",").Append(l2).Append("=\"").Append(v2).Append("\"} ");
            sb.AppendLine(counter.WithLabels(v1, v2).Value.ToString("F0"));
        }
    }

    private static void AppendGauge0(StringBuilder sb, Gauge gauge, string name, string help)
    {
        sb.Append("# HELP ").Append(name).Append(' ').AppendLine(help);
        sb.Append("# TYPE ").Append(name).AppendLine(" gauge");
        sb.Append(name).Append(' ').AppendLine(gauge.Value.ToString("F0"));
    }

    public void PacketForwarded(string dir, int bytes)
    {
        PacketsTotal.WithLabels(dir, "forwarded").Inc();
        BytesTotal.WithLabels(dir).Inc(bytes);
    }

    public void PacketDropped(string dir)
    {
        PacketsTotal.WithLabels(dir, "dropped").Inc();
    }

    public void ConnectionEstablished()
    {
        ConnectionsTotal.Inc();
        ActiveSessions.Inc();
    }

    public void ConnectionDropped(string reason)
    {
        ConnectionsDroppedTotal.WithLabels(reason).Inc();
        ActiveSessions.Dec();
    }

    public void FilterDenial()
    {
        FilterDenialsTotal.Inc();
    }

    public void TcpConnectionEstablished()
    {
        TcpConnectionsTotal.Inc();
        TcpConnectionsActive.Inc();
    }

    public void TcpConnectionClosed()
    {
        TcpConnectionsActive.Dec();
    }

    public void TcpBytesForwarded(string dir, int bytes)
    {
        TcpBytesTotal.WithLabels(dir).Inc(bytes);
    }

    public void TcpConnectionError()
    {
        TcpErrorsTotal.Inc();
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _listener?.Stop();
    }
}