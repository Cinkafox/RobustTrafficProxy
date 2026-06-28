using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace RobustTrafficProxy;

public sealed class FilterEngine
{
    private readonly List<IPNetwork> _allowedNetworks = [];
    private readonly List<IPNetwork> _deniedNetworks = [];
    private readonly ConcurrentDictionary<IPAddress, RateLimitState> _rateLimits = new();
    private readonly int _rateLimitPerSecond;
    private readonly bool _hasAllowList;

    public FilterEngine(ProxyConfig config)
    {
        _rateLimitPerSecond = config.RateLimitPerClient;

        foreach (var entry in config.AllowedIPs)
        {
            if (TryParseNetwork(entry, out var network))
                _allowedNetworks.Add(network);
        }
        _hasAllowList = _allowedNetworks.Count > 0;

        foreach (var entry in config.DeniedIPs)
        {
            if (TryParseNetwork(entry, out var network))
                _deniedNetworks.Add(network);
        }
    }

    public bool IsClientAllowed(IPAddress clientIp)
    {
        if (IPAddress.IsLoopback(clientIp))
            return true;

        // If allowlist is configured, IP must match at least one rule
        if (_hasAllowList && !MatchesAny(clientIp, _allowedNetworks))
            return false;

        // If denylist is configured, IP must not match any rule
        if (_deniedNetworks.Count > 0 && MatchesAny(clientIp, _deniedNetworks))
            return false;

        return true;
    }

    public bool CheckRateLimit(IPAddress clientIp)
    {
        if (_rateLimitPerSecond <= 0)
            return true;

        var state = _rateLimits.GetOrAdd(clientIp, _ => new RateLimitState());
        var now = Stopwatch.GetTimestamp();
        var windowTicks = Stopwatch.Frequency; // 1 second

        if (now - state.WindowStart > windowTicks)
        {
            state.WindowStart = now;
            state.Count = 0;
        }

        state.Count++;
        return state.Count <= _rateLimitPerSecond;
    }

    private static bool MatchesAny(IPAddress ip, List<IPNetwork> networks)
    {
        foreach (var network in networks)
        {
            if (network.Contains(ip))
                return true;
        }
        return false;
    }

    private static bool TryParseNetwork(string input, out IPNetwork network)
    {
        network = default;

        var trimmed = input.Trim();
        if (trimmed.Length == 0)
            return false;

        var slashIndex = trimmed.IndexOf('/');
        if (slashIndex < 0)
        {
            // Exact IP
            if (!IPAddress.TryParse(trimmed, out var addr))
                return false;
            var bits = addr.AddressFamily == AddressFamily.InterNetworkV6 ? 128 : 32;
            network = new IPNetwork(addr, bits);
            return true;
        }

        var ipPart = trimmed[..slashIndex];
        var prefixPart = trimmed[(slashIndex + 1)..];

        if (!IPAddress.TryParse(ipPart, out var baseAddr))
            return false;
        if (!int.TryParse(prefixPart, out var prefixLen))
            return false;

        var totalBits = baseAddr.AddressFamily == AddressFamily.InterNetworkV6 ? 128 : 32;
        if (prefixLen < 0 || prefixLen > totalBits)
            return false;

        network = new IPNetwork(baseAddr, prefixLen);
        return true;
    }

    private sealed class RateLimitState
    {
        public long WindowStart = Stopwatch.GetTimestamp();
        public int Count;
    }

    private readonly struct IPNetwork
    {
        public readonly IPAddress BaseAddress;
        public readonly int PrefixLength;

        public IPNetwork(IPAddress baseAddress, int prefixLength)
        {
            BaseAddress = baseAddress;
            PrefixLength = prefixLength;
        }

        public bool Contains(IPAddress ip)
        {
            if (ip.AddressFamily != BaseAddress.AddressFamily)
                return false;

            var ipBytes = ip.GetAddressBytes();
            var baseBytes = BaseAddress.GetAddressBytes();

            var fullBytes = PrefixLength / 8;
            var remainingBits = PrefixLength % 8;

            for (var i = 0; i < fullBytes; i++)
            {
                if (ipBytes[i] != baseBytes[i])
                    return false;
            }

            if (remainingBits > 0)
            {
                var mask = (byte)(0xFF << (8 - remainingBits));
                if ((ipBytes[fullBytes] & mask) != (baseBytes[fullBytes] & mask))
                    return false;
            }

            return true;
        }
    }
}