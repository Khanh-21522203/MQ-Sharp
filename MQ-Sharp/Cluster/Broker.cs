using System.Globalization;
using System.Text;
using System.Text.Json;
using MQ_Sharp.Request;
using MQ_Sharp.Serialization;
using MQ_Sharp.Utils;

namespace MQ_Sharp.Cluster;

public sealed class Broker(int id, string host, int port)
{
    public int Id { get; private set; } = id;
    public string Host { get; private set; } = host;
    public int Port { get; private set; } = port;
}