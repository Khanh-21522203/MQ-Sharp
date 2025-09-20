using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using MQ_Sharp.Exceptions;
using MQ_Sharp.Request;
using MQ_Sharp.Response;
using MQ_Sharp.Serialization;
using MQ_Sharp.Utils;

namespace MQ_Sharp;

public class KafkaConnection: IKafkaConnection
{
    private static readonly log4net.ILog Logger = log4net.LogManager.GetLogger(typeof(KafkaConnection));
    
    private bool _disposed;
    private readonly Lock _sendLock = new Lock();

    private readonly string _server;
    private readonly int _port;
    private readonly int _bufferSize;
    private readonly int _sendTimeoutMs;
    private readonly int _receiveTimeoutMs;
    private readonly int _networkStreamReadTimeoutMs;
    private readonly int _networkStreamWriteTimeoutMs;
    private readonly long _reconnectIntervalMs;
    
    private Socket? _socket;
    private KafkaBinaryReader? _reader;
    private NetworkStream? _stream;
    private int _lastActiveTimeMs;

    public KafkaConnection(
        string server,
        int port,
        int bufferSize,
        int sendTimeoutMs,
        int receiveTimeoutMs,
        int reconnectIntervalMs,
        int networkStreamReadTimeoutMs = 60 * 1000,
        int networkStreamWriteTimeoutMs = 60 * 1000)
    {
        _server = server;
        _port = port;
        _bufferSize = bufferSize;
        _sendTimeoutMs = sendTimeoutMs;
        _receiveTimeoutMs = receiveTimeoutMs;
        _reconnectIntervalMs = reconnectIntervalMs;
        _networkStreamReadTimeoutMs = networkStreamReadTimeoutMs;
        _networkStreamWriteTimeoutMs = networkStreamWriteTimeoutMs;
        Connect();
    }
    bool Connected =>
        _socket is { Connected: true } && 
        (Environment.TickCount - _lastActiveTimeMs) <= _reconnectIntervalMs;

    private static IEnumerable<IPAddress> Resolve(string host) =>
        IPAddress.TryParse(host, out var ip) ? [ip] : Dns.GetHostAddresses(host);

    private Socket NewSocket(AddressFamily af) => new(af, SocketType.Stream, ProtocolType.Tcp)
    {
        NoDelay = true,
        ReceiveTimeout = _receiveTimeoutMs,
        SendTimeout = _sendTimeoutMs,
        SendBufferSize = _bufferSize,
        ReceiveBufferSize = _bufferSize
    };

    private static bool TryConnect(Socket s, IPAddress addr, int port, int timeoutMs)
    {
        var ar = s.BeginConnect(addr, port, null, null);
        var ok = ar.AsyncWaitHandle.WaitOne(timeoutMs, true);
        ar.AsyncWaitHandle.Close();
        return ok && s.Connected;
    }
    
    private void Connect()
    {
        var watch = Stopwatch.StartNew();
        CloseConnection(true);

        foreach (var addr in Resolve(_server))
        {
            if (addr.AddressFamily is not (AddressFamily.InterNetwork or AddressFamily.InterNetworkV6))
                continue;

            try
            {
                var s = NewSocket(addr.AddressFamily);
                if (!TryConnect(s, addr, _port, _receiveTimeoutMs)) { s.Close(); continue; }

                _socket = s;
                _stream = new NetworkStream(s, ownsSocket: true)
                {
                    ReadTimeout = _networkStreamReadTimeoutMs,
                    WriteTimeout = _networkStreamWriteTimeoutMs
                };
                _reader = new KafkaBinaryReader(_stream);
                _lastActiveTimeMs = Environment.TickCount;

                Logger.DebugFormat("KafkaConnection.Connect ok in {0}ms, {1}:{2}", watch.ElapsedMilliseconds, addr, _port);
                return;
            }
            catch (Exception ex)
            {
                Logger.Error($"Connect error ({addr}:{_port}) after {watch.ElapsedMilliseconds}ms", ex);
            }
        }

        Logger.ErrorFormat("UnableToConnectToHost {0}:{1} after {2}ms", _server, _port, watch.ElapsedMilliseconds);
        throw new UnableToConnectToHostException(_server, _port);
    }
    
    
    private void EnsureConnected()
    {
        if (!Connected) Connect();
    }

    private void CloseConnection(bool silent = false)
    {
        try
        {
            _socket?.Shutdown(SocketShutdown.Both);
        }
        catch when (silent) { }
        catch (Exception ex) { Logger.Warn("Shutdown error", ex); }

        try
        {
            _stream?.Close();
        }
        catch when (silent) { }
        catch (Exception ex) { Logger.Warn("Stream close error", ex); }

        _reader = null;
        _stream = null;
        _socket = null;
    }
    
    public FetchResponse Send(FetchRequest request)
    {
        EnsuresNotDisposed();
        Guard.NotNull(request, "request");
        return Handle(request.RequestBuffer.GetBuffer(), new FetchResponse.Parser());
    }

    public ProduceResponse Send(ProduceRequest request)
    {
        EnsuresNotDisposed();
        Guard.NotNull(request, "request");
        return Handle(request.RequestBuffer.GetBuffer(), new ProduceResponse.Parser(), request.RequiredAcks != 0);
    }

    public OffsetResponse Send(ListOffsetRequest request)
    {
        EnsuresNotDisposed();
        Guard.NotNull(request, "request");
        return Handle(request.RequestBuffer.GetBuffer(), new OffsetResponse.Parser());    
    }

    public IEnumerable<MetaDataResponse> Send(TopicMetadataRequest request)
    {
        EnsuresNotDisposed();
        Guard.NotNull(request, "request");
        return Handle(request.RequestBuffer.GetBuffer(), new TopicMetadataRequest.Parser());
    }
    
    private void EnsuresNotDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
    private T? Handle<T>(byte[] data, IResponseParser<T> parser, bool shouldParse = true)
    {
        try
        {
            lock (_sendLock)
            {
                EnsureConnected();
                _stream!.Write(data, 0, data.Length);
                _stream.Flush();

                var resp = shouldParse ? parser.ParseFrom(_reader!) : default;
                _lastActiveTimeMs = Environment.TickCount;
                return resp;
            }
        }
        catch (Exception e)
        {
            if (e is IOException || e is SocketException || e is InvalidOperationException)
                _socket = null;
            throw;
        }
    }
    
    
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_stream != null)
        {
            CloseConnection();
        }
    }
}