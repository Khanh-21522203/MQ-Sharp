namespace KafkaBroker.Handlers;

public class FindCoordinatorHandler : IRequestHandler
{
    private readonly int _nodeId;
    private readonly string _host;
    private readonly int _port;

    public FindCoordinatorHandler(int nodeId, string host, int port)
    {
        _nodeId = nodeId;
        _host = host;
        _port = port;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        // v0: KeyType:int8 (0=group), Key:string
        var keyType = (sbyte)r.ReadInt16Be(); // nếu client gửi int8, bạn nên đọc byte; ở đây reuse int16 cho giản lược
        if (keyType != 0)
        {
            /* transactional.id không hỗ trợ */
        }

        string groupId = r.ReadKafkaString();

        ResponseWriter.WriteResponse(output, hdr.CorrelationId, w =>
        {
            // v0: ErrorCode:int16 NodeId:int32 Host:string Port:int32
            w.WriteInt16BE(ErrorCodes.None);
            w.WriteInt32BE(_nodeId);
            w.WriteKafkaString(_host);
            w.WriteInt32BE(_port);
        });
    }
}