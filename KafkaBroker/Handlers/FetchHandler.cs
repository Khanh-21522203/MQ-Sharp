namespace KafkaBroker.Handlers;

public class FetchHandler : IRequestHandler
{
    private readonly LogManager _logs;

    public FetchHandler(LogManager logs)
    {
        _logs = logs;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        int replicaId = r.ReadInt32Be(); // -1 from clients
        int maxWaitMs = r.ReadInt32Be(); // ignored in minimal impl
        int minBytes = r.ReadInt32Be(); // ignored in minimal impl

        int topicCount = r.ReadInt32Be();
        var perTopic = new List<(string topic, List<(int part, short err, long hwm, byte[] setBytes)>)>();

        for (int t = 0; t < topicCount; t++)
        {
            string topic = r.ReadKafkaString();
            int partCount = r.ReadInt32Be();
            var partList = new List<(int, short, long, byte[])>(partCount);

            for (int p = 0; p < partCount; p++)
            {
                int partition = r.ReadInt32Be();
                long fetchOffset = r.ReadInt64Be();
                int maxBytes = r.ReadInt32Be();

                if (!_logs.TryGetLog(topic, partition, out var log))
                {
                    partList.Add((partition, ErrorCodes.UnknownTopicOrPartition, 0L, Array.Empty<byte>()));
                    continue;
                }

                var hwm = log!.LatestOffset;
                var read = log.Read(fetchOffset, maxBytes);
                if (read == null)
                {
                    // offset out of range hoặc không có dữ liệu ở offset đó => trả empty với error nếu offset > hwm
                    short err = fetchOffset > hwm ? ErrorCodes.OffsetOutOfRange : ErrorCodes.None;
                    partList.Add((partition, err, hwm, Array.Empty<byte>()));
                }
                else
                {
                    partList.Add((partition, ErrorCodes.None, hwm, read.Value.bytes));
                }
            }

            perTopic.Add((topic, partList));
        }

        ResponseWriter.WriteResponse(output, hdr.CorrelationId, w =>
        {
            w.WriteInt32BE(perTopic.Count);
            foreach (var t in perTopic)
            {
                w.WriteKafkaString(t.topic);
                w.WriteInt32BE(t.Item2.Count);
                foreach (var pr in t.Item2)
                {
                    w.WriteInt32BE(pr.part);
                    w.WriteInt16BE(pr.err);
                    w.WriteInt64BE(pr.hwm);
                    w.WriteInt32BE(pr.setBytes.Length);
                    w.WriteBytes(pr.setBytes);
                }
            }
        });
    }
}