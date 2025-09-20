namespace KafkaBroker.Handlers;

public class ListOffsetsHandler : IRequestHandler
{
    private readonly LogManager _logs;

    public ListOffsetsHandler(LogManager logs)
    {
        _logs = logs;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        int replicaId = r.ReadInt32Be(); // -1
        int topicCount = r.ReadInt32Be();

        var perTopic = new List<(string topic, List<(int part, short err, long[] offsets)>)>();

        for (int t = 0; t < topicCount; t++)
        {
            string topic = r.ReadKafkaString();
            int partCount = r.ReadInt32Be();
            var parts = new List<(int, short, long[])>(partCount);

            for (int p = 0; p < partCount; p++)
            {
                int partition = r.ReadInt32Be();
                long time = r.ReadInt64Be();
                int maxNum = r.ReadInt32Be();

                if (!_logs.TryGetLog(topic, partition, out var log))
                {
                    parts.Add((partition, ErrorCodes.UnknownTopicOrPartition, Array.Empty<long>()));
                    continue;
                }

                long off;
                if (time == -2) off = log!.EarliestOffset;
                else if (time == -1) off = log!.LatestOffset;
                else
                {
                    // timestamp lookup chưa hỗ trợ: trả latest
                    off = log!.LatestOffset;
                }

                var arr = new long[] { off };
                parts.Add((partition, ErrorCodes.None, arr));
            }

            perTopic.Add((topic, parts));
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
                    // Offsets array
                    w.WriteInt32BE(pr.offsets.Length);
                    foreach (var off in pr.offsets) w.WriteInt64BE(off);
                }
            }
        });
    }
}