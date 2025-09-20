namespace KafkaBroker.Handlers;

sealed class OffsetFetchHandler : IRequestHandler
{
    private readonly GroupCoordinator _coord;

    public OffsetFetchHandler(GroupCoordinator coord)
    {
        _coord = coord;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        string groupId = r.ReadKafkaString();
        int topicCount = r.ReadInt32Be();

        var tps = new List<(string topic, int partition)>();
        var topicIndex = new List<(string topic, int count)>();

        for (int i = 0; i < topicCount; i++)
        {
            string topic = r.ReadKafkaString();
            int pcount = r.ReadInt32Be();
            topicIndex.Add((topic, pcount));
            for (int p = 0; p < pcount; p++)
            {
                int partition = r.ReadInt32Be();
                tps.Add((topic, partition));
            }
        }

        var offs = _coord.FetchOffsets(groupId, tps);

        // Response v0-ish: [Topic [Partition Offset Metadata ErrorCode]]
        ResponseWriter.WriteResponse(output, hdr.CorrelationId, w =>
        {
            int idx = 0;
            w.WriteInt32BE(topicIndex.Count);
            foreach (var (topic, pcount) in topicIndex)
            {
                w.WriteKafkaString(topic);
                w.WriteInt32BE(pcount);
                for (int p = 0; p < pcount; p++, idx++)
                {
                    var tp = tps[idx];
                    w.WriteInt32BE(tp.partition);
                    long off = offs.TryGetValue(tp, out var o) ? o : -1;
                    w.WriteInt64BE(off);
                    w.WriteKafkaString(""); // metadata string (optional)
                    w.WriteInt16BE(ErrorCodes.None);
                }
            }
        });
    }
}