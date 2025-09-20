namespace KafkaBroker.Handlers;

sealed class OffsetCommitHandler : IRequestHandler
{
    private readonly GroupCoordinator _coord;

    public OffsetCommitHandler(GroupCoordinator coord)
    {
        _coord = coord;
    }

    public void Handle(RequestHeader hdr, KafkaBinaryReader r, Stream output)
    {
        string groupId = r.ReadKafkaString();
        int generationId = r.ReadInt32Be(); // có thể bỏ qua
        string memberId = r.ReadKafkaString(); // có thể bỏ qua
        int topicCount = r.ReadInt32Be();

        var commits = new List<(string, int, long)>();
        for (int i = 0; i < topicCount; i++)
        {
            string topic = r.ReadKafkaString();
            int partCount = r.ReadInt32Be();
            for (int p = 0; p < partCount; p++)
            {
                int partition = r.ReadInt32Be();
                long offset = r.ReadInt64Be();
                // v0 có thể có timestamp & metadata; giản lược bỏ qua
                commits.Add((topic, partition, offset));
            }
        }

        _coord.CommitOffsets(groupId, commits.Select(c => (c.Item1, c.Item2, c.Item3)));

        // Response: [Topic [Partition ErrorCode]]
        ResponseWriter.WriteResponse(output, hdr.CorrelationId, w =>
        {
            var byTopic = commits.GroupBy(c => c.Item1);
            w.WriteInt32BE(byTopic.Count());
            foreach (var g in byTopic)
            {
                w.WriteKafkaString(g.Key);
                w.WriteInt32BE(g.Count());
                foreach (var c in g)
                {
                    w.WriteInt32BE(c.Item2);
                    w.WriteInt16BE(ErrorCodes.None);
                }
            }
        });
    }
}