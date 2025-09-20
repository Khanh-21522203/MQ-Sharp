namespace MQ_Sharp.Consumer;

public class FetchedDataChunk: IEquatable<FetchedDataChunk>
{
    public BufferedMessageSet Messages { get; set; }

    public PartitionTopicInfo TopicInfo { get; set; }

    public long FetchOffset { get; set; }

    public FetchedDataChunk(BufferedMessageSet messages, PartitionTopicInfo topicInfo, long fetchOffset)
    {
        this.Messages = messages;
        this.TopicInfo = topicInfo;
        this.FetchOffset = fetchOffset;
    }

    public override bool Equals(object obj)
    {
        FetchedDataChunk other = obj as FetchedDataChunk;
        if (other == null)
        {
            return false;
        }
        else
        {
            return this.Equals(other);
        }
    }

    public bool Equals(FetchedDataChunk other)
    {
        return this.Messages == other.Messages &&
               this.TopicInfo == other.TopicInfo &&
               this.FetchOffset == other.FetchOffset;
    }

    public override int GetHashCode()
    {
        return this.Messages.GetHashCode() ^ this.TopicInfo.GetHashCode() ^ this.FetchOffset.GetHashCode();
    }
}