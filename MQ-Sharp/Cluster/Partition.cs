namespace MQ_Sharp.Cluster;

public class Partition(
    string topic,
    int partId,
    Replica? leader = null,
    HashSet<Replica>? assignedReplicas = null,
    HashSet<Replica>? inSyncReplica = null,
    HashSet<Replica>? catchUpReplicas = null,
    HashSet<Replica>? reassignedReplicas = null)
{
    public int PartId { get; private set; } = partId;
    public string Topic { get; private set; } = topic;
    public Replica? Leader { get; set; } = leader;

    public HashSet<Replica> AssignedReplicas { get; private set; } = assignedReplicas ?? new HashSet<Replica>();

    public HashSet<Replica> InSyncReplicas { get; private set; } = inSyncReplica ?? new HashSet<Replica>();

    public HashSet<Replica> CatchUpReplicas { get; private set; } = catchUpReplicas ?? new HashSet<Replica>();

    public HashSet<Replica> ReassignedReplicas { get; private set; } = reassignedReplicas ?? new HashSet<Replica>();

    public override string ToString()
    {
        return
            $"Topic={Topic},PartId={PartId},LeaderBrokerId={(Leader == null ? "NA" : Leader.BrokerId.ToString())}";
    }
}