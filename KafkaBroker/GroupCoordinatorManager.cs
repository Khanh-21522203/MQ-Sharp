using KafkaBroker.Responses;
using KafkaBroker.Utils;

namespace KafkaBroker;

public interface IGroupCoordinatorManager
{
    /// <summary>
    /// Returns the coordinator broker for the given consumer group.
    /// Single-broker: always this node.
    /// </summary>
    GroupCoordinatorResponse FindCoordinator(string groupId);
}

public sealed class GroupCoordinatorManager(int nodeId, string host, int port) : IGroupCoordinatorManager
{
    public GroupCoordinatorResponse FindCoordinator(string groupId)
    {
        return new GroupCoordinatorResponse(
            ErrorCode: (short)KafkaErrorCode.NoError,
            CoordinatorId: nodeId,
            CoordinatorHost: host,
            CoordinatorPort: port
        );
    }
}
