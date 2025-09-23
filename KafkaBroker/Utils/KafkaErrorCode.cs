namespace KafkaBroker.Utils;

public enum KafkaErrorCode : short
{
    NoError = 0, // No error--it worked!
    Unknown = -1, // Unexpected server error

    OffsetOutOfRange = 1, // The requested offset is outside the valid range
    InvalidMessage = 2, // Message CRC does not match / message corrupted
    UnknownTopicOrPartition = 3, // Topic or partition does not exist on this broker
    InvalidMessageSize = 4, // Message has negative size
    LeaderNotAvailable = 5, // No leader (election in progress)
    NotLeaderForPartition = 6, // Client sent to a replica that is not leader
    RequestTimedOut = 7, // Request exceeded user-specified timeout
    BrokerNotAvailable = 8, // Broker not alive (mostly used internally)
    ReplicaNotAvailable = 9, // Expected replica missing on this broker
    MessageSizeTooLarge = 10, // Message exceeds max allowed size
    StaleControllerEpoch = 11, // Internal broker-to-broker error
    OffsetMetadataTooLarge = 12, // Offset metadata string too large

    GroupLoadInProgress = 14, // Group metadata is still loading
    GroupCoordinatorNotAvailable = 15, // Coordinator not active / offsets topic missing
    NotCoordinatorForGroup = 16, // This broker is not the coordinator for the group
    InvalidTopic = 17, // Invalid topic name or attempt to write internal topic
    RecordListTooLarge = 18, // Message batch exceeds segment size
    NotEnoughReplicas = 19, // In-sync replicas < minISR and requiredAcks = -1
    NotEnoughReplicasAfterAppend = 20, // Written but ISR count < minISR
    InvalidRequiredAcks = 21, // requiredAcks not one of -1, 0, 1
    IllegalGeneration = 22, // Generation ID in request not current
    InconsistentGroupProtocol = 23, // Provided protocol(s) incompatible with group
    InvalidGroupId = 24, // GroupId is empty or null
    UnknownMemberId = 25, // MemberId not found in current generation
    InvalidSessionTimeout = 26, // Session timeout outside allowed range
    RebalanceInProgress = 27, // Coordinator has begun rebalancing
    InvalidCommitOffsetSize = 28, // Commit offset metadata too large
    TopicAuthorizationFailed = 29, // Client not authorized for topic
    GroupAuthorizationFailed = 30, // Client not authorized for group
    ClusterAuthorizationFailed = 31 // Client not authorized for cluster
}