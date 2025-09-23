namespace KafkaBroker.LogStorage.Interface;

public interface IOffsetIntrospect
{
    long GetEarliestOffset(); // sau retention (base offset của segment đầu còn lại)
    long GetLatestOffset(); // "end" hiện tại (offset tiếp theo để append)
    long FindOffsetByTimestamp(long timestampMs); // cần time index; tạm NotSupported nếu chưa làm
}