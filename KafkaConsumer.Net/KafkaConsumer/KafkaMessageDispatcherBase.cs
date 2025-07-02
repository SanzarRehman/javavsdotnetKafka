namespace KafkaConsumer;

using System.Threading;
using System.Threading.Tasks;

public abstract class KafkaMessageDispatcherBase<TKey, TValue>
{
    public abstract Task StartAsync(CancellationToken cancellationToken);
    public abstract ValueTask HandleAsync(TKey key, TValue message);
}