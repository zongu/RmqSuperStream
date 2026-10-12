
namespace RmqSuperStream.Model
{
    using System.Threading.Tasks;

    /// <summary>
    /// 調度員介面
    /// </summary>
    /// <typeparam name="TEventStream"></typeparam>
    public interface IPubSubDispatcher<TEventStream>
            where TEventStream : EventStream
    {
        /// <summary>
        /// 調度事件
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        Task<bool> DispatchMessageAsync(TEventStream stream);
    }
}
