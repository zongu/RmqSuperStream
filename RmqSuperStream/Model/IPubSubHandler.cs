
namespace RmqSuperStream.Model
{
    using System.Threading.Tasks;

    /// <summary>
    /// 處裡事件介面
    /// </summary>
    /// <typeparam name="TEventStream"></typeparam>
    public interface IPubSubHandler<TEventStream>
        where TEventStream : EventStream
    {
        /// <summary>
        /// 處裡事件內容
        /// </summary>
        /// <param name="stream">事件流</param>
        Task<bool> HandleAsync(TEventStream stream);
    }
}
