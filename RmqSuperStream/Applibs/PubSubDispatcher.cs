
namespace RmqSuperStream.Applibs
{
    using System;
    using System.Threading.Tasks;
    using Autofac.Features.Indexed;
    using RmqSuperStream.Model;

    /// <summary>
    /// 調度員實例
    /// </summary>
    public class PubSubDispatcher<TEventStream> : IPubSubDispatcher<TEventStream>
            where TEventStream : EventStream
    {
        private IIndex<string, IPubSubHandler<TEventStream>> handerSets;

        /// <summary>
        /// 發生錯誤時 callback回寫
        /// </summary>
        private Action<string> errorCallBack;

        /// <summary>
        /// 建構子
        /// </summary>
        /// <param name="handerSets"></param>
        /// <param name="errorCallBack"></param>
        public PubSubDispatcher(IIndex<string, IPubSubHandler<TEventStream>> handerSets, Action<string> errorCallBack)
        {
            this.handerSets = handerSets;
            this.errorCallBack = errorCallBack;
        }

        /// <summary>
        /// 調度事件
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public async Task<bool> DispatchMessageAsync(TEventStream stream)
        {
            try
            {
                if (this.handerSets.TryGetValue(stream.Type, out var handler))
                {
                    return await handler.HandleAsync(stream);
                }

                return true;
            }
            catch (Exception ex)
            {
                this.errorCallBack($"DispatchMessage Exception:{ex.Message}");
                return false;
            }
        }
    }
}
