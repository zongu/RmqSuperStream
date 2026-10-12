
namespace RmqSuperStream.Tests.Event
{
    /// <summary>
    /// 回呼事件
    /// </summary>
    public class EchoEvent
    {
        /// <summary>
        /// 內容
        /// </summary>
        public string Content { get; set; }

        /// <summary>
        /// 計數器
        /// </summary>
        public int Counter { get; set; }
    }
}
