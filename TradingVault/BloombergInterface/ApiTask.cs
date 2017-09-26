using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BloombergInterface
{
    public class ApiTask
    {
        public string mTicker;
        public string mBbgTicker;
        public string mFields;

        public ApiTask(string argvTicker, string argvBbgTicker, string argvFilds)
        {
            mTicker = argvTicker;
            mBbgTicker = argvBbgTicker;
            mFields = argvFilds;
        }

        public string[] GetFieldList()
        {
            string[] tRet = mFields.Split(',');
            return tRet;
        }
    }

    public class ApiTaskHistoricalData : ApiTask
    {
        public DateTime mStartTime;
        public DateTime mEndTime;

        public ApiTaskHistoricalData(string argvTicker, string argvBbgTicker, string argvFilds, DateTime argvStartTime, DateTime argvEndTime) : base(argvTicker, argvBbgTicker, argvFilds)
        {
            mStartTime = argvStartTime;
            mEndTime = argvEndTime;
        }
    }

    public class ApiTaskRealtime : ApiTask
    {
        public int mDataDeliverInterval;

        public ApiTaskRealtime(string argvTicker, string argvBbgTicker, string argvFilds, int argvDataDeliverInterval) : base(argvTicker, argvBbgTicker, argvFilds)
        {
            mDataDeliverInterval = argvDataDeliverInterval;
        }
    }

    public class ApiTaskBar : ApiTask
    {
        public DateTime mStartTime;
        public DateTime mEndTime;
        public int mBarLengthInMinute;

        public ApiTaskBar(string argvTicker, string argvBbgTicker, string argvFilds, DateTime argvStartTime, DateTime argvEndTime, int argvBarLengthInMinute) : base(argvTicker, argvBbgTicker, argvFilds)
        {
            mStartTime = argvStartTime;
            mEndTime = argvEndTime;
            mBarLengthInMinute = argvBarLengthInMinute;
        }
    }
}
