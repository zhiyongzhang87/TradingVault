using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Bloomberglp.Blpapi;

namespace BloombergInterface
{
    public class InterfaceEventArgs : EventArgs
    {
        public enum xBbgMsgType
        {
            Print,
            LoginSuccess,
            LoginFail,
            RequestResponse,
            IntradayTickResponse,
            IntradayBarResponse,
            SubscriptionResponse,
            ReferenceDataResponse,
            Error
        }

        public xBbgMsgType mMsgType;
        public string mMsg;
        public Dictionary<string, BloombergData> mData;
        public Bloomberglp.Blpapi.Element mBbgMsg;

        public InterfaceEventArgs(xBbgMsgType argvMsgType)
        {
            mMsgType = argvMsgType;
            mBbgMsg = null;
            mData = null;
            mMsg = string.Empty;
        }

        public InterfaceEventArgs(xBbgMsgType argvMsgType, string argvMsg)
        {
            mMsgType = argvMsgType;
            mMsg = argvMsg;
            mBbgMsg = null;
            mData = null;
        }

        public void AddData(string argvTicker)
        {
            if (!mData.ContainsKey(argvTicker))
            {
                mData.Add(argvTicker, new BloombergData(argvTicker));
            }
        }

        public void AddData(string argvTicker, string argvField, string argvTime, string argvData)
        {
            if (mData != null)
            {
                this.AddData(argvTicker);
                mData[argvTicker].Update(argvField, argvTime, argvData);
            }
        }
    }
}
