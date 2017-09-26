using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BloombergInterface
{
    public class BloombergData
    {
        private string mTicker;
        private Dictionary<string, Dictionary<string, string>> mData; //<field, <time, data>>
        private object mLock;

        public BloombergData(string argvTicker)
        {
            mLock = new object();
            mTicker = argvTicker;
            mData = new Dictionary<string, Dictionary<string, string>>();
        }

        public void Update(string argvField, string argvTime, string argvValue)
        {
            lock (mLock)
            {
                if (mData.ContainsKey(argvField))
                {
                    if (mData[argvField].ContainsKey(argvTime))
                    {
                        mData[argvField][argvTime] = argvValue;
                    }
                    else
                    {
                        mData[argvField].Add(argvTime, argvValue);
                    }
                }
                else
                {
                    mData.Add(argvField, new Dictionary<string, string>());
                    mData[argvField].Add(argvTime, argvValue);
                }
            }
        }

        public string GetTicker()
        {
            return mTicker;
        }

        public Dictionary<string, Dictionary<string, string>> GetValue()
        {
            Dictionary<string, Dictionary<string, string>> tRet = new Dictionary<string, Dictionary<string, string>>();
            lock (mLock)
            {
                foreach (string tKey in mData.Keys)
                {
                    tRet.Add(tKey, new Dictionary<string, string>());
                    foreach (string tSubKey in mData[tKey].Keys)
                    {
                        tRet[tKey].Add(tSubKey, mData[tKey][tSubKey]);
                    }
                }
            }
            return tRet;
        }

        public Dictionary<string, string> GetValue(string argvField)
        {
            Dictionary<string, string> tRet = new Dictionary<string, string>();
            lock (mLock)
            {
                if (mData.ContainsKey(argvField))
                {
                    foreach (string tKey in mData[argvField].Keys)
                    {
                        tRet.Add(tKey, mData[argvField][tKey]);
                    }
                }
            }
            return tRet;
        }

        public string GetValue(string argvField, string argvTime)
        {
            string tRet = "-1";
            lock (mLock)
            {
                if (mData.ContainsKey(argvField))
                {
                    if (mData[argvField].ContainsKey(argvTime))
                    {
                        tRet = mData[argvField][argvTime];
                    }
                }
            }
            return tRet;
        }
    }
}
