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
        public System.Data.DataTable mData;
        public Bloomberglp.Blpapi.Element mBbgMsg;

        public InterfaceEventArgs(xBbgMsgType argvMsgType)
        {
            mMsgType = argvMsgType;
            mBbgMsg = null;
            mData = new System.Data.DataTable();
            mMsg = string.Empty;
        }

        public InterfaceEventArgs(xBbgMsgType argvMsgType, string argvMsg)
        {
            mMsgType = argvMsgType;
            mMsg = argvMsg;
            mBbgMsg = null;
            mData = new System.Data.DataTable();
        }

        public void AddData(KeyValuePair<string, string> argvKey, Dictionary<string, string> argvValues)
        {
            if (!mData.Columns.Contains(argvKey.Key))
            {
                this.AddColumn(argvKey.Key, "");
            }

            foreach(string tValueLable in argvValues.Keys)
            {
                if (!mData.Columns.Contains(tValueLable))
                {
                    this.AddColumn(tValueLable, "");
                }
            }

            System.Data.DataRow[] tRowFound = mData.Select(argvKey.Key + " = '" + argvKey.Value + "'");
            if(tRowFound.Count() == 0)
            {
                System.Data.DataRow tNewRow = mData.NewRow();
                tNewRow[argvKey.Key] = argvKey.Value;
                foreach (string tValueLable in argvValues.Keys)
                {
                    tNewRow[tValueLable] = argvValues[tValueLable];
                }
                mData.Rows.Add(tNewRow);
            }
            else
            {
                foreach (string tValueLable in argvValues.Keys)
                {
                    tRowFound[0][tValueLable] = argvValues[tValueLable];
                }
            }
        }

        private void AddColumn(string argvColumnHead, string argvDefaultValue)
        {
            System.Data.DataColumn tNewColumn = new System.Data.DataColumn(argvColumnHead);
            tNewColumn.DefaultValue = argvDefaultValue;
            mData.Columns.Add(tNewColumn);
        }
    }
}
