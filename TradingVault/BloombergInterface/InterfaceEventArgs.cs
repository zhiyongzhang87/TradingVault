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
            HistoricalDataResponse,
            IntradayTickResponse,
            IntradayBarResponse,
            SubscriptionResponse,
            ReferenceDataResponse,
            Error
        }

        public xBbgMsgType mMsgType;
        public string mMsg;
        public long mCorrelationId;
        public System.Data.DataTable mData;

        public InterfaceEventArgs(xBbgMsgType argvMsgType)
        {
            mMsgType = argvMsgType;
            mCorrelationId = -1;
            mData = new System.Data.DataTable();
            mMsg = string.Empty;
        }

        public InterfaceEventArgs(xBbgMsgType argvMsgType, string argvMsg)
        {
            mMsgType = argvMsgType;
            mMsg = argvMsg;
            mData = new System.Data.DataTable();
        }

        public void AddData(Dictionary<string, string> argvValues)
        {
            foreach (string tValueLable in argvValues.Keys)
            {
                if (!mData.Columns.Contains(tValueLable))
                {
                    this.AddColumn(tValueLable, "");
                }
            }

            System.Data.DataRow tNewRow = mData.NewRow();
            foreach (string tValueLable in argvValues.Keys)
            {
                tNewRow[tValueLable] = argvValues[tValueLable];
            }
            mData.Rows.Add(tNewRow);
        }

        private void AddColumn(string argvColumnHead, string argvDefaultValue)
        {
            System.Data.DataColumn tNewColumn = new System.Data.DataColumn(argvColumnHead);
            tNewColumn.DefaultValue = argvDefaultValue;
            mData.Columns.Add(tNewColumn);
        }
    }
}
