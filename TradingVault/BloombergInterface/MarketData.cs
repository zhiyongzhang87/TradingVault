using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BloombergInterface
{
    public class MarketData
    {
        private double mNoValueDouble;
        private DateTime mNoTime;
        private object mChangeValueLock;
        private bool mIsDataChanged;
        private string mTicker;
        private string mBbgTicker;
        private double mBid;
        private double mAsk;
        private double mLastTradePrice;
        private double mBidSize;
        private double mAskSize;
        private double mLastTradeSize;
        private double mCumulativeSizeOnLastTradePrice;
        private DateTime mBidUpdateTime;
        private DateTime mAskUpdateTime;
        private DateTime mLastTradeTime;

        public MarketData(string argvTicker, string argvBbgTicker)
        {
            mNoValueDouble = -999999;
            mNoTime = DateTime.MinValue;
            mChangeValueLock = new object();
            mIsDataChanged = false;
            mTicker = argvTicker;
            mBbgTicker = argvBbgTicker;
            mBid = this.mNoValueDouble;
            mAsk = this.mNoValueDouble;
            mLastTradePrice = this.mNoValueDouble;
            mBidSize = this.mNoValueDouble;
            mAskSize = this.mNoValueDouble;
            mLastTradeSize = this.mNoValueDouble;
            mCumulativeSizeOnLastTradePrice = this.mNoValueDouble;
            mBidUpdateTime = this.mNoTime;
            mAskUpdateTime = this.mNoTime;
            mLastTradeTime = this.mNoTime;
        }

        #region Get functions
        public double NoValueDouble()
        {
            return mNoValueDouble;
        }

        public DateTime NoTime()
        {
            return mNoTime;
        }

        public bool IsDataChanged()
        {
            return mIsDataChanged;
        }

        public string Ticker()
        {
            return mTicker;
        }

        public string BbgTicker()
        {
            return mBbgTicker;
        }

        public double Bid()
        {
            return mBid;
        }

        public double Ask()
        {
            return mAsk;
        }

        public double LastTradePrice()
        {
            return mLastTradePrice;
        }

        public double BidSize()
        {
            return mBidSize;
        }

        public double AskSize()
        {
            return mAskSize;
        }

        public double LastTradeSize()
        {
            return mLastTradeSize;
        }

        public double CumulativeSizeOnLastTradePrice()
        {
            return mCumulativeSizeOnLastTradePrice;
        }

        public DateTime BidUpdateTime()
        {
            return mBidUpdateTime;
        }

        public DateTime AskUpdateTime()
        {
            return mAskUpdateTime;
        }

        public DateTime LastTradeTime()
        {
            return mLastTradeTime;
        }
        #endregion

        #region Set functions
        public bool SetBid(double argvBid, double argvBidSize, DateTime argvBidUpdateTime)
        {
            bool tValueIsChanged = false;

            lock (mChangeValueLock)
            {
                if (argvBid != this.mNoValueDouble && (mBid != argvBid || mBidSize != argvBidSize))
                {
                    tValueIsChanged = true;
                    mIsDataChanged = mIsDataChanged || tValueIsChanged;
                    mBid = argvBid;
                    mBidSize = argvBidSize;
                    mBidUpdateTime = argvBidUpdateTime;
                }
            }

            return tValueIsChanged;
        }

        public bool SetAsk(double argvAsk, double argvAskSize, DateTime argvAskUpdateTime)
        {
            bool tValueIsChanged = false;

            lock (mChangeValueLock)
            {
                if (argvAsk != this.mNoValueDouble && (mAsk != argvAsk || mAskSize != argvAskSize))
                {
                    tValueIsChanged = true;
                    mIsDataChanged = mIsDataChanged || tValueIsChanged;
                    mAsk = argvAsk;
                    mAskSize = argvAskSize;
                    mAskUpdateTime = argvAskUpdateTime;
                }
            }

            return tValueIsChanged;
        }

        public bool SetLastTrade(double argvLastTradePrice, double argvLastTradeSize, DateTime argvLastTradeTime)
        {
            bool tValueIsChanged = false;

            if (argvLastTradePrice != this.mNoValueDouble)
            {
                lock (mChangeValueLock)
                {
                    if (mLastTradePrice != argvLastTradePrice)
                    {
                        tValueIsChanged = true;
                        mCumulativeSizeOnLastTradePrice = 0;
                    }

                    if (tValueIsChanged || mLastTradeSize != argvLastTradeSize || mLastTradeTime != argvLastTradeTime)
                    {
                        tValueIsChanged = true;
                        mIsDataChanged = mIsDataChanged || tValueIsChanged;
                        mLastTradePrice = argvLastTradePrice;
                        mLastTradeSize = argvLastTradeSize;
                        mLastTradeTime = argvLastTradeTime;
                        mCumulativeSizeOnLastTradePrice += argvLastTradeSize;
                    }
                }
            }

            return tValueIsChanged;
        }

        //Reset mIsDataChanged after change is consumed
        public void ChangeIsRead()
        {
            lock (mChangeValueLock)
            {
                mIsDataChanged = false;
            }
        }

        //Return value indicates if data is changed
        public bool UpdateMarketData(System.Data.DataTable argvData)
        {
            double tBid = this.NoValueDouble();
            double tBidSize = this.NoValueDouble();
            DateTime tBidUpdateTime = this.NoTime();
            double tAsk = this.NoValueDouble();
            double tAskSize = this.NoValueDouble();
            DateTime tAskUpdateTime = this.NoTime();
            double tLastTradePrice = this.NoValueDouble();
            double tLastTradeSize = this.NoValueDouble();
            DateTime tLastTradeTime = this.NoTime();

            if ((argvData.Rows[0]["BID"].ToString() + argvData.Rows[0]["BID_SIZE"].ToString() + argvData.Rows[0]["LAST_UPDATE_BID_RT"].ToString()).Length > 0)
            {
                if (argvData.Rows[0]["BID"].ToString().Length > 0)
                {
                    tBid = double.Parse(argvData.Rows[0]["BID"].ToString());
                }

                if (argvData.Rows[0]["BID_SIZE"].ToString().Length > 0)
                {
                    tBidSize = double.Parse(argvData.Rows[0]["BID_SIZE"].ToString());
                }

                if (argvData.Rows[0]["LAST_UPDATE_BID_RT"].ToString().Length > 0)
                {
                    tBidUpdateTime = DateTime.ParseExact(argvData.Rows[0]["LAST_UPDATE_BID_RT"].ToString(), "yyyyMMdd HHmmss.fff", System.Globalization.CultureInfo.InvariantCulture);
                }

                this.SetBid(tBid, tBidSize, tBidUpdateTime);
            }

            if ((argvData.Rows[0]["ASK"].ToString() + argvData.Rows[0]["ASK_SIZE"].ToString() + argvData.Rows[0]["LAST_UPDATE_ASK_RT"].ToString()).Length > 0)
            {
                if (argvData.Rows[0]["ASK"].ToString().Length > 0)
                {
                    tAsk = double.Parse(argvData.Rows[0]["ASK"].ToString());
                }

                if (argvData.Rows[0]["ASK_SIZE"].ToString().Length > 0)
                {
                    tAskSize = double.Parse(argvData.Rows[0]["ASK_SIZE"].ToString());
                }

                if (argvData.Rows[0]["LAST_UPDATE_ASK_RT"].ToString().Length > 0)
                {
                    tAskUpdateTime = DateTime.ParseExact(argvData.Rows[0]["LAST_UPDATE_ASK_RT"].ToString(), "yyyyMMdd HHmmss.fff", System.Globalization.CultureInfo.InvariantCulture);
                }

                this.SetAsk(tAsk, tAskSize, tAskUpdateTime);
            }

            if ((argvData.Rows[0]["LAST_TRADE"].ToString() + argvData.Rows[0]["SIZE_LAST_TRADE"].ToString() + argvData.Rows[0]["TRADE_UPDATE_STAMP_RT"].ToString()).Length > 0)
            {
                if (argvData.Rows[0]["LAST_TRADE"].ToString().Length > 0)
                {
                    tLastTradePrice = double.Parse(argvData.Rows[0]["LAST_TRADE"].ToString());
                }

                if (argvData.Rows[0]["SIZE_LAST_TRADE"].ToString().Length > 0)
                {
                    tLastTradeSize = double.Parse(argvData.Rows[0]["SIZE_LAST_TRADE"].ToString());
                }

                if (argvData.Rows[0]["TRADE_UPDATE_STAMP_RT"].ToString().Length > 0)
                {
                    tLastTradeTime = DateTime.ParseExact(argvData.Rows[0]["TRADE_UPDATE_STAMP_RT"].ToString(), "yyyyMMdd HHmmss.fff", System.Globalization.CultureInfo.InvariantCulture);
                }

                this.SetLastTrade(tLastTradePrice, tLastTradeSize, tLastTradeTime);
            }

            return mIsDataChanged;
        }
        #endregion
    }
}
