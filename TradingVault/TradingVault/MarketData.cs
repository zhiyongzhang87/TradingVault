using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TradingVault
{
    public class MarketData
    {
        private object mChangeValueLock;
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
            mChangeValueLock = new object();
            mTicker = argvTicker;
            mBbgTicker = argvBbgTicker;
            mBid = -999999;
            mAsk = -999999;
            mLastTradePrice = -999999;
            mBidSize = -1;
            mAskSize = -1;
            mLastTradeSize = -1;
            mCumulativeSizeOnLastTradePrice = -1;
            mBidUpdateTime = DateTime.MinValue;
            mAskUpdateTime = DateTime.MinValue;
            mLastTradeTime = DateTime.MinValue;
        }

        #region Get functions
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
                if (mBid != argvBid || mBidSize != argvBidSize)
                {
                    tValueIsChanged = true;
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
                if (mAsk != argvAsk || mAskSize != argvAskSize)
                {
                    tValueIsChanged = true;
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
                    mLastTradePrice = argvLastTradePrice;
                    mLastTradeSize = argvLastTradeSize;
                    mLastTradeTime = argvLastTradeTime;
                    mCumulativeSizeOnLastTradePrice += argvLastTradeSize;
                }
            }

            return tValueIsChanged;
        }
        #endregion
    }
}
