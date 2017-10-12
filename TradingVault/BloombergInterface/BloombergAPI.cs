using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Bloomberglp.Blpapi;
using ToolBox;


namespace BloombergInterface
{
    public class BloombergApi
    {
        public enum xProductTenor { Unknown, Cash, Future, Option }

        public delegate void BbgEventHandler(object sender, InterfaceEventArgs e);

        private object mLockCorrelationIdGenerator;
        private long mCurrentCorrelationId;
        private OutputHelper mOutput;
        private Session mSession;
        private Service mReferenceService;
        private Service mMktService;
        private Service mLookupService;
        private bool isSessionRunning;
        public event BbgEventHandler mBbgMsgEvent;
        private System.Threading.Thread mBbgMsgWorker;
        private Dictionary<string, Subscription> mSubscriptions;
        private Dictionary<string, MarketData> mMktData;
        private Dictionary<string, string> mTradingDates;
        private List<string> mCorrelationID;
        private double mMsgCounter;
        private string mLastIntradayTickTicker;
        private string[] mRequestResponseErrorAttributes;
        private string[] mIntradayBarResponseDefaultAttributes;
        private string[] mIntradayTickResponseDefaultAttributes;
        private string[] mHistoricalDataResponseDefaultAttributes;
        private string[] mRealTimeDataDefaultAttributes;
        private Dictionary<long, ApiTaskHistoricalData> mHistoricalDataRequestByCorrelationId;
        private Dictionary<long, ApiTaskBar> mIntradayBarRequestByCorrelationId;
        private Dictionary<long, ApiTaskHistoricalData> mIntradayTickRequestByCorrelationId;
        private Dictionary<long, ApiTaskRealtime> mSubscriptionByCorrelationId;
        private Dictionary<long, ApiTask> mOtherRequestByCorrelationId;

        public BloombergApi()
        {
            mLockCorrelationIdGenerator = new object();
            mCurrentCorrelationId = 0;
            mOutput = new OutputHelper("BloombergApi" + DateTime.Now.ToString("yyyyMMddHHmmss"));
            isSessionRunning = false;
            mSubscriptions = new Dictionary<string, Subscription>();
            mMktData = new Dictionary<string, MarketData>();
            mTradingDates = new Dictionary<string, string>();
            mCorrelationID = new List<string>();
            mMsgCounter = 0;

            mHistoricalDataRequestByCorrelationId = new Dictionary<long, ApiTaskHistoricalData>();
            mIntradayBarRequestByCorrelationId = new Dictionary<long, ApiTaskBar>();
            mIntradayTickRequestByCorrelationId = new Dictionary<long, ApiTaskHistoricalData>();
            mSubscriptionByCorrelationId = new Dictionary<long, ApiTaskRealtime>();
            mOtherRequestByCorrelationId = new Dictionary<long, ApiTask>();

            mRequestResponseErrorAttributes = new string[2];
            mRequestResponseErrorAttributes[0] = "message";
            mRequestResponseErrorAttributes[1] = "subcategory";

            mIntradayBarResponseDefaultAttributes = new string[5];
            mIntradayBarResponseDefaultAttributes[0] = "time";
            mIntradayBarResponseDefaultAttributes[1] = "open";
            mIntradayBarResponseDefaultAttributes[2] = "high";
            mIntradayBarResponseDefaultAttributes[3] = "low";
            mIntradayBarResponseDefaultAttributes[4] = "close";

            mIntradayTickResponseDefaultAttributes = new string[6];
            mIntradayTickResponseDefaultAttributes[0] = "time";
            mIntradayTickResponseDefaultAttributes[1] = "value";
            mIntradayTickResponseDefaultAttributes[2] = "size";
            mIntradayTickResponseDefaultAttributes[3] = "type";
            mIntradayTickResponseDefaultAttributes[4] = "conditionCodes";
            mIntradayTickResponseDefaultAttributes[5] = "tradeId";

            mHistoricalDataResponseDefaultAttributes = new string[6];
            mHistoricalDataResponseDefaultAttributes[0] = "security";
            mHistoricalDataResponseDefaultAttributes[1] = "date";
            mHistoricalDataResponseDefaultAttributes[2] = "OPEN";
            mHistoricalDataResponseDefaultAttributes[3] = "HIGH";
            mHistoricalDataResponseDefaultAttributes[4] = "LOW";
            mHistoricalDataResponseDefaultAttributes[5] = "LAST_PRICE";

            mRealTimeDataDefaultAttributes = new string[6];
            mRealTimeDataDefaultAttributes[0] = "LAST_UPDATE_BID_RT";
            mRealTimeDataDefaultAttributes[1] = "BID";
            mRealTimeDataDefaultAttributes[2] = "ASK";
            mRealTimeDataDefaultAttributes[3] = "LAST_UPDATE_ASK_RT";
            mRealTimeDataDefaultAttributes[4] = "LAST_TRADE";
            mRealTimeDataDefaultAttributes[5] = "TRADE_UPDATE_STAMP_RT";
        }

        private long GenerateCorrelationId()
        {
            long tReturnValue = 0;
            lock (mLockCorrelationIdGenerator)
            {
                tReturnValue = ++mCurrentCorrelationId;
            }
            return tReturnValue;
        }

        public void Connect()
        {
            bool tResult;

            // Create a SessionOptions object to hold the session parameters
            SessionOptions tSessionOptions = new SessionOptions();
            // Since this program will run on the same PC as the Bloomberg software,
            // we use “localhost” as the host name and port 8194 as the default port.
            tSessionOptions.ServerHost = "localhost";
            tSessionOptions.ServerPort = 8194;
            // Create a Session object using the sessionOptions
            try
            {
                tResult = true;
                mSession = new Session(tSessionOptions);
            }
            catch (Exception e)
            {
                tResult = false;
                mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Can't create new session: " + e.Message)));
                return;
            }

            // Start the Session
            tResult = mSession.Start();
            if (!tResult)
            {
                mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Can't start session.")));
            }
            else
            {
                // Open up the Market data Service
                tResult = mSession.OpenService("//blp/mktdata");
                // Get a reference to the service
                if (tResult)
                {
                    mMktService = mSession.GetService("//blp/mktdata");
                }
                else
                {
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Can't open mktdata service.")));
                }
                // Open up the Reference data Service
                tResult = mSession.OpenService("//blp/refdata");
                // Get a reference to the service
                if (tResult)
                {
                    mReferenceService = mSession.GetService("//blp/refdata");
                }
                else
                {
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Can't open refdata service.")));
                }
                // Open up the Market data Service
                tResult = mSession.OpenService("//blp/instruments");
                // Get a reference to the service
                if (tResult)
                {
                    mLookupService = mSession.GetService("//blp/instruments");
                }
                else
                {
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Can't open instruments service.")));
                }

                if (!isSessionRunning)
                {
                    isSessionRunning = true;
                    mBbgMsgWorker = new System.Threading.Thread(ProcessBbgEvent);
                    mBbgMsgWorker.Start();
                }
            }
        }

        public void SendRequestForHistoricalData(List<ApiTaskHistoricalData> argvTaskCollection)
        {
            foreach (ApiTaskHistoricalData tTask in argvTaskCollection)
            {
                this.SendRequestForHistoricalData(tTask);
            }
        }

        public void SendRequestForHistoricalData(ApiTaskHistoricalData argvTask)
        {
            //Generate correlation ID
            long tUniqueId = this.GenerateCorrelationId();
            CorrelationID tCorrelationId = new CorrelationID(tUniqueId);
            mHistoricalDataRequestByCorrelationId.Add(tUniqueId, argvTask);
            // Create the Request object to represent the data request
            Request tRequest = mReferenceService.CreateRequest("HistoricalDataRequest");
            // Specify the security we are interested in
            string[] tBbgTickers = argvTask.GetBbgTickerList();
            foreach (string tBbgTicker in tBbgTickers)
            {
                tRequest.Append("securities", tBbgTicker);
            }
            // Specify the fields we are interested in 
            string[] tFields = argvTask.GetFieldList();
            foreach (string tField in tFields)
            {
                tRequest.Append("fields", tField);
            }
            // Set the start and ending dates and the max number of data points
            tRequest.Set("startDate", argvTask.mStartTime.ToString("yyyyMMdd"));
            tRequest.Set("endDate", argvTask.mEndTime.ToString("yyyyMMdd"));
            tRequest.Set("periodicitySelection", "DAILY");
            tRequest.Set("periodicityAdjustment", "ACTUAL");
            tRequest.Set("nonTradingDayFillOption", "ACTIVE_DAYS_ONLY");
            tRequest.Set("nonTradingDayFillMethod", "PREVIOUS_VALUE");
            tRequest.Set("pricingOption", "PRICING_OPTION_PRICE");
            tRequest.Set("overrideOption", "OVERRIDE_OPTION_CLOSE");
            tRequest.Set("maxDataPoints", Convert.ToInt32((argvTask.mEndTime - argvTask.mStartTime).TotalDays) + 1);
            tRequest.Set("adjustmentNormal", false);
            tRequest.Set("adjustmentAbnormal", false);
            tRequest.Set("adjustmentSplit", false);
            tRequest.Set("adjustmentFollowDPDF", true);
            // Submit the request
            mSession.SendRequest(tRequest, tCorrelationId);
            //mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "HistoricalDataRequest: " + argvTask.mTicker + " for fields " + argvTask.mFields
            //            + " from " + argvTask.mStartTime.ToString("yyyyMMdd") + " to " + argvTask.mEndTime.ToString("yyyyMMdd")));
        }

        public void SendRequestForReferenceData(List<ApiTask> argvTasks)
        {
            //mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Sending request for reference data..."));
            foreach (ApiTask tTask in argvTasks)
            {
                //Generate correlation ID
                long tUniqueId = this.GenerateCorrelationId();
                CorrelationID tCorrelationId = new CorrelationID(tUniqueId);
                mOtherRequestByCorrelationId.Add(tUniqueId, tTask);

                // Create the Request object to represent the data request
                Request tRequest = mReferenceService.CreateRequest("ReferenceDataRequest");
                // Specify the security we are interested in
                tRequest.Append("securities", tTask.mTicker);
                // Specify the fields we are interested in 
                string[] tFields = tTask.GetFieldList();
                foreach (string tField in tFields)
                {
                    tRequest.Append("fields", tField);
                }

                tRequest.Set("returnEids", false);
                tRequest.Set("returnFormattedValue", false);
                tRequest.Set("useUTCTime", false);
                tRequest.Set("forcedDelay", false);
                tRequest.Set("returnNullValue", false);
                // Submit the request
                mSession.SendRequest(tRequest, tCorrelationId);
                //mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "ReferenceDataRequest: " + tTask.mTicker + " for fields " + tTask.mFields));
            }
        }

        public void SendRequestForIntradayTick(ApiTaskHistoricalData argvTask)
        {
            //Generate correlation ID
            long tUniqueId = this.GenerateCorrelationId();
            CorrelationID tCorrelationId = new CorrelationID(tUniqueId);
            mIntradayTickRequestByCorrelationId.Add(tUniqueId, argvTask);
            //mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Sending request for intraday tick..."));
            // Create the Request object to represent the data request
            Request tRequest = mReferenceService.CreateRequest("IntradayTickRequest");
            // Specify the security we are interested in
            tRequest.Set("security", argvTask.mBbgTicker);
            // Add fields to request
            Element eventTypes = tRequest.GetElement("eventTypes");
            string[] tFields = argvTask.GetFieldList();
            foreach (string tField in tFields)
            {
                eventTypes.AppendValue(tField);
            }
            tRequest.Set("includeConditionCodes", false);
            tRequest.Set("includeNonPlottableEvents", true);
            tRequest.Set("includeExchangeCodes", false);
            tRequest.Set("returnEids", true);
            tRequest.Set("includeBrokerCodes", false);
            tRequest.Set("includeRpsCodes", false);
            tRequest.Set("includeBicMicCodes", false);
            tRequest.Set("includeSpreadPrice", false);
            tRequest.Set("includeYield", false);
            tRequest.Set("includeActionCodes", false);
            tRequest.Set("includeIndicatorCodes", false);
            tRequest.Set("includeTradeTime", false);
            tRequest.Set("includeUpfrontPrice", false);
            tRequest.Set("includeTradeId", false);
            // Set the start and ending dates and the max number of data points
            tRequest.Set("startDateTime", argvTask.mStartTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"));
            tRequest.Set("endDateTime", argvTask.mEndTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"));
            tRequest.Set("maxDataPoints", 100000);
            mLastIntradayTickTicker = argvTask.mBbgTicker;
            // Submit the request
            mSession.SendRequest(tRequest, tCorrelationId);
            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "IntradayTickRequest: " + argvTask.mTicker + " for fields " + argvTask.mFields
                        + " from " + argvTask.mStartTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss") + " to " + argvTask.mEndTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")));
        }

        public void SendRequestForInstradayBar(ApiTaskBar argvTask)
        {
            //Generate correlation ID
            long tUniqueId = this.GenerateCorrelationId();
            CorrelationID tCorrelationId = new CorrelationID(tUniqueId);
            mIntradayBarRequestByCorrelationId.Add(tUniqueId, argvTask);
            //mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Sending Intraday Bar request..."));
            // Create the Request object to represent the data request
            Request tRequest = mReferenceService.CreateRequest("IntradayBarRequest");
            // Specify the security we are interested in
            tRequest.Set("security", argvTask.mBbgTicker);
            // Add fields to request
            tRequest.Set("eventType", argvTask.mFields);
            tRequest.Set("gapFillInitialBar", false);
            tRequest.Set("adjustmentNormal", false);
            tRequest.Set("adjustmentAbnormal", false);
            tRequest.Set("adjustmentSplit", false);
            tRequest.Set("adjustmentFollowDPDF", false);
            tRequest.Set("interval", argvTask.mBarLengthInMinute);
            // Set the start and ending dates and the max number of data points
            tRequest.Set("startDateTime", argvTask.mStartTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"));
            tRequest.Set("endDateTime", argvTask.mEndTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss"));
            int tMaxDataPoints = (int)Math.Round((argvTask.mEndTime - argvTask.mStartTime).TotalMinutes / argvTask.mBarLengthInMinute + 10);
            tRequest.Set("maxDataPoints", tMaxDataPoints);
            mLastIntradayTickTicker = argvTask.mBbgTicker;
            // Submit the request
            mSession.SendRequest(tRequest, tCorrelationId);
            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "IntradayBarRequest: " + argvTask.mTicker + " for fields " + argvTask.mFields
                        + " from " + argvTask.mStartTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss") + " to " + argvTask.mEndTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")));
        }

        public void SubscribeMktData(List<ApiTaskRealtime> argvTasks)
        {
            List<Subscription> tNewTask = new List<Subscription>();
            foreach (ApiTaskRealtime tTask in argvTasks)
            {
                if (!mSubscriptions.ContainsKey(tTask.mBbgTicker))
                {
                    //Generate correlation ID
                    long tUniqueId = this.GenerateCorrelationId();
                    CorrelationID tCorrelationId = new CorrelationID(tUniqueId);
                    mSubscriptionByCorrelationId.Add(tUniqueId, tTask);

                    List<string> tFields = tTask.GetFieldList().ToList();
                    List<string> tOptions = new List<string>();
                    tOptions.Add("interval=" + tTask.mDataDeliverInterval.ToString());
                    Bloomberglp.Blpapi.Subscription tSubscription = new Subscription(tTask.mBbgTicker, tFields, tOptions, tCorrelationId);
                    mSubscriptions.Add(tTask.mBbgTicker, tSubscription);
                    tNewTask.Add(tSubscription);
                    mMktData.Add(tTask.mBbgTicker, new MarketData(tTask.mTicker, tTask.mBbgTicker));
                    mTradingDates.Add(tTask.mBbgTicker, DateTime.Today.ToString("yyyy-MM-dd"));
                }
            }

            mSession.Subscribe(tNewTask);
        }

        public void UnsubscribeMktData(string argvBbgTicker)
        {
            if (mSubscriptions.ContainsKey(argvBbgTicker))
            {
                mSession.Cancel(mSubscriptions[argvBbgTicker].CorrelationID);
            }
            else
            {
                mOutput.Print("Can't find subscription for " + argvBbgTicker);
            }
        }

        public void UnsubscribeAllMktData()
        {
            if (mSession != null)
            {
                foreach (long tCorrelationId in mSubscriptionByCorrelationId.Keys)
                {
                    mSession.Cancel(new Bloomberglp.Blpapi.CorrelationID(tCorrelationId));
                }
            }
        }

        private void ProcessBbgEvent()
        {
            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Listening to event...")));
            // Now we can process the incoming Events
            while (isSessionRunning)
            {
                // Grab the next Event object
                Event eventObject = mSession.NextEvent(100);
                if (eventObject.Count() > 0)
                {
                    mMsgCounter++;
                    if (eventObject.Type == Event.EventType.PARTIAL_RESPONSE || eventObject.Type == Event.EventType.RESPONSE)
                    {
                        ProcessRequestResponse(eventObject);
                    }
                    else if (eventObject.Type == Event.EventType.SESSION_STATUS)
                    {
                        ProcessSessionStatusChange(eventObject);
                    }
                    else if (eventObject.Type == Event.EventType.SERVICE_STATUS)
                    {
                        ProcessSessionStatusChange(eventObject);
                    }
                    else if (eventObject.Type == Event.EventType.SUBSCRIPTION_DATA)
                    {
                        ProcessSubscriptionData(eventObject);
                    } 
                    else if (eventObject.Type == Event.EventType.SUBSCRIPTION_STATUS)
                    {
                        ProcessSubscriptionStatus(eventObject);
                    } 
                    else
                    {
                        mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "New event " + eventObject.Type.ToString()));
                        // Loop over all of the messages in this Event
                        foreach (Bloomberglp.Blpapi.Message msg in eventObject)
                        {
                            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Error, msg.ToString()));
                        }
                    } 
                }
            } // end while not done
        }

        private void ProcessSessionStatusChange(Bloomberglp.Blpapi.Event argvEvent)
        {
            foreach (Bloomberglp.Blpapi.Message msg in argvEvent)
            {
                mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, msg.ToString()));

                if (msg.MessageType == Bloomberglp.Blpapi.Name.FindName("SessionTerminated") || msg.MessageType == Bloomberglp.Blpapi.Name.FindName("SessionConnectionDown"))
                {
                    isSessionRunning = false;
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.LoginFail, msg.ToString()));
                    this.ShutDown();
                }

                if (msg.MessageType == Bloomberglp.Blpapi.Name.FindName("SessionStarted"))
                {
                    isSessionRunning = true;
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.LoginSuccess, "LoginSuccess"));
                }
            }
        }

        private void ProcessRequestResponse(Bloomberglp.Blpapi.Event argvEvent)
        {
            // Loop over all of the messages in this Event
            foreach (Bloomberglp.Blpapi.Message tMsg in argvEvent)
            {
                Bloomberglp.Blpapi.Element tElementMsg = tMsg.AsElement;
                System.Data.DataTable tResponseErrorTable = this.ExtractValueByName(tElementMsg, mRequestResponseErrorAttributes);

                if (tResponseErrorTable.Rows.Count > 0)
                {
                    string tErrorMsg = "Message Type: " + tMsg.MessageType.ToString() + Environment.NewLine;
                    for (int i = 0; i < tResponseErrorTable.Rows.Count; i++)
                    {
                        tErrorMsg += "Error Message: " + tResponseErrorTable.Rows[i]["MESSAGE"] + ", Category: " + tResponseErrorTable.Rows[i]["SUBCATEGORY"] + Environment.NewLine;
                    }
                    InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Error, tErrorMsg);
                    mBbgMsgEvent(this, tEventArgvs);
                }

                if (tMsg.MessageType.Equals(new Bloomberglp.Blpapi.Name("HistoricalDataResponse")))
                {
                    this.ProcessHistoricalDataResponse(tMsg);
                }
                else if (tMsg.MessageType.Equals(new Bloomberglp.Blpapi.Name("IntradayTickResponse")))
                {
                    this.ProcessIntradayTickResponse(tMsg);
                }
                else if (tMsg.MessageType.Equals(new Bloomberglp.Blpapi.Name("IntradayBarResponse")))
                {
                    this.ProcessIntradayBarResponse(tMsg);
                }
                else if (tMsg.MessageType.Equals(new Bloomberglp.Blpapi.Name("ReferenceDataResponse")))
                {
                    this.ProcessReferenceDataResponse(tMsg);
                }
                else
                {
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, tMsg.ToString()));
                }
            }
        }

        private void ProcessHistoricalDataResponse(Bloomberglp.Blpapi.Message argvMessage)
        {
            Bloomberglp.Blpapi.Element tElementMsg = argvMessage.AsElement;
            string[] tTargetAttributes = mHistoricalDataResponseDefaultAttributes;
            if (mHistoricalDataRequestByCorrelationId.ContainsKey(argvMessage.CorrelationID.Value))
            {
                tTargetAttributes = mHistoricalDataRequestByCorrelationId[argvMessage.CorrelationID.Value].GetFieldList();
            }
            else
            {
                InterfaceEventArgs tArgs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Error);
                tArgs.mMsg = "Unknown historical data response. Correlation ID: " + argvMessage.CorrelationID.Value + ". Will use default attributes to parse the message.";
                mBbgMsgEvent(this, tArgs);
            }

            string[] tFullAttributes = new string[tTargetAttributes.Length + 2];
            tFullAttributes[0] = "security";
            tFullAttributes[1] = "date";
            Array.Copy(tTargetAttributes, 0, tFullAttributes, 2, tTargetAttributes.Length);

            System.Data.DataTable tExtractedValues = this.ExtractValueByName(tElementMsg, tFullAttributes);
            mOutput.PrintDataTable(tExtractedValues);
            InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.HistoricalDataResponse);
            tEventArgvs.mData = tExtractedValues;
            mBbgMsgEvent(this, tEventArgvs);
        }

        private void ProcessIntradayTickResponse(Bloomberglp.Blpapi.Message argvMessage)
        {
            if (mIntradayTickRequestByCorrelationId.ContainsKey(argvMessage.CorrelationID.Value))
            {
                string tTicker = mIntradayTickRequestByCorrelationId[argvMessage.CorrelationID.Value].mTicker;
                Bloomberglp.Blpapi.Element tElementMsg = argvMessage.AsElement;
                System.Data.DataTable tExtractedValues = this.ExtractValueByName(tElementMsg, mIntradayTickResponseDefaultAttributes);
                System.Data.DataColumn tNewColumn = new System.Data.DataColumn("SECURITY");
                tNewColumn.DefaultValue = tTicker;
                tExtractedValues.Columns.Add(tNewColumn);
                tNewColumn.SetOrdinal(0);
                mOutput.PrintDataTable(tExtractedValues);

                InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.IntradayTickResponse);
                tEventArgvs.mData = tExtractedValues;
                mBbgMsgEvent(this, tEventArgvs);
            }
            else
            {
                InterfaceEventArgs tArgs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Error);
                tArgs.mMsg = "Unknown intraday tick response. Correlation ID: " + argvMessage.CorrelationID.Value + ". Won't parse the message.";
                mBbgMsgEvent(this, tArgs);
            }
        }

        private void ProcessIntradayBarResponse(Bloomberglp.Blpapi.Message argvMessage)
        {
            if (mIntradayBarRequestByCorrelationId.ContainsKey(argvMessage.CorrelationID.Value))
            {
                string tTicker = mIntradayBarRequestByCorrelationId[argvMessage.CorrelationID.Value].mTicker;
                Bloomberglp.Blpapi.Element tElementMsg = argvMessage.AsElement;
                System.Data.DataTable tExtractedValues = this.ExtractValueByName(tElementMsg, mIntradayBarResponseDefaultAttributes);
                System.Data.DataColumn tNewColumn = new System.Data.DataColumn("SECURITY");
                tNewColumn.DefaultValue = tTicker;
                tExtractedValues.Columns.Add(tNewColumn);
                tNewColumn.SetOrdinal(0);
                mOutput.PrintDataTable(tExtractedValues);

                InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.IntradayBarResponse);
                tEventArgvs.mData = tExtractedValues;
                mBbgMsgEvent(this, tEventArgvs);
            }
            else
            {
                InterfaceEventArgs tArgs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Error);
                tArgs.mMsg = "Unknown intraday bar response. Correlation ID: " + argvMessage.CorrelationID.Value + ". Won't parse the message.";
                mBbgMsgEvent(this, tArgs);
            }
        }

        private void ProcessSubscriptionStatus(Bloomberglp.Blpapi.Event argEvent)
        {
            foreach (Bloomberglp.Blpapi.Message tMsg in argEvent)
            {
                foreach (CorrelationID tId in tMsg.CorrelationIDs)
                {
                    if (tMsg.MessageType.ToString() == "SubscriptionFailure")
                    {
                        string tTicker = string.Empty;
                        if (mSubscriptionByCorrelationId.ContainsKey(tId.Value))
                        {
                            tTicker = mSubscriptionByCorrelationId[tId.Value].mTicker;
                        }

                        InterfaceEventArgs tArgs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Error);
                        tArgs.mMsg = "SubscriptionFailure: Ticker '" + tTicker + "'";
                        mBbgMsgEvent(this, tArgs);
                    }
                }
            }
        }

        private void ProcessSubscriptionData(Bloomberglp.Blpapi.Event argEvent)
        {
            foreach (Bloomberglp.Blpapi.Message tMsg in argEvent)
            {
                Bloomberglp.Blpapi.Element tElementMsg = tMsg.AsElement;
                System.Data.DataTable tExtractedValues = this.ExtractValueByName(tElementMsg, mRealTimeDataDefaultAttributes);
                System.Data.DataColumn tNewColumn = new System.Data.DataColumn("SECURITY");
                tNewColumn.DefaultValue = tMsg.TopicName;
                tExtractedValues.Columns.Add(tNewColumn);
                tNewColumn.SetOrdinal(0);
                mOutput.PrintDataTable(tExtractedValues);

                if (tExtractedValues.Rows.Count > 0)
                {
                    if (mMktData.ContainsKey(tMsg.TopicName))
                    {
                        DateTime tTradingDate = DateTime.MinValue;
                        double tBid = 0;
                        DateTime tBidUpdateTime = DateTime.MinValue;
                        double tAsk = 0;
                        DateTime tAskUpdateTime = DateTime.MinValue;
                        double tLastTradePrice = 0;
                        DateTime tLastTradeTime = DateTime.MinValue;

                        if (tExtractedValues.Rows[0]["Bid"].ToString().Length != 0)
                        {
                            tBid = double.Parse(tExtractedValues.Rows[0]["Bid"].ToString());
                        }

                        if (tExtractedValues.Rows[0]["LAST_UPDATE_BID_RT"].ToString().Length != 0)
                        {
                            tBidUpdateTime = DateTime.ParseExact(tExtractedValues.Rows[0]["LAST_UPDATE_BID_RT"].ToString(), "yyyyMMdd HHmmss.fff", System.Globalization.CultureInfo.InvariantCulture);
                        }

                        if (tExtractedValues.Rows[0]["Ask"].ToString().Length != 0)
                        {
                            tAsk = double.Parse(tExtractedValues.Rows[0]["Ask"].ToString());
                        }

                        if (tExtractedValues.Rows[0]["LAST_UPDATE_ASK_RT"].ToString().Length != 0)
                        {
                            tAskUpdateTime = DateTime.ParseExact(tExtractedValues.Rows[0]["LAST_UPDATE_ASK_RT"].ToString(), "yyyyMMdd HHmmss.fff", System.Globalization.CultureInfo.InvariantCulture);
                        }

                        if (tExtractedValues.Rows[0]["LAST_TRADE"].ToString().Length != 0)
                        {
                            tLastTradePrice = double.Parse(tExtractedValues.Rows[0]["LAST_TRADE"].ToString());
                        }

                        if (tExtractedValues.Rows[0]["TRADE_UPDATE_STAMP_RT"].ToString().Length != 0)
                        {
                            tLastTradeTime = DateTime.ParseExact(tExtractedValues.Rows[0]["TRADE_UPDATE_STAMP_RT"].ToString(), "yyyyMMdd HHmmss.fff", System.Globalization.CultureInfo.InvariantCulture);
                        }

                        mMktData[tMsg.TopicName].Update(DateTime.MinValue, tBid, tBidUpdateTime, tAsk, tAskUpdateTime, tLastTradePrice, tLastTradeTime);
                    }

                    InterfaceEventArgs tArgs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.SubscriptionResponse);
                    tArgs.mData = tExtractedValues;
                    mBbgMsgEvent(this, tArgs);
                }
            }
        }

        private void ProcessReferenceDataResponse(Bloomberglp.Blpapi.Message argvMessage)
        {
            Bloomberglp.Blpapi.Element tElementMsg = argvMessage.AsElement;
            InterfaceEventArgs tRetData = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.ReferenceDataResponse);
            tRetData.mBbgMsg = tElementMsg;
            mBbgMsgEvent(this, tRetData);
        }

        public System.Data.DataTable ExtractValueByName(Bloomberglp.Blpapi.Element argvElement, string[] argvNames)
        {
            System.Data.DataTable tReturnValue = new System.Data.DataTable();
            string[] tUpperCaseNames = new string[argvNames.Length];
            for (int i = 0; i < argvNames.Length; i++)
            {
                tUpperCaseNames[i] = argvNames[i].ToUpper();
                tReturnValue.Columns.Add(tUpperCaseNames[i]);
            }

            if (argvElement.Datatype == Schema.Datatype.CHOICE || argvElement.Datatype == Schema.Datatype.SEQUENCE)
            {
                List<Bloomberglp.Blpapi.Element> tSubElementArray = new List<Element>();
                if (argvElement.IsArray)
                {
                    for (int i = 0; i < argvElement.NumValues; i++)
                    {
                        tSubElementArray.Add((Bloomberglp.Blpapi.Element)argvElement.GetValue(i));
                    }
                }
                else
                {
                    foreach (Bloomberglp.Blpapi.Element tSubElement in argvElement.Elements)
                    {
                        tSubElementArray.Add(tSubElement);
                    }
                }

                bool tRowIsAdded = false;
                foreach (Bloomberglp.Blpapi.Element tSubElement in tSubElementArray)
                {
                    //Console.WriteLine(temp_sub_element.Name.ToString() + " type is " + temp_sub_element.Datatype.ToString());
                    if (tSubElement.Datatype == Schema.Datatype.CHOICE || tSubElement.Datatype == Schema.Datatype.SEQUENCE)
                    {
                        System.Data.DataTable tLowerLevelValueTable = this.ExtractValueByName(tSubElement, tUpperCaseNames);
                        foreach (System.Data.DataRow tLowerLevelValueRow in tLowerLevelValueTable.Rows)
                        {
                            System.Data.DataRow tNewRow = tReturnValue.NewRow();
                            foreach (System.Data.DataColumn tColumn in tLowerLevelValueTable.Columns)
                            {
                                tNewRow[tColumn.ColumnName] = tLowerLevelValueRow[tColumn.ColumnName];
                            }
                            tReturnValue.Rows.Add(tNewRow);
                        }
                    }
                    else if (tSubElement.NumValues > 0)
                    {
                        //Console.WriteLine(temp_sub_element.Name.ToString() + " value is " + temp_sub_element.GetValueAsString(0));
                        if (tUpperCaseNames.Contains(tSubElement.Name.ToString().ToUpper()))
                        {
                            if (!tRowIsAdded)
                            {
                                System.Data.DataRow tNewRow = tReturnValue.NewRow();
                                tReturnValue.Rows.Add(tNewRow);
                                tRowIsAdded = true;
                            }
                            System.Type tValueType = tSubElement.GetValue().GetType();
                            if (tValueType == typeof(Datetime))
                            {
                                tReturnValue.Rows[tReturnValue.Rows.Count - 1][tSubElement.Name.ToString().ToUpper()] = tSubElement.GetValueAsDatetime().ToSystemDateTime().ToString("yyyyMMdd HHmmss.fff");
                            }
                            else
                            {
                                tReturnValue.Rows[tReturnValue.Rows.Count - 1][tSubElement.Name.ToString().ToUpper()] = tSubElement.GetValueAsString(0);
                            }
                        }
                    }
                }
            }

            return tReturnValue;
        }

        private List<Bloomberglp.Blpapi.Element> ConvertElementArrayToList(Bloomberglp.Blpapi.Element argvElement)
        {
            List<Bloomberglp.Blpapi.Element> tReturnValue = new List<Element>();

            if (argvElement.IsArray)
            {
                for (int i = 0; i < argvElement.NumValues; i++)
                {
                    tReturnValue.Add(argvElement.GetValueAsElement(i));
                }
            }
            else
            {
                tReturnValue.Add(argvElement);
            }

            return tReturnValue;
        }

        public double GetMessageCount()
        {
            return mMsgCounter;
        }

        public void ShutDown()
        {
            isSessionRunning = false;

            if (mSession != null)
            {
                this.UnsubscribeAllMktData();

                if (mBbgMsgWorker != null)
                {
                    mBbgMsgWorker.Join();
                }

                mSession.Stop();
            }
        }
    }
}
