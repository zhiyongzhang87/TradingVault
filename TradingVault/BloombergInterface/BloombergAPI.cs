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

        private OutputHelper mOutput;
        private Session mSession;
        private Service mReferenceService;
        private Service mMktService;
        private Service mLookupService;
        private bool isSessionRunning;
        public event BbgEventHandler mBbgMsgEvent;
        private System.Threading.Thread mBbgMsgWorker;
        private Dictionary<string, ApiTask> mTasks;
        private Dictionary<string, Subscription> mSubscriptions;
        private Dictionary<string, BloombergData> mMktData;
        private Dictionary<string, string> mTradingDates;
        private List<string> mCorrelationID;
        private double mMsgCounter;
        private string mLastIntradayTickTicker;
        private List<string> mRequestResponseErrorAttributes;
        private List<string> mIntradayBarResponseDefaultAttributes;
        private List<string> mIntradayBarResponseAttributes;
        private List<string> mIntradayTickResponseDefaultAttributes;
        private List<string> mIntradayTickResponseAttributes;
        private List<string> mHistoricalDataResponseDefaultAttributes;
        private Dictionary<string, string[]> mHistoricalDataResponseAttributes;

        public BloombergApi()
        {
            mOutput = new OutputHelper("BloombergApi" + DateTime.Now.ToString("yyyyMMddHHmmss"));
            isSessionRunning = false;
            mTasks = new Dictionary<string, ApiTask>();
            mSubscriptions = new Dictionary<string, Subscription>();
            mMktData = new Dictionary<string, BloombergData>();
            mTradingDates = new Dictionary<string, string>();
            mCorrelationID = new List<string>();
            mMsgCounter = 0;

            mRequestResponseErrorAttributes = new List<string>();
            mRequestResponseErrorAttributes.Add("message");
            mRequestResponseErrorAttributes.Add("subcategory");

            mIntradayBarResponseAttributes = new List<string>();
            mIntradayBarResponseDefaultAttributes = new List<string>();
            mIntradayBarResponseDefaultAttributes.Add("time");
            mIntradayBarResponseDefaultAttributes.Add("open");
            mIntradayBarResponseDefaultAttributes.Add("high");
            mIntradayBarResponseDefaultAttributes.Add("low");
            mIntradayBarResponseDefaultAttributes.Add("close");

            mIntradayTickResponseAttributes = new List<string>();
            mIntradayTickResponseDefaultAttributes = new List<string>();
            mIntradayTickResponseDefaultAttributes.Add("time");
            mIntradayTickResponseDefaultAttributes.Add("value");
            mIntradayTickResponseDefaultAttributes.Add("size");
            mIntradayTickResponseDefaultAttributes.Add("type");
            mIntradayTickResponseDefaultAttributes.Add("conditionCodes");
            mIntradayTickResponseDefaultAttributes.Add("tradeId");

            mHistoricalDataResponseAttributes = new Dictionary<string, string[]>();
            mHistoricalDataResponseDefaultAttributes = new List<string>();
            mHistoricalDataResponseDefaultAttributes.Add("security");
            mHistoricalDataResponseDefaultAttributes.Add("date");
            mHistoricalDataResponseDefaultAttributes.Add("OPEN");
            mHistoricalDataResponseDefaultAttributes.Add("HIGH");
            mHistoricalDataResponseDefaultAttributes.Add("LOW");
            mHistoricalDataResponseDefaultAttributes.Add("LAST_PRICE");
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
                mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Can't create new session.")));
                mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, (e.Message)));
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
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Can't open mktdata.")));
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
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Can't open refdata.")));
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
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, ("Can't open instruments.")));
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
            Guid tIdGenerator = Guid.NewGuid();
            string tUniqueId = argvTask.mTicker + "@" + Convert.ToBase64String(tIdGenerator.ToByteArray());
            CorrelationID tCorrelationId = new CorrelationID(tUniqueId);
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
            mHistoricalDataResponseAttributes.Add(tUniqueId, tFields);
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
            mSession.SendRequest(tRequest, null);
            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "HistoricalDataRequest: " + argvTask.mTicker + " for fields " + argvTask.mFields
                        + " from " + argvTask.mStartTime.ToString("yyyyMMdd") + " to " + argvTask.mEndTime.ToString("yyyyMMdd")));
        }

        public void SendRequestForReferenceData(Dictionary<string, ApiTask> argvTasks)
        {
            //mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Sending request for reference data..."));
            foreach (string tTicker in argvTasks.Keys)
            {
                ApiTask tTask = argvTasks[tTicker];
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
                mSession.SendRequest(tRequest, null);
                mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "ReferenceDataRequest: " + tTask.mTicker + " for fields " + tTask.mFields));
            }
        }

        public void SendRequestForIntradayTick(ApiTaskHistoricalData argvTask)
        {
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
            mSession.SendRequest(tRequest, null);
            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "IntradayTickRequest: " + argvTask.mTicker + " for fields " + argvTask.mFields
                        + " from " + argvTask.mStartTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss") + " to " + argvTask.mEndTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")));
        }

        public void SendRequestForInstradayBar(ApiTaskBar argvTask)
        {
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
            mSession.SendRequest(tRequest, null);
            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "IntradayBarRequest: " + argvTask.mTicker + " for fields " + argvTask.mFields
                        + " from " + argvTask.mStartTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss") + " to " + argvTask.mEndTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")));
        }

        public void SubscribeMktData(Dictionary<string, ApiTaskRealtime> argvTasks)
        {
            Guid tIdGenerator = Guid.NewGuid();
            List<Subscription> tNewTask = new List<Subscription>();
            foreach (string tTicker in argvTasks.Keys)
            {
                if (!mSubscriptions.ContainsKey(tTicker))
                {
                    string tUniqueId = Convert.ToBase64String(tIdGenerator.ToByteArray());
                    CorrelationID tCorrelationId = new CorrelationID(tTicker + "@" + tUniqueId);
                    mCorrelationID.Add(tCorrelationId.Object.ToString());
                    List<string> tFields = argvTasks[tTicker].GetFieldList().ToList();
                    List<string> tOptions = new List<string>();
                    tOptions.Add("interval=" + argvTasks[tTicker].mDataDeliverInterval.ToString());
                    Bloomberglp.Blpapi.Subscription tSubscription = new Subscription(tTicker, tFields, tOptions, tCorrelationId);
                    mSubscriptions.Add(tTicker, tSubscription);
                    tNewTask.Add(tSubscription);
                    mMktData.Add(tTicker, new BloombergData(tTicker));
                    mTradingDates.Add(tTicker, DateTime.Today.ToString("yyyy-MM-dd"));
                }
            }

            mSession.Subscribe(tNewTask);
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
            string tStrCorrelationId = argvMessage.CorrelationID.ToString();
            Bloomberglp.Blpapi.Element tElementMsg = argvMessage.AsElement;
            List<string> tTargetAttributes = mHistoricalDataResponseDefaultAttributes;
            if (mHistoricalDataResponseAttributes.ContainsKey(tStrCorrelationId))
            {
                mOutput.Print("Found target attributes");
                tTargetAttributes = mHistoricalDataResponseAttributes[tStrCorrelationId].ToList();
            }
            else
            {
                mOutput.Print("Can't find target attributes for " + tStrCorrelationId);
            }
            System.Data.DataTable tExtractedValues = this.ExtractValueByName(tElementMsg, tTargetAttributes);
            mOutput.PrintDataTable(tExtractedValues);
        }

        private void ProcessIntradayTickResponse(Bloomberglp.Blpapi.Message argvMessage)
        {
            Bloomberglp.Blpapi.Element tElementMsg = argvMessage.AsElement;
            List<string> tTargetAttributes = mIntradayTickResponseDefaultAttributes;
            if (mIntradayTickResponseAttributes.Count > 0)
            {
                tTargetAttributes = mIntradayTickResponseAttributes;
            }
            System.Data.DataTable tExtractedValues = this.ExtractValueByName(tElementMsg, tTargetAttributes);
            //mOutput.PrintDataTable(tExtractedValues);
            if (tExtractedValues.Rows.Count > 0)
            {
                InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.IntradayTickResponse);
                tEventArgvs.mData = tExtractedValues;
                mBbgMsgEvent(this, tEventArgvs);
            }
            else
            {
                InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Error);
                tEventArgvs.mMsg = "IntradayTickResponse doesn't have any data.";
                mBbgMsgEvent(this, tEventArgvs);
            }
        }

        private void ProcessIntradayBarResponse(Bloomberglp.Blpapi.Message argvMessage)
        {
            Bloomberglp.Blpapi.Element tElementMsg = argvMessage.AsElement;
            List<string> tTargetAttributes = mIntradayBarResponseDefaultAttributes;
            if(mIntradayBarResponseAttributes.Count > 0)
            {
                tTargetAttributes = mIntradayBarResponseAttributes;
            }
            System.Data.DataTable tExtractedValues = this.ExtractValueByName(tElementMsg, tTargetAttributes);
            mOutput.PrintDataTable(tExtractedValues);

            if(tExtractedValues.Rows.Count > 0)
            {
                InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.IntradayBarResponse);
                tEventArgvs.mData = tExtractedValues;
                mBbgMsgEvent(this, tEventArgvs);
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
                        InterfaceEventArgs tArgs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Error, "Correlation ID: " + tMsg.CorrelationID + ", Msg Type: " + tMsg.MessageType);
                        mBbgMsgEvent(this, tArgs);
                    }
                }
            }
        }

        private void ProcessSubscriptionData(Bloomberglp.Blpapi.Event argEvent)
        {
            foreach (Bloomberglp.Blpapi.Message tMsg in argEvent)
            {
                string tTicker = tMsg.TopicName;

                if (tMsg.HasElement("TRADING_DT_REALTIME"))
                {
                    mTradingDates[tTicker] = tMsg.GetElementAsString("TRADING_DT_REALTIME");
                }
                string tTradingDate = mTradingDates[tTicker];
                if (tMsg.HasElement("BID"))
                {
                    if (!tMsg.GetElement("BID").IsNull)
                    {
                        string tValue = tMsg.GetElementAsString("BID");
                        if (tValue.Length == 0)
                        {
                            tValue = "0";
                        }
                        mMktData[tTicker].Update("BID", tTradingDate, tValue);
                    }
                }

                if (tMsg.HasElement("ASK"))
                {
                    if (!tMsg.GetElement("ASK").IsNull)
                    {
                        string tValue = tMsg.GetElementAsString("ASK");
                        if (tValue.Length == 0)
                        {
                            tValue = "0";
                        }
                        mMktData[tTicker].Update("ASK", tTradingDate, tValue);
                    }
                }

                string tBid = mMktData[tTicker].GetValue("BID", tTradingDate);
                string tAsk = mMktData[tTicker].GetValue("ASK", tTradingDate);

                InterfaceEventArgs tArgs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.SubscriptionResponse);
                tArgs.mMsg = tMsg.TopicName + ", " + tMsg.CorrelationID.Object.ToString() + ": Trading Date " + tTradingDate + ", BID " + tBid + ", ASK " + tAsk;
                tArgs.mData.Columns.Add(new System.Data.DataColumn("TICKER"));
                tArgs.mData.Columns.Add(new System.Data.DataColumn("BID"));
                tArgs.mData.Columns.Add(new System.Data.DataColumn("ASK"));
                System.Data.DataRow tNewRow = tArgs.mData.NewRow();
                tNewRow["TICKER"] = tTicker;
                tNewRow["BID"] = tBid;
                tNewRow["ASK"] = tAsk;
                tArgs.mData.Rows.Add(tNewRow);
                mBbgMsgEvent(this, tArgs);
            }
        }

        private void ProcessReferenceDataResponse(Bloomberglp.Blpapi.Message argvMessage)
        {
            Bloomberglp.Blpapi.Element tElementMsg = argvMessage.AsElement;
            InterfaceEventArgs tRetData = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.ReferenceDataResponse);
            tRetData.mBbgMsg = tElementMsg;
            mBbgMsgEvent(this, tRetData);
        }

        public System.Data.DataTable ExtractValueByName(Bloomberglp.Blpapi.Element argvElement, List<string> argvNames)
        {
            System.Data.DataTable tReturnValue = new System.Data.DataTable();
            List<string> tUpperCaseNames = new List<string>();
            foreach (string tName in argvNames)
            {
                tUpperCaseNames.Add(tName.ToUpper());
                tReturnValue.Columns.Add(tName.ToUpper());
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
                            tReturnValue.Rows[tReturnValue.Rows.Count - 1][tSubElement.Name.ToString().ToUpper()] = tSubElement.GetValueAsString(0);
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
                List<Subscription> tSubscription = new List<Subscription>();
                foreach (string tTicker in mSubscriptions.Keys)
                {
                    if (mSubscriptions[tTicker].SubscriptionStatus != Session.SubscriptionStatus.UNSUBSCRIBED)
                    {
                        tSubscription.Add(mSubscriptions[tTicker]);
                    }
                }

                if (tSubscription.Count > 0)
                {
                    mSession.Unsubscribe(tSubscription);
                }

                if (mBbgMsgWorker != null)
                {
                    mBbgMsgWorker.Join();
                }

                mSession.Stop();
            }
        }
    }
}
