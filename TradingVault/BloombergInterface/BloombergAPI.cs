using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Bloomberglp.Blpapi;

namespace BloombergInterface
{
    public class BloombergApi
    {
        public enum xProductTenor { Unknown, Cash, Future, Option }

        public delegate void BbgEventHandler(object sender, InterfaceEventArgs e);

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

        public BloombergApi()
        {
            isSessionRunning = false;
            mTasks = new Dictionary<string, ApiTask>();
            mSubscriptions = new Dictionary<string, Subscription>();
            mMktData = new Dictionary<string, BloombergData>();
            mTradingDates = new Dictionary<string, string>();
            mCorrelationID = new List<string>();
            mMsgCounter = 0;
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

        public void SendRequestForHistoricalData(Dictionary<string, ApiTaskHistoricalData> argvTasks)
        {
            //mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Sending request for historical data..."));
            //t_tasks = argv_tasks;
            foreach (string tTicker in argvTasks.Keys)
            {
                ApiTaskHistoricalData tTask = argvTasks[tTicker];
                // Create the Request object to represent the data request
                Request tRequest = mReferenceService.CreateRequest("HistoricalDataRequest");
                // Specify the security we are interested in
                tRequest.Append("securities", tTask.mTicker);
                // Specify the fields we are interested in 
                string[] tFields = tTask.GetFieldList();
                foreach (string tField in tFields)
                {
                    tRequest.Append("fields", tField);
                }
                // Set the start and ending dates and the max number of data points
                tRequest.Set("startDate", tTask.mStartTime.ToString("yyyyMMdd"));
                tRequest.Set("endDate", tTask.mEndTime.ToString("yyyyMMdd"));
                tRequest.Set("periodicitySelection", "DAILY");
                tRequest.Set("periodicityAdjustment", "ACTUAL");
                tRequest.Set("nonTradingDayFillOption", "ACTIVE_DAYS_ONLY");
                tRequest.Set("nonTradingDayFillMethod", "PREVIOUS_VALUE");
                tRequest.Set("pricingOption", "PRICING_OPTION_PRICE");
                tRequest.Set("overrideOption", "OVERRIDE_OPTION_CLOSE");
                tRequest.Set("maxDataPoints", Convert.ToInt32((tTask.mEndTime - tTask.mStartTime).TotalDays) + 1);
                tRequest.Set("adjustmentNormal", false);
                tRequest.Set("adjustmentAbnormal", false);
                tRequest.Set("adjustmentSplit", false);
                tRequest.Set("adjustmentFollowDPDF", true);
                // Submit the request
                mSession.SendRequest(tRequest, null);
                mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "HistoricalDataRequest: " + tTask.mTicker + " for fields " + tTask.mFields
                            + " from " + tTask.mStartTime.ToString("yyyyMMdd") + " to " + tTask.mEndTime.ToString("yyyyMMdd")));
            }
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
                eventTypes.AppendValue("TRADE");
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
                // If this event type is SUBSCRIPTION_STATUS then process the messages
                if (eventObject.Count() > 0)
                {
                    mMsgCounter++;
                    // If this event type is PARTIAL_RESPONSE/RESPONSE then process the messages
                    if (eventObject.Type == Event.EventType.PARTIAL_RESPONSE || eventObject.Type == Event.EventType.RESPONSE)
                    {
                        ProcessRequestResponse(eventObject);
                    } // End if event type is SUBSCRIPTION_DATA
                    // If this event type is SESSION_STATUS then process the messages
                    else if (eventObject.Type == Event.EventType.SESSION_STATUS)
                    {
                        ProcessSessionStatusChange(eventObject);
                    } // End if event type is SESSION_STATUS
                    // If this event type is SERVICE_STATUS then process the messages
                    else if (eventObject.Type == Event.EventType.SERVICE_STATUS)
                    {
                        ProcessSessionStatusChange(eventObject);
                    } // End if event type is SERVICE_STATUS
                    // If this event type is SUBSCRIPTION_DATA then process the messages
                    else if (eventObject.Type == Event.EventType.SUBSCRIPTION_DATA)
                    {
                        ProcessSubscriptionData(eventObject);
                    } // End if event type is SUBSCRIPTION_DATA
                    // If this event type is SUBSCRIPTION_STATUS then process the messages
                    else if (eventObject.Type == Event.EventType.SUBSCRIPTION_STATUS)
                    {
                        ProcessSubscriptionStatus(eventObject);
                    } // End if event type is SUBSCRIPTION_STATUS
                    else
                    {
                        mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "New event " + eventObject.Type.ToString()));
                        // Loop over all of the messages in this Event
                        foreach (Bloomberglp.Blpapi.Message msg in eventObject)
                        {
                            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Error, msg.ToString()));
                        }
                    } // End if event type is unknown events
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
                    Environment.Exit(0);
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
                //printMessage("New message : " + temp_element_msg.ToString());
                if (tMsg.MessageType.Equals(new Bloomberglp.Blpapi.Name("HistoricalDataResponse")))
                {
                    this.ProcessHistoricalDataResponse(tElementMsg);
                }
                else if (tMsg.MessageType.Equals(new Bloomberglp.Blpapi.Name("IntradayTickResponse")))
                {
                    //BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, msg.ToString()));
                    this.ProcessIntradayTickResponse(tElementMsg);
                }
                else if (tMsg.MessageType.Equals(new Bloomberglp.Blpapi.Name("IntradayBarResponse")))
                {
                    //BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, msg.ToString()));
                    this.ProcessIntradayBarResponse(tElementMsg);
                }
                else if (tMsg.MessageType.Equals(new Bloomberglp.Blpapi.Name("ReferenceDataResponse")))
                {
                    //BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, msg.ToString()));
                    this.ProcessReferenceDataResponse(tElementMsg);
                }
                else
                {
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, tMsg.ToString()));
                }
            }
        }

        private void ProcessHistoricalDataResponse(Bloomberglp.Blpapi.Element argvElement)
        {
            if (argvElement.HasElement("responseError"))
            {
                Bloomberglp.Blpapi.Element tResponseErrorArray = argvElement.GetElement("responseError");
                List<Bloomberglp.Blpapi.Element> tResponseErrorList = ConvertElementArrayToList(tResponseErrorArray);
                for (int i = 0; i < tResponseErrorList.Count; i++)
                {
                    Bloomberglp.Blpapi.Element tResponseError = tResponseErrorList.ElementAt(i);
                    mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Message error: " + tResponseError.GetElementAsString("message") + ", " + tResponseError.GetElementAsString("subcategory")));
                }
            }

            if (argvElement.HasElement("securityData"))
            {
                //printMessage("Parsing securityData...");
                Bloomberglp.Blpapi.Element tSecurityDataArray = argvElement.GetElement("securityData");
                List<Bloomberglp.Blpapi.Element> tSecurityDataList = ConvertElementArrayToList(tSecurityDataArray);
                for (int i = 0; i < tSecurityDataList.Count; i++)
                {
                    Bloomberglp.Blpapi.Element tSecurityData = tSecurityDataList.ElementAt(i);
                    if (tSecurityData.HasElement("security"))
                    {
                        //Ticker
                        string tTicker = tSecurityData.GetElementAsString("security");
                        mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Ticker is " + tTicker));
                        //Print out all expections
                        if (tSecurityData.HasElement("fieldExceptions"))
                        {
                            //printMessage("Parsing fieldExceptions...");
                            Bloomberglp.Blpapi.Element tFieldExceptionsArray = tSecurityData.GetElement("fieldExceptions");
                            List<Bloomberglp.Blpapi.Element> tFieldExceptionsList = ConvertElementArrayToList(tFieldExceptionsArray);
                            for (int j = 0; j < tFieldExceptionsList.Count; j++)
                            {
                                Bloomberglp.Blpapi.Element tFieldException = tFieldExceptionsList.ElementAt(j);
                                mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Field error: " + tFieldException.GetElementAsString("fieldId") + ", "
                                    + tFieldException.GetElement("errorInfo").GetElementAsString("message") + ", "
                                    + tFieldException.GetElement("errorInfo").GetElementAsString("subcategory")));
                            }
                        }
                        else
                        {
                            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Can't find fieldExceptions"));
                        }

                        //Handle filedData array
                        if (tSecurityData.HasElement("fieldData"))
                        {
                            //printMessage("Parsing fieldData...");
                            Bloomberglp.Blpapi.Element tFieldDataArray = tSecurityData.GetElement("fieldData");
                            //printMessage(temp_fieldDataArray.ToString());
                            List<Bloomberglp.Blpapi.Element> tFieldDataList = ConvertElementArrayToList(tFieldDataArray);
                            InterfaceEventArgs tEvent = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.RequestResponse);
                            tEvent.mBbgMsg = argvElement;
                            tEvent.AddData(tTicker);
                            for (int j = 0; j < tFieldDataList.Count; j++)
                            {
                                Bloomberglp.Blpapi.Element tFieldData = tFieldDataList.ElementAt(j);
                                string tPrintMsg = "Field data: " + tFieldData.GetElementAsString("date");
                                foreach (string tField in mTasks[tTicker].GetFieldList())
                                {
                                    string tTime = tFieldData.GetElementAsString("date");
                                    if (tFieldData.HasElement(tField))
                                    {
                                        tEvent.AddData(tTicker, tField, tTime, tFieldData.GetElementAsFloat64(tField).ToString());
                                    }
                                }
                            }
                            mBbgMsgEvent(this, tEvent);
                        }
                        else
                        {
                            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "Can't find fieldData"));
                        }
                    }
                }
            }
        }

        private void ProcessIntradayTickResponse(Bloomberglp.Blpapi.Element argvElement)
        {
            if (argvElement.HasElement("responseError"))
            {
                Bloomberglp.Blpapi.Element tResponseErrorArray = argvElement.GetElement("responseError");
                List<Bloomberglp.Blpapi.Element> tResponseErrorList = ConvertElementArrayToList(tResponseErrorArray);
                string tErrorMsg = string.Empty;
                for (int i = 0; i < tResponseErrorList.Count; i++)
                {
                    Bloomberglp.Blpapi.Element tResponseError = tResponseErrorList.ElementAt(i);
                    tErrorMsg += "Message error: " + tResponseError.GetElementAsString("message") + ", " + tResponseError.GetElementAsString("subcategory");
                }
                InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.IntradayTickResponse, tErrorMsg);
                mBbgMsgEvent(this, tEventArgvs);
            }
            else if (argvElement.HasElement("tickData"))
            {
                Dictionary<string, double> tTrades = new Dictionary<string, double>();
                Bloomberglp.Blpapi.Element tElementArray = argvElement.GetElement("tickData").GetElement("tickData");
                int tElementCount = tElementArray.NumValues;
                string tPrintMsg = string.Empty;
                InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.IntradayTickResponse);
                tEventArgvs.AddData(mLastIntradayTickTicker);
                for (int i = 0; i < tElementCount; i++)
                {
                    Bloomberglp.Blpapi.Element tElement = tElementArray.GetValueAsElement(i); ;
                    double tPrice = tElement.GetElementAsFloat64("value");
                    double tSize = tElement.GetElementAsFloat64("size");
                    string tType = tElement.GetElementAsString("type");
                    string tCode = string.Empty;
                    if (tElement.HasElement("conditionCodes"))
                    {
                        tCode = tElement.GetElementAsString("conditionCodes");
                    }
                    string tTradeID = string.Empty;
                    if (tElement.HasElement("tradeId"))
                    {
                        tTradeID = tElement.GetElementAsString("tradeId");
                    }
                    DateTime tTime = new DateTime(tElement.GetElementAsDatetime("time").ToSystemDateTime().Ticks, DateTimeKind.Utc);
                    tTime = tTime.ToLocalTime();
                    tPrintMsg += Environment.NewLine + tTime.ToString("yyyy-MM-dd HH:mm:ss") + ", " + tType + ", " + tCode
                        + ", " + tPrice.ToString() + ", " + tSize.ToString() + ", " + tTradeID;
                    string tId = tTradeID + tCode + tTime.ToString("yyyy-MM-dd HH:mm:ss") + tPrice.ToString();
                    if (tTrades.ContainsKey(tId))
                    {
                        tTrades[tId] += tSize;
                        tSize = tTrades[tId];
                    }
                    else
                    {
                        tTrades.Add(tId, tSize);
                    }

                    if (tSize > 0)
                    {
                        tEventArgvs.AddData(mLastIntradayTickTicker, tTradeID, tCode + "," + tTime.ToString("yyyy-MM-dd HH:mm:ss") + "," + tPrice.ToString(), tSize.ToString());
                    }
                }
                tEventArgvs.mMsg = tPrintMsg;
                mBbgMsgEvent(this, tEventArgvs);
            }
        }

        private void ProcessIntradayBarResponse(Bloomberglp.Blpapi.Element argvElement)
        {
            if (argvElement.HasElement("responseError"))
            {
                Bloomberglp.Blpapi.Element tResponseErrorArray = argvElement.GetElement("responseError");
                List<Bloomberglp.Blpapi.Element> tResponseErrorList = ConvertElementArrayToList(tResponseErrorArray);
                string tErrorMsg = string.Empty;
                for (int i = 0; i < tResponseErrorList.Count; i++)
                {
                    Bloomberglp.Blpapi.Element tResponseError = tResponseErrorList.ElementAt(i);
                    tErrorMsg += "Message error: " + tResponseError.GetElementAsString("message") + ", " + tResponseError.GetElementAsString("subcategory");
                }
                InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.IntradayBarResponse, tErrorMsg);
                mBbgMsgEvent(this, tEventArgvs);
            }
            else if (argvElement.HasElement("barData"))
            {
                Bloomberglp.Blpapi.Element tElementArray = argvElement.GetElement("barData").GetElement("barTickData");
                int tElementCount = tElementArray.NumValues;
                string tPrint = string.Empty;
                InterfaceEventArgs tEventArgvs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.IntradayBarResponse);
                tEventArgvs.AddData(mLastIntradayTickTicker);
                for (int i = 0; i < tElementCount; i++)
                {
                    Bloomberglp.Blpapi.Element tElement = tElementArray.GetValueAsElement(i); ;
                    double tOpen = tElement.GetElementAsFloat64("open");
                    double tHigh = tElement.GetElementAsFloat64("high");
                    double tLow = tElement.GetElementAsFloat64("low");
                    double tClose = tElement.GetElementAsFloat64("close");
                    DateTime tTime = new DateTime(tElement.GetElementAsDatetime("time").ToSystemDateTime().Ticks, DateTimeKind.Utc);
                    tTime = tTime.ToLocalTime();
                    tPrint += Environment.NewLine + tTime.ToString("yyyy-MM-dd HH:mm:ss") + ", " + tOpen.ToString() + ", " + tHigh.ToString()
                        + ", " + tLow.ToString() + ", " + tClose.ToString();

                    tEventArgvs.AddData(mLastIntradayTickTicker, mLastIntradayTickTicker, tTime.ToString("yyyy-MM-dd HH:mm:ss")
                        , tOpen.ToString() + "," + tHigh.ToString() + "," + tLow.ToString() + "," + tClose.ToString());
                }
                tEventArgvs.mMsg = tPrint;
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

                InterfaceEventArgs tArgs = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.SubscriptionResponse, tMsg.TopicName + ", " + tMsg.CorrelationID.Object.ToString() + ": Trading Date " + tTradingDate + ", BID " + tBid
                    + ", ASK " + tAsk);
                //+ ", ASK " + temp_ask + Environment.NewLine + msg.ToString());
                tArgs.AddData(tMsg.TopicName);
                tArgs.AddData(tMsg.TopicName, "BID", tTradingDate, tBid);
                tArgs.AddData(tMsg.TopicName, "ASK", tTradingDate, tAsk);
                mBbgMsgEvent(this, tArgs);
            }
        }

        private void ProcessReferenceDataResponse(Bloomberglp.Blpapi.Element argvElement)
        {
            InterfaceEventArgs tRetData = new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.ReferenceDataResponse);
            tRetData.mBbgMsg = argvElement;
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
            List<Subscription> tSubscription = new List<Subscription>();
            foreach (string tTicker in mSubscriptions.Keys)
            {
                if (mSubscriptions[tTicker].SubscriptionStatus != Session.SubscriptionStatus.UNSUBSCRIBED)
                {
                    tSubscription.Add(mSubscriptions[tTicker]);
                }
            }
            mSession.Unsubscribe(tSubscription);
            mBbgMsgWorker.Join();
            mSession.Stop();
        }
    }
}
