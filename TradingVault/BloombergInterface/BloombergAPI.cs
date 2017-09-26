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
                    mBbgMsgWorker = new System.Threading.Thread(process_bbg_event);
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
                string[] temp_fields = tTask.GetFieldList();
                foreach (string temp_field in temp_fields)
                {
                    tRequest.Append("fields", temp_field);
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
            int temp_max_data_points = (int)Math.Round((argvTask.mEndTime - argvTask.mStartTime).TotalMinutes / argvTask.mBarLengthInMinute + 10);
            tRequest.Set("maxDataPoints", temp_max_data_points);
            mLastIntradayTickTicker = argvTask.mBbgTicker;
            // Submit the request
            mSession.SendRequest(tRequest, null);
            mBbgMsgEvent(this, new InterfaceEventArgs(InterfaceEventArgs.xBbgMsgType.Print, "IntradayBarRequest: " + argvTask.mTicker + " for fields " + argvTask.mFields
                        + " from " + argvTask.mStartTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss") + " to " + argvTask.mEndTime.ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss")));
        }

        public void SubscribeMktData(Dictionary<string, ApiTaskRealtime> argv_tasks)
        {
            List<Subscription> temp_newTask = new List<Subscription>();
            foreach (string temp_ticker in argv_tasks.Keys)
            {
                if (!mSubscriptions.ContainsKey(temp_ticker))
                {
                    CorrelationID temp_id = new CorrelationID(temp_ticker + " GSG1900");
                    t_correlationID.Add(temp_id.Object.ToString());
                    List<string> temp_fields = argv_tasks[temp_ticker].getFieldList().ToList();
                    List<string> temp_options = new List<string>();
                    temp_options.Add("interval=" + argv_tasks[temp_ticker].t_subscription_interval.ToString());
                    Bloomberglp.Blpapi.Subscription temp_sub = new Subscription(temp_ticker, temp_fields, temp_options, temp_id);
                    t_subscriptions.Add(temp_ticker, temp_sub);
                    temp_newTask.Add(temp_sub);
                    t_mktData.Add(temp_ticker, new BBGData(temp_ticker));
                    t_tradingDates.Add(temp_ticker, DateTime.Today.ToString("yyyy-MM-dd"));
                }
            }

            session.Subscribe(temp_newTask);
        }

        private void process_bbg_event()
        {
            BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, ("Listening to event...")));
            // Now we can process the incoming Events
            while (is_session_running)
            {
                // Grab the next Event object
                Event eventObject = session.NextEvent(100);
                // If this event type is SUBSCRIPTION_STATUS then process the messages
                if (eventObject.Count() > 0)
                {
                    t_msgCounter++;
                    // If this event type is PARTIAL_RESPONSE/RESPONSE then process the messages
                    if (eventObject.Type == Event.EventType.PARTIAL_RESPONSE || eventObject.Type == Event.EventType.RESPONSE)
                    {
                        process_response(eventObject);
                    } // End if event type is SUBSCRIPTION_DATA
                    // If this event type is SESSION_STATUS then process the messages
                    else if (eventObject.Type == Event.EventType.SESSION_STATUS)
                    {
                        process_session_status(eventObject);
                    } // End if event type is SESSION_STATUS
                    // If this event type is SERVICE_STATUS then process the messages
                    else if (eventObject.Type == Event.EventType.SERVICE_STATUS)
                    {
                        process_session_status(eventObject);
                    } // End if event type is SERVICE_STATUS
                    // If this event type is SUBSCRIPTION_DATA then process the messages
                    else if (eventObject.Type == Event.EventType.SUBSCRIPTION_DATA)
                    {
                        process_subscription_data(eventObject);
                    } // End if event type is SUBSCRIPTION_DATA
                    // If this event type is SUBSCRIPTION_STATUS then process the messages
                    else if (eventObject.Type == Event.EventType.SUBSCRIPTION_STATUS)
                    {
                        process_subscription_status(eventObject);
                    } // End if event type is SUBSCRIPTION_STATUS
                    else
                    {
                        BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, "New event " + eventObject.Type.ToString()));
                        // Loop over all of the messages in this Event
                        foreach (Bloomberglp.Blpapi.Message msg in eventObject)
                        {
                            BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Error, msg.ToString()));
                        }
                    } // End if event type is unknown events
                }
            } // end while not done
        }

        private void process_session_status(Bloomberglp.Blpapi.Event par_event)
        {
            foreach (Bloomberglp.Blpapi.Message msg in par_event)
            {
                BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, msg.ToString()));

                if (msg.MessageType == Bloomberglp.Blpapi.Name.FindName("SessionTerminated") || msg.MessageType == Bloomberglp.Blpapi.Name.FindName("SessionConnectionDown"))
                {
                    is_session_running = false;
                    BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.LoginFail, msg.ToString()));
                    Environment.Exit(0);
                }

                if (msg.MessageType == Bloomberglp.Blpapi.Name.FindName("SessionStarted"))
                {
                    is_session_running = true;
                    BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.LoginSuccess, "LoginSuccess"));
                }
            }
        }

        private void process_response(Bloomberglp.Blpapi.Event par_event)
        {
            // Loop over all of the messages in this Event
            foreach (Bloomberglp.Blpapi.Message msg in par_event)
            {
                Bloomberglp.Blpapi.Element temp_element_msg = msg.AsElement;
                //printMessage("New message : " + temp_element_msg.ToString());
                if (msg.MessageType.Equals(new Bloomberglp.Blpapi.Name("HistoricalDataResponse")))
                {
                    this.process_historicalDataResponse(temp_element_msg);
                }
                else if (msg.MessageType.Equals(new Bloomberglp.Blpapi.Name("IntradayTickResponse")))
                {
                    //BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, msg.ToString()));
                    this.process_intradayTickResponse(temp_element_msg);
                }
                else if (msg.MessageType.Equals(new Bloomberglp.Blpapi.Name("IntradayBarResponse")))
                {
                    //BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, msg.ToString()));
                    this.process_intradayBarResponse(temp_element_msg);
                }
                else if (msg.MessageType.Equals(new Bloomberglp.Blpapi.Name("ReferenceDataResponse")))
                {
                    //BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, msg.ToString()));
                    this.Process_Reference_Data_Response(temp_element_msg);
                }
                else
                {
                    BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, msg.ToString()));
                }
            }
        }

        private void process_historicalDataResponse(Bloomberglp.Blpapi.Element argv_element)
        {
            if (argv_element.HasElement("responseError"))
            {
                Bloomberglp.Blpapi.Element temp_responseErrorArray = argv_element.GetElement("responseError");
                List<Bloomberglp.Blpapi.Element> temp_responseErrorList = convertElementArrayToList(temp_responseErrorArray);
                for (int i = 0; i < temp_responseErrorList.Count; i++)
                {
                    Bloomberglp.Blpapi.Element temp_responseError = temp_responseErrorList.ElementAt(i);
                    BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, "Message error: " + temp_responseError.GetElementAsString("message") + ", " + temp_responseError.GetElementAsString("subcategory")));
                }
            }

            if (argv_element.HasElement("securityData"))
            {
                //printMessage("Parsing securityData...");
                Bloomberglp.Blpapi.Element temp_securityDataArray = argv_element.GetElement("securityData");
                List<Bloomberglp.Blpapi.Element> temp_securityDataList = convertElementArrayToList(temp_securityDataArray);
                for (int i = 0; i < temp_securityDataList.Count; i++)
                {
                    Bloomberglp.Blpapi.Element temp_securityData = temp_securityDataList.ElementAt(i);
                    if (temp_securityData.HasElement("security"))
                    {
                        //Ticker
                        string temp_ticker = temp_securityData.GetElementAsString("security");
                        BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, "Ticker is " + temp_ticker));
                        //Print out all expections
                        if (temp_securityData.HasElement("fieldExceptions"))
                        {
                            //printMessage("Parsing fieldExceptions...");
                            Bloomberglp.Blpapi.Element temp_fieldExceptionsArray = temp_securityData.GetElement("fieldExceptions");
                            List<Bloomberglp.Blpapi.Element> temp_fieldExceptionsList = convertElementArrayToList(temp_fieldExceptionsArray);
                            for (int j = 0; j < temp_fieldExceptionsList.Count; j++)
                            {
                                Bloomberglp.Blpapi.Element temp_fieldException = temp_fieldExceptionsList.ElementAt(j);
                                BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, "Field error: " + temp_fieldException.GetElementAsString("fieldId") + ", "
                                    + temp_fieldException.GetElement("errorInfo").GetElementAsString("message") + ", "
                                    + temp_fieldException.GetElement("errorInfo").GetElementAsString("subcategory")));
                            }
                        }
                        else
                        {
                            BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, "Can't find fieldExceptions"));
                        }

                        //Handle filedData array
                        if (temp_securityData.HasElement("fieldData"))
                        {
                            //printMessage("Parsing fieldData...");
                            Bloomberglp.Blpapi.Element temp_fieldDataArray = temp_securityData.GetElement("fieldData");
                            //printMessage(temp_fieldDataArray.ToString());
                            List<Bloomberglp.Blpapi.Element> temp_fieldDataList = convertElementArrayToList(temp_fieldDataArray);
                            BBGEventArgs temp_event = new BBGEventArgs(BBGEventArgs.BBG_MsgType.RequestResponse);
                            temp_event.t_bbg_msg = argv_element;
                            temp_event.AddData(temp_ticker);
                            for (int j = 0; j < temp_fieldDataList.Count; j++)
                            {
                                Bloomberglp.Blpapi.Element temp_fieldData = temp_fieldDataList.ElementAt(j);
                                string temp_printMsg = "Field data: " + temp_fieldData.GetElementAsString("date");
                                foreach (string temp_field in t_tasks[temp_ticker].getFieldList())
                                {
                                    string temp_time = temp_fieldData.GetElementAsString("date");
                                    if (temp_fieldData.HasElement(temp_field))
                                    {
                                        temp_event.AddData(temp_ticker, temp_field, temp_time, temp_fieldData.GetElementAsFloat64(temp_field).ToString());
                                    }
                                }
                            }
                            BBG_Msg(this, temp_event);
                        }
                        else
                        {
                            BBG_Msg(this, new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, "Can't find fieldData"));
                        }
                    }
                }
            }
        }

        private void process_intradayTickResponse(Bloomberglp.Blpapi.Element argv_element)
        {
            if (argv_element.HasElement("responseError"))
            {
                Bloomberglp.Blpapi.Element temp_responseErrorArray = argv_element.GetElement("responseError");
                List<Bloomberglp.Blpapi.Element> temp_responseErrorList = convertElementArrayToList(temp_responseErrorArray);
                string temp_errorMsg = string.Empty;
                for (int i = 0; i < temp_responseErrorList.Count; i++)
                {
                    Bloomberglp.Blpapi.Element temp_responseError = temp_responseErrorList.ElementAt(i);
                    temp_errorMsg += "Message error: " + temp_responseError.GetElementAsString("message") + ", " + temp_responseError.GetElementAsString("subcategory");
                }
                BBGEventArgs temp_eventArgvs = new BBGEventArgs(BBGEventArgs.BBG_MsgType.IntradayTickResponse, temp_errorMsg);
                BBG_Msg(this, temp_eventArgvs);
            }
            else if (argv_element.HasElement("tickData"))
            {
                Dictionary<string, double> temp_trades = new Dictionary<string, double>();
                Bloomberglp.Blpapi.Element temp_elementArray = argv_element.GetElement("tickData").GetElement("tickData");
                int temp_elementCount = temp_elementArray.NumValues;
                string temp_print = string.Empty;
                BBGEventArgs temp_eventArgvs = new BBGEventArgs(BBGEventArgs.BBG_MsgType.IntradayTickResponse);
                temp_eventArgvs.AddData(t_lastIntradayTickTicker);
                for (int i = 0; i < temp_elementCount; i++)
                {
                    Bloomberglp.Blpapi.Element temp_element = temp_elementArray.GetValueAsElement(i); ;
                    double temp_px = temp_element.GetElementAsFloat64("value");
                    double temp_size = temp_element.GetElementAsFloat64("size");
                    string temp_type = temp_element.GetElementAsString("type");
                    string temp_code = string.Empty;
                    if (temp_element.HasElement("conditionCodes"))
                    {
                        temp_code = temp_element.GetElementAsString("conditionCodes");
                    }
                    string temp_tradeID = string.Empty;
                    if (temp_element.HasElement("tradeId"))
                    {
                        temp_tradeID = temp_element.GetElementAsString("tradeId");
                    }
                    DateTime temp_time = new DateTime(temp_element.GetElementAsDatetime("time").ToSystemDateTime().Ticks, DateTimeKind.Utc);
                    temp_time = temp_time.ToLocalTime();
                    temp_print += Environment.NewLine + temp_time.ToString("yyyy-MM-dd HH:mm:ss") + ", " + temp_type + ", " + temp_code
                        + ", " + temp_px.ToString() + ", " + temp_size.ToString() + ", " + temp_tradeID;
                    string temp_id = temp_tradeID + temp_code + temp_time.ToString("yyyy-MM-dd HH:mm:ss") + temp_px.ToString();
                    if (temp_trades.ContainsKey(temp_id))
                    {
                        temp_trades[temp_id] += temp_size;
                        temp_size = temp_trades[temp_id];
                    }
                    else
                    {
                        temp_trades.Add(temp_id, temp_size);
                    }

                    if (temp_size > 0)
                    {
                        temp_eventArgvs.AddData(t_lastIntradayTickTicker, temp_tradeID, temp_code + "," + temp_time.ToString("yyyy-MM-dd HH:mm:ss") + "," + temp_px.ToString(), temp_size.ToString());
                    }
                }
                temp_eventArgvs.t_msg = temp_print;
                BBG_Msg(this, temp_eventArgvs);
            }
        }

        private void process_intradayBarResponse(Bloomberglp.Blpapi.Element argv_element)
        {
            if (argv_element.HasElement("responseError"))
            {
                Bloomberglp.Blpapi.Element temp_responseErrorArray = argv_element.GetElement("responseError");
                List<Bloomberglp.Blpapi.Element> temp_responseErrorList = convertElementArrayToList(temp_responseErrorArray);
                string temp_errorMsg = string.Empty;
                for (int i = 0; i < temp_responseErrorList.Count; i++)
                {
                    Bloomberglp.Blpapi.Element temp_responseError = temp_responseErrorList.ElementAt(i);
                    temp_errorMsg += "Message error: " + temp_responseError.GetElementAsString("message") + ", " + temp_responseError.GetElementAsString("subcategory");
                }
                BBGEventArgs temp_eventArgvs = new BBGEventArgs(BBGEventArgs.BBG_MsgType.IntradayBarResponse, temp_errorMsg);
                BBG_Msg(this, temp_eventArgvs);
            }
            else if (argv_element.HasElement("barData"))
            {
                Bloomberglp.Blpapi.Element temp_elementArray = argv_element.GetElement("barData").GetElement("barTickData");
                int temp_elementCount = temp_elementArray.NumValues;
                string temp_print = string.Empty;
                BBGEventArgs temp_eventArgvs = new BBGEventArgs(BBGEventArgs.BBG_MsgType.IntradayBarResponse);
                temp_eventArgvs.AddData(t_lastIntradayTickTicker);
                for (int i = 0; i < temp_elementCount; i++)
                {
                    Bloomberglp.Blpapi.Element temp_element = temp_elementArray.GetValueAsElement(i); ;
                    double temp_open = temp_element.GetElementAsFloat64("open");
                    double temp_high = temp_element.GetElementAsFloat64("high");
                    double temp_low = temp_element.GetElementAsFloat64("low");
                    double temp_close = temp_element.GetElementAsFloat64("close");
                    DateTime temp_time = new DateTime(temp_element.GetElementAsDatetime("time").ToSystemDateTime().Ticks, DateTimeKind.Utc);
                    temp_time = temp_time.ToLocalTime();
                    temp_print += Environment.NewLine + temp_time.ToString("yyyy-MM-dd HH:mm:ss") + ", " + temp_open.ToString() + ", " + temp_high.ToString()
                        + ", " + temp_low.ToString() + ", " + temp_close.ToString();

                    temp_eventArgvs.AddData(t_lastIntradayTickTicker, t_lastIntradayTickTicker, temp_time.ToString("yyyy-MM-dd HH:mm:ss")
                        , temp_open.ToString() + "," + temp_high.ToString() + "," + temp_low.ToString() + "," + temp_close.ToString());
                }
                temp_eventArgvs.t_msg = temp_print;
                BBG_Msg(this, temp_eventArgvs);
            }
        }

        private void process_subscription_status(Bloomberglp.Blpapi.Event par_event)
        {
            foreach (Bloomberglp.Blpapi.Message msg in par_event)
            {
                foreach (CorrelationID temp_id in msg.CorrelationIDs)
                {
                    if (msg.MessageType.ToString() == "SubscriptionFailure")
                    {
                        BBGEventArgs temp_args = new BBGEventArgs(BBGEventArgs.BBG_MsgType.Error, "Correlation ID: " + msg.CorrelationID + ", Msg Type: " + msg.MessageType);
                        BBG_Msg(this, temp_args);
                    }
                    //else
                    //{
                    //    BBGEventArgs temp_args = new BBGEventArgs(BBGEventArgs.BBG_MsgType.Print, "Correlation ID: " + msg.CorrelationID + ", Msg Type: " + msg.MessageType);
                    //    BBG_Msg(this, temp_args);
                    //}
                }
            }
        }

        private void process_subscription_data(Bloomberglp.Blpapi.Event par_event)
        {
            foreach (Bloomberglp.Blpapi.Message msg in par_event)
            {
                string temp_ticker = msg.TopicName;

                if (msg.HasElement("TRADING_DT_REALTIME"))
                {
                    t_tradingDates[temp_ticker] = msg.GetElementAsString("TRADING_DT_REALTIME");
                }
                string temp_tradingDate = t_tradingDates[temp_ticker];
                if (msg.HasElement("BID"))
                {
                    if (!msg.GetElement("BID").IsNull)
                    {
                        string temp_value = msg.GetElementAsString("BID");
                        if (temp_value.Length == 0)
                        {
                            temp_value = "0";
                        }
                        t_mktData[temp_ticker].Update("BID", temp_tradingDate, temp_value);
                    }
                }

                if (msg.HasElement("ASK"))
                {
                    if (!msg.GetElement("ASK").IsNull)
                    {
                        string temp_value = msg.GetElementAsString("ASK");
                        if (temp_value.Length == 0)
                        {
                            temp_value = "0";
                        }
                        t_mktData[temp_ticker].Update("ASK", temp_tradingDate, temp_value);
                    }
                }

                string temp_bid = t_mktData[temp_ticker].GetValue("BID", temp_tradingDate);
                string temp_ask = t_mktData[temp_ticker].GetValue("ASK", temp_tradingDate);

                BBGEventArgs temp_args = new BBGEventArgs(BBGEventArgs.BBG_MsgType.SubscriptionResponse, msg.TopicName + ", " + msg.CorrelationID.Object.ToString() + ": Trading Date " + temp_tradingDate + ", BID " + temp_bid
                    + ", ASK " + temp_ask);
                //+ ", ASK " + temp_ask + Environment.NewLine + msg.ToString());
                temp_args.AddData(msg.TopicName);
                temp_args.AddData(msg.TopicName, "BID", temp_tradingDate, temp_bid);
                temp_args.AddData(msg.TopicName, "ASK", temp_tradingDate, temp_ask);
                BBG_Msg(this, temp_args);
            }
        }

        private void Process_Reference_Data_Response(Bloomberglp.Blpapi.Element argv_element)
        {
            BBGEventArgs temp_ret_data = new BBGEventArgs(BBGEventArgs.BBG_MsgType.ReferenceDataResponse);
            temp_ret_data.t_bbg_msg = argv_element;
            BBG_Msg(this, temp_ret_data);
        }

        public System.Data.DataTable Extract_Value_By_Name(Bloomberglp.Blpapi.Element argv_element, List<string> argv_names)
        {
            System.Data.DataTable temp_return_value = new System.Data.DataTable();
            List<string> temp_upper_case_names = new List<string>();
            foreach (string temp_name in argv_names)
            {
                temp_upper_case_names.Add(temp_name.ToUpper());
                temp_return_value.Columns.Add(temp_name.ToUpper());
            }

            if (argv_element.Datatype == Schema.Datatype.CHOICE || argv_element.Datatype == Schema.Datatype.SEQUENCE)
            {
                List<Bloomberglp.Blpapi.Element> temp_sub_element_array = new List<Element>();
                if (argv_element.IsArray)
                {
                    for (int i = 0; i < argv_element.NumValues; i++)
                    {
                        temp_sub_element_array.Add((Bloomberglp.Blpapi.Element)argv_element.GetValue(i));
                    }
                }
                else
                {
                    foreach (Bloomberglp.Blpapi.Element temp_sub_element in argv_element.Elements)
                    {
                        temp_sub_element_array.Add(temp_sub_element);
                    }
                }

                bool temp_row_is_added = false;
                foreach (Bloomberglp.Blpapi.Element temp_sub_element in temp_sub_element_array)
                {
                    //Console.WriteLine(temp_sub_element.Name.ToString() + " type is " + temp_sub_element.Datatype.ToString());
                    if (temp_sub_element.Datatype == Schema.Datatype.CHOICE || temp_sub_element.Datatype == Schema.Datatype.SEQUENCE)
                    {
                        System.Data.DataTable temp_lower_level_value_table = this.Extract_Value_By_Name(temp_sub_element, temp_upper_case_names);
                        foreach (System.Data.DataRow temp_lower_level_value_row in temp_lower_level_value_table.Rows)
                        {
                            System.Data.DataRow temp_new_row = temp_return_value.NewRow();
                            foreach (System.Data.DataColumn temp_column in temp_lower_level_value_table.Columns)
                            {
                                temp_new_row[temp_column.ColumnName] = temp_lower_level_value_row[temp_column.ColumnName];
                            }
                            temp_return_value.Rows.Add(temp_new_row);
                        }
                    }
                    else if (temp_sub_element.NumValues > 0)
                    {
                        //Console.WriteLine(temp_sub_element.Name.ToString() + " value is " + temp_sub_element.GetValueAsString(0));
                        if (temp_upper_case_names.Contains(temp_sub_element.Name.ToString().ToUpper()))
                        {
                            if (!temp_row_is_added)
                            {
                                System.Data.DataRow temp_new_row = temp_return_value.NewRow();
                                temp_return_value.Rows.Add(temp_new_row);
                                temp_row_is_added = true;
                            }
                            temp_return_value.Rows[temp_return_value.Rows.Count - 1][temp_sub_element.Name.ToString().ToUpper()] = temp_sub_element.GetValueAsString(0);
                        }
                    }
                }
            }

            return temp_return_value;
        }

        private List<Bloomberglp.Blpapi.Element> convertElementArrayToList(Bloomberglp.Blpapi.Element argv_element)
        {
            List<Bloomberglp.Blpapi.Element> ret = new List<Element>();

            if (argv_element.IsArray)
            {
                for (int i = 0; i < argv_element.NumValues; i++)
                {
                    ret.Add(argv_element.GetValueAsElement(i));
                }
            }
            else
            {
                ret.Add(argv_element);
            }

            return ret;
        }

        public double getMessageCount()
        {
            return t_msgCounter;
        }

        public void shutDown()
        {
            is_session_running = false;
            List<Subscription> temp_subs = new List<Subscription>();
            foreach (string temp_ticker in t_subscriptions.Keys)
            {
                if (t_subscriptions[temp_ticker].SubscriptionStatus != Session.SubscriptionStatus.UNSUBSCRIBED)
                {
                    temp_subs.Add(t_subscriptions[temp_ticker]);
                }
            }
            session.Unsubscribe(temp_subs);
            t_bbg_msg_worker.Join();
            session.Stop();
        }
    }
}
