﻿/* Copyright 2013. Bloomberg Finance L.P.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:  The above
 * copyright notice and this permission notice shall be included in all copies
 * or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

using Name = Bloomberglp.Blpapi.Name;
using SessionOptions = Bloomberglp.Blpapi.SessionOptions;
using Session = Bloomberglp.Blpapi.Session;
using Service = Bloomberglp.Blpapi.Service;
using Request = Bloomberglp.Blpapi.Request;
using Element = Bloomberglp.Blpapi.Element;
using CorrelationID = Bloomberglp.Blpapi.CorrelationID;
using Event = Bloomberglp.Blpapi.Event;
using Message = Bloomberglp.Blpapi.Message;
using EventHandler = Bloomberglp.Blpapi.EventHandler;
using Subscription = Bloomberglp.Blpapi.Subscription;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System;
using System.Threading;

namespace com.bloomberg.samples
{
    public class EMSXTrigger
    {

        /* Run with:-
         *     EMSXTrigger 
	     *         SELLAT=[BID,ASK] BUYAT=[BID,ASK] AMOUNT=1000 TICKER="IBM US Equity" 
	     */

        private static readonly Name ORDER_ROUTE_FIELDS = new Name("OrderRouteFields");
        private static readonly Name ERROR_INFO = new Name("ErrorInfo");
        private static readonly Name CREATE_ORDER_AND_ROUTE_EX = new Name("CreateOrderAndRouteEx");


        // ADMIN
        private static readonly Name SLOW_CONSUMER_WARNING = new Name("SlowConsumerWarning");
        private static readonly Name SLOW_CONSUMER_WARNING_CLEARED = new Name("SlowConsumerWarningCleared");

        // SESSION_STATUS
        private static readonly Name SESSION_STARTED = new Name("SessionStarted");
        private static readonly Name SESSION_TERMINATED = new Name("SessionTerminated");
        private static readonly Name SESSION_STARTUP_FAILURE = new Name("SessionStartupFailure");
        private static readonly Name SESSION_CONNECTION_UP = new Name("SessionConnectionUp");
        private static readonly Name SESSION_CONNECTION_DOWN = new Name("SessionConnectionDown");

        // SERVICE_STATUS
        private static readonly Name SERVICE_OPENED = new Name("ServiceOpened");
        private static readonly Name SERVICE_OPEN_FAILURE = new Name("ServiceOpenFailure");

        // SUBSCRIPTION_STATUS + SUBSCRIPTION_DATA
        private static readonly Name SUBSCRIPTION_FAILURE = new Name("SubscriptionFailure");
        private static readonly Name SUBSCRIPTION_STARTED = new Name("SubscriptionStarted");
        private static readonly Name SUBSCRIPTION_TERMINATED = new Name("SubscriptionTerminated");
        
        private static readonly Name SECURITY_DATA = new Name("securityData");
        private static readonly Name SECURITY = new Name("security");
        private static readonly Name FIELD_DATA = new Name("fieldData");

        private Subscription emsx_order_sub;
        private Subscription emsx_route_sub;
        private List<Subscription> emsxOrderSubscription = new List<Subscription>();
        private List<Subscription> emsxRouteSubscription = new List<Subscription>();
        private CorrelationID order_sub_id;
        private CorrelationID route_sub_id;

        private Subscription mktdata_sub;
        private List<Subscription> mktdata_subscriptions = new List<Subscription>();
        private CorrelationID mktdata_sub_id;

        private Request request;
        private CorrelationID refdata_req_id;
      
        private string d_host;
        private int d_port;

        private static string service_mktdata = "//blp/mktdata";
        private static string service_emsx = "//blp/emapisvc_beta";
        private static string service_refdata = "//blp/refdata";

        private Service refdata;
        private Service emapisvc;

        private string[] pricePoints = {"BID","ASK"};

        private String sellat;
        private String buyat;
        private int amount = 0;
        private string ticker = "";

        private int emsx_req_id = 100;

        public ConcurrentQueue<Request> pendingTrades = new ConcurrentQueue<Request>();
        public ConcurrentDictionary<CorrelationID, Request> outstandingRequests = new ConcurrentDictionary<CorrelationID,Request>();
        public static readonly object logLock = new object();

        private double lastBid=0;
        private double lastAsk=0;

        private Dispatcher dispatcher;
        Thread dispatcherThread;

        public static void Main(string[] args)
        {
            log("Bloomberg - EMSX API Example - EMSXTrigger");
            log("Press ENTER at anytime to quit");

            EMSXTrigger example = new EMSXTrigger();
            example.run(args);

        }

        public EMSXTrigger()
        {

            // Define the service required, in this case the beta service, 
            // and the values to be used by the SessionOptions object
            // to identify IP/port of the back-end process.

            d_host = "localhost";
            d_port = 8194;

        }

        private void run(string[] args)
        {

            if (!parseCommandLine(args)) return;

            showParameters();

            SessionOptions d_sessionOptions = new SessionOptions();
            d_sessionOptions.ServerHost = d_host;
            d_sessionOptions.ServerPort = d_port;

            Session session = new Session(d_sessionOptions, new EventHandler(processEvent));

            dispatcher = new Dispatcher(session, pendingTrades, outstandingRequests);
            dispatcherThread = new Thread(new ThreadStart(dispatcher.dispatch));
            dispatcherThread.Start();

            session.StartAsync();

            System.Console.Read();
            dispatcherThread.Abort();
            dispatcherThread.Join();

        }

        public void processEvent(Event evt, Session session)
        {
            try
            {
                switch (evt.Type)
                {
                    case Event.EventType.ADMIN:
                        processAdminEvent(evt, session);
                        break;
                    case Event.EventType.SESSION_STATUS:
                        processSessionEvent(evt, session);
                        break;
                    case Event.EventType.SERVICE_STATUS:
                        processServiceEvent(evt, session);
                        break;
                    case Event.EventType.SUBSCRIPTION_DATA:
                        processSubscriptionDataEvent(evt, session);
                        break;
                    case Event.EventType.SUBSCRIPTION_STATUS:
                        processSubscriptionStatusEvent(evt, session);
                        break;
                    case Event.EventType.RESPONSE:
                    case Event.EventType.PARTIAL_RESPONSE:
                        processResponseEvent(evt, session);
                        break;
                    default:
                        processMiscEvent(evt, session);
                        break;
                }
            }
            catch (Exception e)
            {
                logErr("Error: " + e.ToString());
            }
        }

        private void processAdminEvent(Event evt, Session session)
        {
            log("Processing " + evt.Type);

            foreach (Message msg in evt)
            {
                if (msg.MessageType.Equals(SLOW_CONSUMER_WARNING))
                {
                    logErr("Warning: Entered Slow Consumer status");
                }
                else if (msg.MessageType.Equals(SLOW_CONSUMER_WARNING_CLEARED))
                {
                    logErr("Slow consumer status cleaered");
                }
            }
        }

        private void processSessionEvent(Event evt, Session session)
        {
            log("Processing " + evt.Type);
            foreach (Message msg in evt)
            {
                if (msg.MessageType.Equals(SESSION_STARTED))
                {
                    log("Session started...");
                    session.OpenServiceAsync(service_refdata);
                }
                else if (msg.MessageType.Equals(SESSION_STARTUP_FAILURE))
                {
                    logErr("Error: Session startup failed");
                }
                else if (msg.MessageType.Equals(SESSION_TERMINATED))
                {
                    log("Error: Session has been terminated");
                }
                else if (msg.MessageType.Equals(SESSION_CONNECTION_UP))
                {
                    log("Session connection is up");
                }
                else if (msg.MessageType.Equals(SESSION_CONNECTION_DOWN))
                {
                    log("Error: Session connection is down");
                }
            }
        }

        private void processServiceEvent(Event evt, Session session)
        {
            log("Processing " + evt.Type);

            foreach (Message msg in evt)
            {
                // Identify which service this message belongs to...
                String svc = msg.GetElementAsString("serviceName");

                if (msg.MessageType.Equals(SERVICE_OPENED))
                {
                    log("Service opened [" + svc + "]...");

                    if (svc == service_refdata)
                    {
                        sendRefDataRequest(session);
                    }
                    else if (svc == service_mktdata)
                    {
                        createMarketDataSubscription(session);
                    }
                    else if (svc == service_emsx)
                    {
                        emapisvc = session.GetService(service_emsx);
                        createOrderSubscription(session);
                    }
                }
                else if (msg.MessageType.Equals(SERVICE_OPEN_FAILURE))
                {
                    logErr("Error: Service failed to open [" + svc + "]");
                }
            }
        }

        private void processSubscriptionStatusEvent(Event evt, Session session)
        {
            log("Processing " + evt.Type);
            foreach (Message msg in evt)
            {
                if (msg.MessageType.Equals(SUBSCRIPTION_STARTED))
                {
                    if (msg.CorrelationID == mktdata_sub_id)
                    {
                        log("Subscription started [" + service_mktdata + "]");
                    }
                    else if (msg.CorrelationID == order_sub_id)
                    {
                        log("Subscription started [" + service_emsx + " -  Orders]");
                        // create the Route subscription
                        createRouteSubscription(session);
                    }
                    else if (msg.CorrelationID == route_sub_id)
                    {
                        log("Subscription started [" + service_emsx + " - Routes]");
                        // Open the market data service.
                        session.OpenServiceAsync(service_mktdata);
                    }

                }
                else if (msg.MessageType.Equals(SUBSCRIPTION_FAILURE))
                {
                    logErr("Error: Subscription failed");
                    log("MESSAGE: " + msg);
                }
                else if (msg.MessageType.Equals(SUBSCRIPTION_TERMINATED))
                {
                    logErr("Error: Subscription terminated");
                    log("MESSAGE: " + msg);
                }
            }
        }

        private void processSubscriptionDataEvent(Event evt, Session session)
        {
            //log("Processing " + evt.Type);
            foreach (Message msg in evt)
            {
                if (msg.CorrelationID == mktdata_sub_id)
                {
                    double bidPrice = msg.HasElement("BID") ? msg.GetElementAsFloat64("BID") : 0;
                    double askPrice = msg.HasElement("ASK") ? msg.GetElementAsFloat64("ASK") : 0;

                    if (bidPrice > 0) lastBid = bidPrice;
                    if (askPrice > 0) lastAsk = askPrice;

                    if ((bidPrice > 0 || askPrice > 0) && (lastBid > 0 && lastAsk > 0))
                    {

                        log("Trading at " + bidPrice + "/" + askPrice);
                        sendTradeRequest(session, "BUY", buyat == "BID" ? lastBid : lastAsk);
                        sendTradeRequest(session, "SELL", sellat == "BID" ? lastBid : lastAsk);
                    }
                }
                else if (msg.CorrelationID == order_sub_id || msg.CorrelationID == route_sub_id)
                {

                    int eventStatus = msg.GetElementAsInt32("EVENT_STATUS");

                    if (eventStatus == 1)
                    {
                        System.Console.Write(".");
                    }
                    else
                    {
                        string msgSubType = msg.HasElement("MSG_SUB_TYPE") ? msg.GetElementAsString("MSG_SUB_TYPE") : "";

                        if (msgSubType == "O")
                        {
                            int emsxSequence = msg.HasElement("EMSX_SEQUENCE") ? msg.GetElementAsInt32("EMSX_SEQUENCE") : 0;
                            string emsxStatus = msg.HasElement("EMSX_STATUS") ? msg.GetElementAsString("EMSX_STATUS") : "";
                            string emsxTicker = msg.HasElement("EMSX_TICKER") ? msg.GetElementAsString("EMSX_TICKER") : "";
                            int emsxAmount = msg.HasElement("EMSX_AMOUNT") ? msg.GetElementAsInt32("EMSX_AMOUNT") : 0;
                            int emsxWorking = msg.HasElement("EMSX_WORKING") ? msg.GetElementAsInt32("EMSX_WORKING") : 0;
                            int emsxFilled = msg.HasElement("EMSX_FILLED") ? msg.GetElementAsInt32("EMSX_FILLED") : 0;
                            double emsxAvgPrice = msg.HasElement("EMSX_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_AVG_PRICE") : 0;

                            log("Order event (" + eventStatus + "): Sequence=" + emsxSequence + "\tStatus=" + emsxStatus + "\tTicker=" + emsxTicker + "\tAmount=" + emsxAmount + "\tWorking=" + emsxWorking + "\tFilled=" + emsxFilled + "\tAveragePrice=" + emsxAvgPrice);

                        }
                        else if (msgSubType == "R")
                        {

                            int emsxSequence = msg.HasElement("EMSX_SEQUENCE") ? msg.GetElementAsInt32("EMSX_SEQUENCE") : 0;
                            string emsxStatus = msg.HasElement("EMSX_STATUS") ? msg.GetElementAsString("EMSX_STATUS") : "";
                            string emsxTicker = msg.HasElement("EMSX_TICKER") ? msg.GetElementAsString("EMSX_TICKER") : "";
                            int emsxAmount = msg.HasElement("EMSX_AMOUNT") ? msg.GetElementAsInt32("EMSX_AMOUNT") : 0;
                            int emsxWorking = msg.HasElement("EMSX_WORKING") ? msg.GetElementAsInt32("EMSX_WORKING") : 0;
                            int emsxFilled = msg.HasElement("EMSX_FILLED") ? msg.GetElementAsInt32("EMSX_FILLED") : 0;
                            double emsxAvgPrice = msg.HasElement("EMSX_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_AVG_PRICE") : 0;

                            log("Route event (" + eventStatus + "): Sequence=" + emsxSequence + "\tStatus=" + emsxStatus + "\tWorking=" + emsxWorking + "\tFilled=" + emsxFilled + "\tAveragePrice=" + emsxAvgPrice);
                        }
                    }
                }
                else
                {
                    logErr("Error: Unexpected message");
                }

            }
        }

        private void processResponseEvent(Event evt, Session session)
        {
            log("Processing " + evt.Type);

            foreach (Message msg in evt)
            {
                if (msg.CorrelationID == refdata_req_id)
                {
                    log("RefData RESPONSE received...");

                    Element securities = msg.GetElement(SECURITY_DATA);
                    Element security = securities.GetValueAsElement(0);
                    Element fields = security.GetElement(FIELD_DATA);
                    String ticker_desc = fields.GetElementAsString("PARSEKYABLE_DES_SOURCE");
                    
                    log("Ticker Description: " + ticker_desc);

                    // Open EMSX service...
                    session.OpenServiceAsync(service_emsx);

                }
                else
                {

                    CorrelationID matchCorID = msg.CorrelationID;

                    if (msg.MessageType.Equals(ERROR_INFO))
                    {
                        int errorCode = msg.GetElementAsInt32("ERROR_CODE");
                        string errorMessage = msg.GetElementAsString("ERROR_MESSAGE");

                        log("RESPONSE ERROR\nERROR CODE: " + errorCode + "\tERROR MESSAGE: " + errorMessage);
                    }
                    else if (msg.MessageType.Equals(CREATE_ORDER_AND_ROUTE_EX))
                    {

                        if(outstandingRequests.ContainsKey(matchCorID))
                        {
                            Request r;
                            outstandingRequests.TryRemove(matchCorID,out r);
                        }
                        else
                        {
                            log("Request ID not recognised(" + matchCorID + ")...ignoring");
                        }
                    }
                }
            }
        }

        private void processMiscEvent(Event evt, Session session)
        {
            log("Processing " + evt.Type);
            foreach (Message msg in evt)
            {
                log("MESSAGE: " + msg);
            }
        }

        private void sendRefDataRequest(Session session)
        {

            // Static data request to retrieve the ticker description (PARSEKYABLE_DES_SOURCE)
            refdata = session.GetService(service_refdata);

            request = refdata.CreateRequest("ReferenceDataRequest");
            Element securities = request.GetElement("securities");
            securities.AppendValue(ticker);
            Element fields = request.GetElement("fields");
            fields.AppendValue("PARSEKYABLE_DES_SOURCE");

            log("Sending Request: " + request);

            refdata_req_id = new CorrelationID(3);

            session.SendRequest(request, refdata_req_id);

            log("Request sent.");

        }

        private void createOrderSubscription(Session session)
        {

            // Create the topic string for the subscription. Here, we are subscribing 
            // to every available order field, however, you can subscribe to only the fields
            // required for your application.
            string orderTopic = service_emsx + "/order?fields=";

            orderTopic = orderTopic + "API_SEQ_NUM,";
            orderTopic = orderTopic + "EMSX_ACCOUNT,";
            orderTopic = orderTopic + "EMSX_AMOUNT,";
            orderTopic = orderTopic + "EMSX_ARRIVAL_PRICE,";
            orderTopic = orderTopic + "EMSX_ASSET_CLASS,";
            orderTopic = orderTopic + "EMSX_AVG_PRICE,";
            orderTopic = orderTopic + "EMSX_BROKER,";
            orderTopic = orderTopic + "EMSX_CFD_FLAG,";
            orderTopic = orderTopic + "EMSX_DATE,";
            orderTopic = orderTopic + "EMSX_DAY_AVG_PRICE,";
            orderTopic = orderTopic + "EMSX_DAY_FILL,";
            orderTopic = orderTopic + "EMSX_EXCHANGE,";
            orderTopic = orderTopic + "EMSX_EXEC_INSTRUCTION,";
            orderTopic = orderTopic + "EMSX_FILLED,";
            orderTopic = orderTopic + "EMSX_HAND_INSTRUCTION,";
            orderTopic = orderTopic + "EMSX_IDLE_AMOUNT,";
            orderTopic = orderTopic + "EMSX_LIMIT_PRICE,";
            orderTopic = orderTopic + "EMSX_ORD_REF_ID,";
            orderTopic = orderTopic + "EMSX_ORDER_TYPE,";
            orderTopic = orderTopic + "EMSX_PERCENT_REMAIN,";
            orderTopic = orderTopic + "EMSX_REMAIN_BALANCE,";
            orderTopic = orderTopic + "EMSX_SEC_NAME,";
            orderTopic = orderTopic + "EMSX_SEQUENCE,";
            orderTopic = orderTopic + "EMSX_SIDE,";
            orderTopic = orderTopic + "EMSX_START_AMOUNT,";
            orderTopic = orderTopic + "EMSX_STATUS,";
            orderTopic = orderTopic + "EMSX_TICKER,";
            orderTopic = orderTopic + "EMSX_TIF,";
            orderTopic = orderTopic + "EMSX_TIME_STAMP,";
            orderTopic = orderTopic + "EMSX_TYPE,";
            orderTopic = orderTopic + "EMSX_WORKING";

            // We define a correlation ID that allows us to identify the original
            // request when we examine the responses. This is useful in situations where
            // the same event handler is used the process messages for the Order and Route 
            // subscriptions, as well as request/response requests.
            order_sub_id = new CorrelationID(1);

            emsx_order_sub = new Subscription(orderTopic, order_sub_id);
            log("Order Topic: " + orderTopic);

            emsxOrderSubscription.Add(emsx_order_sub);

            try
            {
                session.Subscribe(emsxOrderSubscription);
                log("Order subscription sent");
            }
            catch (Exception ex)
            {
                logErr("Failed to create Order EMSX subscription: " + ex.Message);
            }

        }

        private void createRouteSubscription(Session session)
        {

            String routeTopic = service_emsx + "/route?fields=";

            routeTopic = routeTopic + "API_SEQ_NUM,";
            routeTopic = routeTopic + "EMSX_AMOUNT,";
            routeTopic = routeTopic + "EMSX_AVG_PRICE,";
            routeTopic = routeTopic + "EMSX_BROKER,";
            routeTopic = routeTopic + "EMSX_DAY_AVG_PRICE,";
            routeTopic = routeTopic + "EMSX_DAY_FILL,";
            routeTopic = routeTopic + "EMSX_EXEC_INSTRUCTION,";
            routeTopic = routeTopic + "EMSX_FILL_ID,";
            routeTopic = routeTopic + "EMSX_FILLED,";
            routeTopic = routeTopic + "EMSX_GTD_DATE,";
            routeTopic = routeTopic + "EMSX_HAND_INSTRUCTION,";
            routeTopic = routeTopic + "EMSX_IS_MANUAL_ROUTE,";
            routeTopic = routeTopic + "EMSX_LAST_FILL_DATE,";
            routeTopic = routeTopic + "EMSX_LAST_FILL_TIME,";
            routeTopic = routeTopic + "EMSX_LAST_MARKET,";
            routeTopic = routeTopic + "EMSX_LAST_PRICE,";
            routeTopic = routeTopic + "EMSX_LAST_SHARES,";
            routeTopic = routeTopic + "EMSX_LIMIT_PRICE,";
            routeTopic = routeTopic + "EMSX_NOTES,";
            routeTopic = routeTopic + "EMSX_ORDER_TYPE,";
            routeTopic = routeTopic + "EMSX_PERCENT_REMAIN,";
            routeTopic = routeTopic + "EMSX_REMAIN_BALANCE,";
            routeTopic = routeTopic + "EMSX_ROUTE_CREATE_DATE,";
            routeTopic = routeTopic + "EMSX_ROUTE_CREATE_TIME,";
            routeTopic = routeTopic + "EMSX_ROUTE_ID,";
            routeTopic = routeTopic + "EMSX_ROUTE_LAST_UPDATE_TIME,";
            routeTopic = routeTopic + "EMSX_ROUTE_PRICE,";
            routeTopic = routeTopic + "EMSX_SEQUENCE,";
            routeTopic = routeTopic + "EMSX_STATUS,";
            routeTopic = routeTopic + "EMSX_TIF,";
            routeTopic = routeTopic + "EMSX_TIME_STAMP,";
            routeTopic = routeTopic + "EMSX_TYPE,";
            routeTopic = routeTopic + "EMSX_WORKING";

            route_sub_id = new CorrelationID(2);

            emsx_route_sub = new Subscription(routeTopic, route_sub_id);
            log("Route Topic: " + routeTopic);
            emsxRouteSubscription.Add(emsx_route_sub);

            try
            {
                session.Subscribe(emsxRouteSubscription);
            }
            catch (Exception ex)
            {
                logErr("Failed to create Route EMSX subscription: " + ex.Message);
            }
        }

        private void createMarketDataSubscription(Session session)
        {
            mktdata_sub_id = new CorrelationID(4);
            mktdata_sub = new Subscription(ticker, "BID, ASK", mktdata_sub_id);
            log("Market Data Topic: " + mktdata_sub);
            mktdata_subscriptions.Add(mktdata_sub);

            try
            {
                session.Subscribe(mktdata_subscriptions);
            }
            catch (Exception ex)
            {
                logErr("Failed to create Market Data subscription: " + ex.Message);
            }

        }

        private void sendTradeRequest(Session session, String side, double limitPrice)
        {
            Request request = emapisvc.CreateRequest("CreateOrderAndRouteEx");

            request.Set("EMSX_TICKER", ticker);
            request.Set("EMSX_AMOUNT", amount);
            request.Set("EMSX_ORDER_TYPE", "LMT");
            request.Set("EMSX_BROKER", "BB");
            request.Set("EMSX_TIF", "DAY");
            request.Set("EMSX_HAND_INSTRUCTION", "ANY");
            request.Set("EMSX_SIDE", side);
            request.Set("EMSX_LIMIT_PRICE", limitPrice);

            pendingTrades.Enqueue(request);

        }

        public class Dispatcher
        {

            private ConcurrentQueue<Request> _pendingTrades;
            private ConcurrentDictionary<CorrelationID, Request> _outstandingRequests;
            private Session _session;

            public Dispatcher(Session session, ConcurrentQueue<Request> pendingTrades, ConcurrentDictionary<CorrelationID, Request> outstandingRequests)
            {
                this._session = session;
                this._pendingTrades = pendingTrades;
                this._outstandingRequests = outstandingRequests;
            }

            public void dispatch()
            {
                while (true)
                {

                    Request nextReq;
                    if(_pendingTrades.TryDequeue(out nextReq))
                    {
                        CorrelationID newCorrID = new CorrelationID();
                        _outstandingRequests.TryAdd(newCorrID, nextReq);

                        try
                        {
                            log("Sending " + nextReq.GetElement("EMSX_SIDE") + " request at " + nextReq.GetElement("EMSX_LIMIT_PRICE"));
                            _session.SendRequest(nextReq, newCorrID);
                        }
                        catch (Exception ex)
                        {
                            _outstandingRequests.TryRemove(newCorrID, out nextReq);
                            logErr("Failed to send EMSX trade request: " + ex.Message);
                        }
                    }
                }
            }
        }

        private bool parseCommandLine(string[] args)
        {
            bool valid = true;

            if (args.Length < 4)
            {
                log("Error: Missing required parameters\n");
                printUsage();
                return false;
            }
            else
            {
                for (int i = 0; i < args.Length; i++)
                {
                    if (isArg(args[i], "SELLAT"))
                    {
                        sellat = getArgValue(args[i]);
                        if (!Array.Exists(pricePoints, element => element == sellat))
                        {
                            valid = false;
                            log("Error: Invalid SELLAT parameter value.");
                        }
                    }
                    else if (isArg(args[i], "BUYAT"))
                    {
                        buyat = getArgValue(args[i]);
                        if (!Array.Exists(pricePoints, element => element == buyat))
                        {
                            valid = false;
                            log("Error: Invalid BUYAT parameter value.");
                        }
                    }
                    else if (isArg(args[i], "AMOUNT")) amount = System.Convert.ToInt32(getArgValue(args[i]));
                    else if (isArg(args[i], "TICKER")) ticker = getArgValue(args[i]);
                    else
                    {
                        valid = false;
                        log("Warning>> Unknown parameter:" + args[i]);
                    }

                }

            }
            return valid;
        }

        private void showParameters()
        {
            log("Parameter List:-");
            log("SELLAT: " + sellat);
            log("BUYAT: " + buyat);
            log("AMOUNT: " + amount);
            log("TICKER: " + ticker);

        }

        private bool isArg(string arg, string find)
        {
            if (arg.IndexOf(find) >= 0) return true;
            else return false;
        }

        private String getArgValue(string arg)
        {
            return (arg.Substring(arg.IndexOf("=") + 1));
        }

        private void printUsage()
        {
            log("Usage:");
            log("EMSXCreateOrderAsyncRequest SELLAT=[BID,ASK] BUYAT=[BID,ASK] AMOUNT=<value> TICKER=\"<value>\"");
        }

        public static void log(String message)
        {
            lock(logLock)
            {
                System.Console.WriteLine(message);
            }
        }
        public static void logErr(String message)
        {
            lock (logLock)
            {
                System.Console.Error.WriteLine(message);
            }
        }
    }
}
