/* Copyright 2013. Bloomberg Finance L.P.
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
using System;

namespace com.bloomberg.samples
{
    public class EMSXTrigger
    {

        /* Run with:-
         *     EMSXTrigger 
	     *         SELLAT=163.4 BUYAT=154.7 AMOUNT=1000 TICKER="IBM US Equity" 
	     */

        private static readonly Name ORDER_ROUTE_FIELDS = new Name("OrderRouteFields");
        private static readonly Name ERROR_INFO = new Name("ErrorInfo");
        private static readonly Name CREATE_ORDER_AND_ROUTE = new Name("CreateOrderAndRoute");


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
        private List<Subscription> emsx_subscriptions = new List<Subscription>();
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

        private double sellat = 0;
        private double buyat = 0;
        private int amount = 0;
        private string ticker = "";

        private int emsx_req_id = 100;

        public class IDSequenceMap {
            public CorrelationID requestID;
            public int sequenceNo;
            public int routeID;
        }
        
        private List<IDSequenceMap> idMapping = new List<IDSequenceMap>();

        public static void Main(string[] args)
        {
            System.Console.WriteLine("Bloomberg - EMSX API Example - EMSXSubscriptionsOrdersAsync");
            System.Console.WriteLine("Press ENTER at anytime to quit");

            EMSXTrigger example = new EMSXTrigger();
            example.run(args);

            System.Console.Read();
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

            SessionOptions d_sessionOptions = new SessionOptions();
            d_sessionOptions.ServerHost = d_host;
            d_sessionOptions.ServerPort = d_port;

            Session session = new Session(d_sessionOptions, new EventHandler(processEvent));
            session.Stop();
            session.StartAsync();
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
                System.Console.Error.WriteLine("Error: " + e.ToString());
            }
        }

        private void processAdminEvent(Event evt, Session session)
        {
            System.Console.WriteLine("Processing " + evt.Type);

            foreach (Message msg in evt)
            {
                if (msg.MessageType.Equals(SLOW_CONSUMER_WARNING))
                {
                    System.Console.Error.WriteLine("Warning: Entered Slow Consumer status");
                }
                else if (msg.MessageType.Equals(SLOW_CONSUMER_WARNING_CLEARED))
                {
                    System.Console.WriteLine("Slow consumer status cleaered");
                }
            }
        }

        private void processSessionEvent(Event evt, Session session)
        {
            System.Console.WriteLine("Processing " + evt.Type);
            foreach (Message msg in evt)
            {
                if (msg.MessageType.Equals(SESSION_STARTED))
                {
                    System.Console.WriteLine("Session started...");
                    session.OpenServiceAsync(service_refdata);
                    session.OpenServiceAsync(service_mktdata);
                    session.OpenServiceAsync(service_emsx);
                }
                else if (msg.MessageType.Equals(SESSION_STARTUP_FAILURE))
                {
                    System.Console.Error.WriteLine("Error: Session startup failed");
                }
                else if (msg.MessageType.Equals(SESSION_TERMINATED))
                {
                    System.Console.Error.WriteLine("Error: Session has been terminated");
                }
                else if (msg.MessageType.Equals(SESSION_CONNECTION_UP))
                {
                    System.Console.WriteLine("Session connection is up");
                }
                else if (msg.MessageType.Equals(SESSION_CONNECTION_DOWN))
                {
                    System.Console.Error.WriteLine("Error: Session connection is down");
                }
            }
        }

        private void processServiceEvent(Event evt, Session session)
        {
            System.Console.WriteLine("Processing " + evt.Type);
            foreach (Message msg in evt)
            {
                // Identify which service this message belongs to...
                String svc = msg.GetElementAsString("serviceName");

                if (msg.MessageType.Equals(SERVICE_OPENED))
                {
                    System.Console.WriteLine("Service opened [" + svc + "]...");

                    if (svc == service_refdata)
                    {
                        // Static data request to retrieve the ticker description (PARSEKYABLE_DES_SOURCE)
                        refdata = session.GetService(service_refdata);

                        request = refdata.CreateRequest("ReferenceDataRequest");
                        Element securities = request.GetElement("securities");
                        securities.AppendValue(ticker);
                        Element fields = request.GetElement("fields");
                        fields.AppendValue("PARSEKYABLE_DES_SOURCE");
                        
                        System.Console.WriteLine("Sending Request: " + request);

                        refdata_req_id = new CorrelationID(3);

                        session.SendRequest(request, refdata_req_id);
                    }
                    else if (svc == service_mktdata)
                    {
                        // Create subscription for LAST_PRICE

                        mktdata_sub_id = new CorrelationID(4);

                        mktdata_sub = new Subscription(ticker, "LAST_PRICE", mktdata_sub_id);
                        System.Console.WriteLine("Market Data Topic: " + mktdata_sub);
                        mktdata_subscriptions.Add(mktdata_sub);

                        try
                        {
                            session.Subscribe(mktdata_subscriptions);
                        }
                        catch (Exception ex)
                        {
                            System.Console.Error.WriteLine("Failed to create Market Data subscription: " + ex.Message);
                        }

                    }
                    else if (svc == service_emsx)
                    {

                        emapisvc = session.GetService(service_emsx);

                        // Create the topic string for the subscription. Here, we are subscribing 
                        // to every available order field, however, you can subscribe to only the fields
                        // required for your application.
                        string orderTopic = service_emsx + "/order?fields=";

                        orderTopic = orderTopic + "API_SEQ_NUM,";
                        orderTopic = orderTopic + "EMSX_ACCOUNT,";
                        orderTopic = orderTopic + "EMSX_AMOUNT,";
                        orderTopic = orderTopic + "EMSX_ARRIVAL_PRICE,";
                        orderTopic = orderTopic + "EMSX_ASSET_CLASS,";
                        orderTopic = orderTopic + "EMSX_ASSIGNED_TRADER,";
                        orderTopic = orderTopic + "EMSX_AVG_PRICE,";
                        orderTopic = orderTopic + "EMSX_BASKET_NAME,";
                        orderTopic = orderTopic + "EMSX_BASKET_NUM,";
                        orderTopic = orderTopic + "EMSX_BROKER,";
                        orderTopic = orderTopic + "EMSX_BROKER_COMM,";
                        orderTopic = orderTopic + "EMSX_BSE_AVG_PRICE,";
                        orderTopic = orderTopic + "EMSX_BSE_FILLED,";
                        orderTopic = orderTopic + "EMSX_CFD_FLAG,";
                        orderTopic = orderTopic + "EMSX_COMM_DIFF_FLAG,";
                        orderTopic = orderTopic + "EMSX_COMM_RATE,";
                        orderTopic = orderTopic + "EMSX_CURRENCY_PAIR,";
                        orderTopic = orderTopic + "EMSX_DATE,";
                        orderTopic = orderTopic + "EMSX_DAY_AVG_PRICE,";
                        orderTopic = orderTopic + "EMSX_DAY_FILL,";
                        orderTopic = orderTopic + "EMSX_DIR_BROKER_FLAG,";
                        orderTopic = orderTopic + "EMSX_EXCHANGE,";
                        orderTopic = orderTopic + "EMSX_EXCHANGE_DESTINATION,";
                        orderTopic = orderTopic + "EMSX_EXEC_INSTRUCTION,";
                        orderTopic = orderTopic + "EMSX_FILL_ID,";
                        orderTopic = orderTopic + "EMSX_FILLED,";
                        orderTopic = orderTopic + "EMSX_GTD_DATE,";
                        orderTopic = orderTopic + "EMSX_HAND_INSTRUCTION,";
                        orderTopic = orderTopic + "EMSX_IDLE_AMOUNT,";
                        orderTopic = orderTopic + "EMSX_INVESTOR_ID,";
                        orderTopic = orderTopic + "EMSX_ISIN,";
                        orderTopic = orderTopic + "EMSX_LIMIT_PRICE,";
                        orderTopic = orderTopic + "EMSX_NOTES,";
                        orderTopic = orderTopic + "EMSX_NSE_AVG_PRICE,";
                        orderTopic = orderTopic + "EMSX_NSE_FILLED,";
                        orderTopic = orderTopic + "EMSX_ORD_REF_ID,";
                        orderTopic = orderTopic + "EMSX_ORDER_TYPE,";
                        orderTopic = orderTopic + "EMSX_ORIGINATE_TRADER,";
                        orderTopic = orderTopic + "EMSX_ORIGINATE_TRADER_FIRM,";
                        orderTopic = orderTopic + "EMSX_PERCENT_REMAIN,";
                        orderTopic = orderTopic + "EMSX_PM_UUID,";
                        orderTopic = orderTopic + "EMSX_PORT_MGR,";
                        orderTopic = orderTopic + "EMSX_PORT_NAME,";
                        orderTopic = orderTopic + "EMSX_PORT_NUM,";
                        orderTopic = orderTopic + "EMSX_POSITION,";
                        orderTopic = orderTopic + "EMSX_PRINCIPAL,";
                        orderTopic = orderTopic + "EMSX_PRODUCT,";
                        orderTopic = orderTopic + "EMSX_QUEUED_DATE,";
                        orderTopic = orderTopic + "EMSX_QUEUED_TIME,";
                        orderTopic = orderTopic + "EMSX_REASON_CODE,";
                        orderTopic = orderTopic + "EMSX_REASON_DESC,";
                        orderTopic = orderTopic + "EMSX_REMAIN_BALANCE,";
                        orderTopic = orderTopic + "EMSX_ROUTE_ID,";
                        orderTopic = orderTopic + "EMSX_ROUTE_PRICE,";
                        orderTopic = orderTopic + "EMSX_SEC_NAME,";
                        orderTopic = orderTopic + "EMSX_SEDOL,";
                        orderTopic = orderTopic + "EMSX_SEQUENCE,";
                        orderTopic = orderTopic + "EMSX_SETTLE_AMOUNT,";
                        orderTopic = orderTopic + "EMSX_SETTLE_DATE,";
                        orderTopic = orderTopic + "EMSX_SIDE,";
                        orderTopic = orderTopic + "EMSX_START_AMOUNT,";
                        orderTopic = orderTopic + "EMSX_STATUS,";
                        orderTopic = orderTopic + "EMSX_STEP_OUT_BROKER,";
                        orderTopic = orderTopic + "EMSX_STOP_PRICE,";
                        orderTopic = orderTopic + "EMSX_STRATEGY_END_TIME,";
                        orderTopic = orderTopic + "EMSX_STRATEGY_PART_RATE1,";
                        orderTopic = orderTopic + "EMSX_STRATEGY_PART_RATE2,";
                        orderTopic = orderTopic + "EMSX_STRATEGY_START_TIME,";
                        orderTopic = orderTopic + "EMSX_STRATEGY_STYLE,";
                        orderTopic = orderTopic + "EMSX_STRATEGY_TYPE,";
                        orderTopic = orderTopic + "EMSX_TICKER,";
                        orderTopic = orderTopic + "EMSX_TIF,";
                        orderTopic = orderTopic + "EMSX_TIME_STAMP,";
                        orderTopic = orderTopic + "EMSX_TRAD_UUID,";
                        orderTopic = orderTopic + "EMSX_TRADE_DESK,";
                        orderTopic = orderTopic + "EMSX_TRADER,";
                        orderTopic = orderTopic + "EMSX_TRADER_NOTES,";
                        orderTopic = orderTopic + "EMSX_TS_ORDNUM,";
                        orderTopic = orderTopic + "EMSX_TYPE,";
                        orderTopic = orderTopic + "EMSX_UNDERLYING_TICKER,";
                        orderTopic = orderTopic + "EMSX_USER_COMM_AMOUNT,";
                        orderTopic = orderTopic + "EMSX_USER_COMM_RATE,";
                        orderTopic = orderTopic + "EMSX_USER_FEES,";
                        orderTopic = orderTopic + "EMSX_USER_NET_MONEY,";
                        orderTopic = orderTopic + "EMSX_WORK_PRICE,";
                        orderTopic = orderTopic + "EMSX_WORKING,";
                        orderTopic = orderTopic + "EMSX_YELLOW_KEY";


                        // We define a correlation ID that allows us to identify the original
                        // request when we examine the responses. This is useful in situations where
                        // the same event handler is used the process messages for the Order and Route 
                        // subscriptions, as well as request/response requests.
                        order_sub_id = new CorrelationID(1);

                        emsx_order_sub= new Subscription(orderTopic, order_sub_id);
                        System.Console.WriteLine("Order Topic: " + orderTopic);
                        emsx_subscriptions.Add(emsx_order_sub);

                        String routeTopic = service_emsx + "/route?fields=";

                        routeTopic = routeTopic + "API_SEQ_NUM,";
                        routeTopic = routeTopic + "EMSX_AMOUNT,";
                        routeTopic = routeTopic + "EMSX_AVG_PRICE,";
                        routeTopic = routeTopic + "EMSX_BROKER,";
                        routeTopic = routeTopic + "EMSX_BROKER_COMM,";
                        routeTopic = routeTopic + "EMSX_BSE_AVG_PRICE,";
                        routeTopic = routeTopic + "EMSX_BSE_FILLED,";
                        routeTopic = routeTopic + "EMSX_CLEARING_ACCOUNT,";
                        routeTopic = routeTopic + "EMSX_CLEARING_FIRM,";
                        routeTopic = routeTopic + "EMSX_COMM_DIFF_FLAG,";
                        routeTopic = routeTopic + "EMSX_COMM_RATE,";
                        routeTopic = routeTopic + "EMSX_CURRENCY_PAIR,";
                        routeTopic = routeTopic + "EMSX_CUSTOM_ACCOUNT,";
                        routeTopic = routeTopic + "EMSX_DAY_AVG_PRICE,";
                        routeTopic = routeTopic + "EMSX_DAY_FILL,";
                        routeTopic = routeTopic + "EMSX_EXCHANGE_DESTINATION,";
                        routeTopic = routeTopic + "EMSX_EXEC_INSTRUCTION,";
                        routeTopic = routeTopic + "EMSX_EXECUTE_BROKER,";
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
                        routeTopic = routeTopic + "EMSX_MISC_FEES,";
                        routeTopic = routeTopic + "EMSX_ML_LEG_QUANTITY,";
                        routeTopic = routeTopic + "EMSX_ML_NUM_LEGS,";
                        routeTopic = routeTopic + "EMSX_ML_PERCENT_FILLED,";
                        routeTopic = routeTopic + "EMSX_ML_RATIO,";
                        routeTopic = routeTopic + "EMSX_ML_REMAIN_BALANCE,";
                        routeTopic = routeTopic + "EMSX_ML_STRATEGY,";
                        routeTopic = routeTopic + "EMSX_ML_TOTAL_QUANTITY,";
                        routeTopic = routeTopic + "EMSX_NOTES,";
                        routeTopic = routeTopic + "EMSX_NSE_AVG_PRICE,";
                        routeTopic = routeTopic + "EMSX_NSE_FILLED,";
                        routeTopic = routeTopic + "EMSX_ORDER_TYPE,";
                        routeTopic = routeTopic + "EMSX_P_A,";
                        routeTopic = routeTopic + "EMSX_PERCENT_REMAIN,";
                        routeTopic = routeTopic + "EMSX_PRINCIPAL,";
                        routeTopic = routeTopic + "EMSX_QUEUED_DATE,";
                        routeTopic = routeTopic + "EMSX_QUEUED_TIME,";
                        routeTopic = routeTopic + "EMSX_REASON_CODE,";
                        routeTopic = routeTopic + "EMSX_REASON_DESC,";
                        routeTopic = routeTopic + "EMSX_REMAIN_BALANCE,";
                        routeTopic = routeTopic + "EMSX_ROUTE_CREATE_DATE,";
                        routeTopic = routeTopic + "EMSX_ROUTE_CREATE_TIME,";
                        routeTopic = routeTopic + "EMSX_ROUTE_ID,";
                        routeTopic = routeTopic + "EMSX_ROUTE_LAST_UPDATE_TIME,";
                        routeTopic = routeTopic + "EMSX_ROUTE_PRICE,";
                        routeTopic = routeTopic + "EMSX_SEQUENCE,";
                        routeTopic = routeTopic + "EMSX_SETTLE_AMOUNT,";
                        routeTopic = routeTopic + "EMSX_SETTLE_DATE,";
                        routeTopic = routeTopic + "EMSX_STATUS,";
                        routeTopic = routeTopic + "EMSX_STOP_PRICE,";
                        routeTopic = routeTopic + "EMSX_STRATEGY_END_TIME,";
                        routeTopic = routeTopic + "EMSX_STRATEGY_PART_RATE1,";
                        routeTopic = routeTopic + "EMSX_STRATEGY_PART_RATE2,";
                        routeTopic = routeTopic + "EMSX_STRATEGY_START_TIME,";
                        routeTopic = routeTopic + "EMSX_STRATEGY_STYLE,";
                        routeTopic = routeTopic + "EMSX_STRATEGY_TYPE,";
                        routeTopic = routeTopic + "EMSX_TIF,";
                        routeTopic = routeTopic + "EMSX_TIME_STAMP,";
                        routeTopic = routeTopic + "EMSX_TYPE,";
                        routeTopic = routeTopic + "EMSX_URGENCY_LEVEL,";
                        routeTopic = routeTopic + "EMSX_USER_COMM_AMOUNT,";
                        routeTopic = routeTopic + "EMSX_USER_COMM_RATE,";
                        routeTopic = routeTopic + "EMSX_USER_FEES,";
                        routeTopic = routeTopic + "EMSX_USER_NET_MONEY,";
                        routeTopic = routeTopic + "EMSX_WORKING";

                        route_sub_id = new CorrelationID(2);

                        emsx_route_sub = new Subscription(routeTopic, route_sub_id);
                        System.Console.WriteLine("Route Topic: " + routeTopic);
                        emsx_subscriptions.Add(emsx_route_sub);

                        try
                        {
                            session.Subscribe(emsx_subscriptions);
                        }
                        catch (Exception ex)
                        {
                            System.Console.Error.WriteLine("Failed to create EMSX subscription: " + ex.Message);
                        }
                    }
                }
                else if (msg.MessageType.Equals(SERVICE_OPEN_FAILURE))
                {
                    System.Console.Error.WriteLine("Error: Service failed to open [" + svc + "]");
                }
            }
        }

        private void processSubscriptionStatusEvent(Event evt, Session session)
        {
            System.Console.WriteLine("Processing " + evt.Type);
            foreach (Message msg in evt)
            {
                if (msg.MessageType.Equals(SUBSCRIPTION_STARTED))
                {
                    if (msg.CorrelationID == mktdata_sub_id)
                    {
                        System.Console.WriteLine("Subscription started [" + service_mktdata + "]");
                    }
                    else if (msg.CorrelationID == order_sub_id)
                    {
                        System.Console.WriteLine("Subscription started [" + service_emsx + " -  Orders]");
                    }
                    else if (msg.CorrelationID == route_sub_id)
                    {
                        System.Console.WriteLine("Subscription started [" + service_emsx + " - Routes]");
                    }

                }
                else if (msg.MessageType.Equals(SUBSCRIPTION_FAILURE))
                {
                    System.Console.Error.WriteLine("Error: Subscription failed");
                    System.Console.WriteLine("MESSAGE: " + msg);
                }
                else if (msg.MessageType.Equals(SUBSCRIPTION_TERMINATED))
                {
                    System.Console.Error.WriteLine("Error: Subscription terminated");
                    System.Console.WriteLine("MESSAGE: " + msg);
                }
            }
        }

        private void processSubscriptionDataEvent(Event evt, Session session)
        {
            System.Console.WriteLine("Processing " + evt.Type);
            foreach (Message msg in evt)
            {
                if (msg.CorrelationID == mktdata_sub_id)
                {
                    System.Console.WriteLine("Subscription data for MktData");

                    //System.Console.WriteLine(msg);
                    double last_price = msg.HasElement("LAST_PRICE") ? msg.GetElementAsFloat64("LAST_PRICE") : 0;
                    if (last_price > 0)
                    {
                        System.Console.WriteLine("MktData: LAST_PRICE = " + last_price.ToString("0.0000"));

                        if (last_price < buyat || last_price > sellat)
                        {
                            Request request = emapisvc.CreateRequest("CreateOrderAndRoute");

                            request.Set("EMSX_TICKER", ticker);
                            request.Set("EMSX_AMOUNT", amount);
                            request.Set("EMSX_ORDER_TYPE", "MKT");
                            request.Set("EMSX_BROKER", "BB");
                            request.Set("EMSX_TIF", "DAY");
                            request.Set("EMSX_HAND_INSTRUCTION", "ANY");

                            if(last_price < buyat) request.Set("EMSX_SIDE", "BUY");
                            else request.Set("EMSX_SIDE", "SELL");

                            CorrelationID requestID = new CorrelationID(emsx_req_id++);

                            IDSequenceMap map = new IDSequenceMap();
                            map.requestID = requestID;

                            idMapping.Add(map);

                            session.SendRequest(request, requestID);

                        }
                    }
                }
                else if (msg.CorrelationID == order_sub_id || msg.CorrelationID == route_sub_id)
                {
                    System.Console.WriteLine("Subscription data for EMSX Orders");

                    int event_status = msg.GetElementAsInt32("EVENT_STATUS");

                    if (event_status == 1)
                    {
                        System.Console.WriteLine("EMSX Subscription Heartbeat...");
                    }
                    else
                    {
                        string msg_sub_type = msg.HasElement("MSG_SUB_TYPE") ? msg.GetElementAsString("MSG_SUB_TYPE") : "";

                        if (msg_sub_type == "O")
                        {
                            long api_seq_num = msg.HasElement("API_SEQ_NUM") ? msg.GetElementAsInt64("API_SEQ_NUM") : 0;
                            string emsx_account = msg.HasElement("EMSX_ACCOUNT") ? msg.GetElementAsString("EMSX_ACCOUNT") : "";
                            int emsx_amount = msg.HasElement("EMSX_AMOUNT") ? msg.GetElementAsInt32("EMSX_AMOUNT") : 0;
                            double emsx_arrival_price = msg.HasElement("EMSX_ARRIVAL_PRICE") ? msg.GetElementAsFloat64("EMSX_ARRIVAL_PRICE") : 0;
                            string emsx_asset_class = msg.HasElement("EMSX_ASSET_CLASS") ? msg.GetElementAsString("EMSX_ASSET_CLASS") : "";
                            string emsx_assigned_trader = msg.HasElement("EMSX_ASSIGNED_TRADER") ? msg.GetElementAsString("EMSX_ASSIGNED_TRADER") : "";
                            double emsx_avg_price = msg.HasElement("EMSX_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_AVG_PRICE") : 0;
                            string emsx_basket_name = msg.HasElement("EMSX_BASKET_NAME") ? msg.GetElementAsString("EMSX_BASKET_NAME") : "";
                            int emsx_basket_num = msg.HasElement("EMSX_BASKET_NUM") ? msg.GetElementAsInt32("EMSX_BASKET_NUM") : 0;
                            string emsx_broker = msg.HasElement("EMSX_BROKER") ? msg.GetElementAsString("EMSX_BROKER") : "";
                            double emsx_broker_comm = msg.HasElement("EMSX_BROKER_COMM") ? msg.GetElementAsFloat64("EMSX_BROKER_COMM") : 0;
                            double emsx_bse_avg_price = msg.HasElement("EMSX_BSE_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_BSE_AVG_PRICE") : 0;
                            int emsx_bse_filled = msg.HasElement("EMSX_BSE_FILLED") ? msg.GetElementAsInt32("EMSX_BSE_FILLED") : 0;
                            string emsx_cfd_flag = msg.HasElement("EMSX_CFD_FLAG") ? msg.GetElementAsString("EMSX_CFD_FLAG") : "";
                            string emsx_comm_diff_flag = msg.HasElement("EMSX_COMM_DIFF_FLAG") ? msg.GetElementAsString("EMSX_COMM_DIFF_FLAG") : "";
                            double emsx_comm_rate = msg.HasElement("EMSX_COMM_RATE") ? msg.GetElementAsFloat64("EMSX_COMM_RATE") : 0;
                            string emsx_currency_pair = msg.HasElement("EMSX_CURRENCY_PAIR") ? msg.GetElementAsString("EMSX_CURRENCY_PAIR") : "";
                            int emsx_date = msg.HasElement("EMSX_DATE") ? msg.GetElementAsInt32("EMSX_DATE") : 0;
                            double emsx_day_avg_price = msg.HasElement("EMSX_DAY_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_DAY_AVG_PRICE") : 0;
                            int emsx_day_fill = msg.HasElement("EMSX_DAY_FILL") ? msg.GetElementAsInt32("EMSX_DAY_FILL") : 0;
                            string emsx_dir_broker_flag = msg.HasElement("EMSX_DIR_BROKER_FLAG") ? msg.GetElementAsString("EMSX_DIR_BROKER_FLAG") : "";
                            string emsx_exchange = msg.HasElement("EMSX_EXCHANGE") ? msg.GetElementAsString("EMSX_EXCHANGE") : "";
                            string emsx_exchange_destination = msg.HasElement("EMSX_EXCHANGE_DESTINATION") ? msg.GetElementAsString("EMSX_EXCHANGE_DESTINATION") : "";
                            string emsx_exec_instruction = msg.HasElement("EMSX_EXEC_INSTRUCTION") ? msg.GetElementAsString("EMSX_EXEC_INSTRUCTION") : "";
                            int emsx_fill_id = msg.HasElement("EMSX_FILL_ID") ? msg.GetElementAsInt32("EMSX_FILL_ID") : 0;
                            int emsx_filled = msg.HasElement("EMSX_FILLED") ? msg.GetElementAsInt32("EMSX_FILLED") : 0;
                            int emsx_gtd_date = msg.HasElement("EMSX_GTD_DATE") ? msg.GetElementAsInt32("EMSX_GTD_DATE") : 0;
                            string emsx_hand_instruction = msg.HasElement("EMSX_HAND_INSTRUCTION") ? msg.GetElementAsString("EMSX_HAND_INSTRUCTION") : "";
                            int emsx_idle_amount = msg.HasElement("EMSX_IDLE_AMOUNT") ? msg.GetElementAsInt32("EMSX_IDLE_AMOUNT") : 0;
                            string emsx_investor_id = msg.HasElement("EMSX_INVESTOR_ID") ? msg.GetElementAsString("EMSX_INVESTOR_ID") : "";
                            string emsx_isin = msg.HasElement("EMSX_ISIN") ? msg.GetElementAsString("EMSX_ISIN") : "";
                            double emsx_limit_price = msg.HasElement("EMSX_LIMIT_PRICE") ? msg.GetElementAsFloat64("EMSX_LIMIT_PRICE") : 0;
                            string emsx_notes = msg.HasElement("EMSX_NOTES") ? msg.GetElementAsString("EMSX_NOTES") : "";
                            double emsx_nse_avg_price = msg.HasElement("EMSX_NSE_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_NSE_AVG_PRICE") : 0;
                            int emsx_nse_filled = msg.HasElement("EMSX_NSE_FILLED") ? msg.GetElementAsInt32("EMSX_NSE_FILLED") : 0;
                            string emsx_ord_ref_id = msg.HasElement("EMSX_ORD_REF_ID") ? msg.GetElementAsString("EMSX_ORD_REF_ID") : "";
                            string emsx_order_type = msg.HasElement("EMSX_ORDER_TYPE") ? msg.GetElementAsString("EMSX_ORDER_TYPE") : "";
                            string emsx_originate_trader = msg.HasElement("EMSX_ORIGINATE_TRADER") ? msg.GetElementAsString("EMSX_ORIGINATE_TRADER") : "";
                            string emsx_originate_trader_firm = msg.HasElement("EMSX_ORIGINATE_TRADER_FIRM") ? msg.GetElementAsString("EMSX_ORIGINATE_TRADER_FIRM") : "";
                            double emsx_percent_remain = msg.HasElement("EMSX_PERCENT_REMAIN") ? msg.GetElementAsFloat64("EMSX_PERCENT_REMAIN") : 0;
                            int emsx_pm_uuid = msg.HasElement("EMSX_PM_UUID") ? msg.GetElementAsInt32("EMSX_PM_UUID") : 0;
                            string emsx_port_mgr = msg.HasElement("EMSX_PORT_MGR") ? msg.GetElementAsString("EMSX_PORT_MGR") : "";
                            string emsx_port_name = msg.HasElement("EMSX_PORT_NAME") ? msg.GetElementAsString("EMSX_PORT_NAME") : "";
                            int emsx_port_num = msg.HasElement("EMSX_PORT_NUM") ? msg.GetElementAsInt32("EMSX_PORT_NUM") : 0;
                            string emsx_position = msg.HasElement("EMSX_POSITION") ? msg.GetElementAsString("EMSX_POSITION") : "";
                            double emsx_principle = msg.HasElement("EMSX_PRINCIPAL") ? msg.GetElementAsFloat64("EMSX_PRINCIPAL") : 0;
                            string emsx_product = msg.HasElement("EMSX_PRODUCT") ? msg.GetElementAsString("EMSX_PRODUCT") : "";
                            int emsx_queued_date = msg.HasElement("EMSX_QUEUED_DATE") ? msg.GetElementAsInt32("EMSX_QUEUED_DATE") : 0;
                            int emsx_queued_time = msg.HasElement("EMSX_QUEUED_TIME") ? msg.GetElementAsInt32("EMSX_QUEUED_TIME") : 0;
                            string emsx_reason_code = msg.HasElement("EMSX_REASON_CODE") ? msg.GetElementAsString("EMSX_REASON_CODE") : "";
                            string emsx_reason_desc = msg.HasElement("EMSX_REASON_DESC") ? msg.GetElementAsString("EMSX_REASON_DESC") : "";
                            double emsx_remain_balance = msg.HasElement("EMSX_REMAIN_BALANCE") ? msg.GetElementAsFloat64("EMSX_REMAIN_BALANCE") : 0;
                            int emsx_route_id = msg.HasElement("EMSX_ROUTE_ID") ? msg.GetElementAsInt32("EMSX_ROUTE_ID") : 0;
                            double emsx_route_price = msg.HasElement("EMSX_ROUTE_PRICE") ? msg.GetElementAsFloat64("EMSX_ROUTE_PRICE") : 0;
                            string emsx_sec_name = msg.HasElement("EMSX_SEC_NAME") ? msg.GetElementAsString("EMSX_SEC_NAME") : "";
                            string emsx_sedol = msg.HasElement("EMSX_SEDOL") ? msg.GetElementAsString("EMSX_SEDOL") : "";
                            int emsx_sequence = msg.HasElement("EMSX_SEQUENCE") ? msg.GetElementAsInt32("EMSX_SEQUENCE") : 0;
                            double emsx_settle_amount = msg.HasElement("EMSX_SETTLE_AMOUNT") ? msg.GetElementAsFloat64("EMSX_SETTLE_AMOUNT") : 0;
                            int emsx_settle_date = msg.HasElement("EMSX_SETTLE_DATE") ? msg.GetElementAsInt32("EMSX_SETTLE_DATE") : 0;
                            string emsx_side = msg.HasElement("EMSX_SIDE") ? msg.GetElementAsString("EMSX_SIDE") : "";
                            int emsx_start_amount = msg.HasElement("EMSX_START_AMOUNT") ? msg.GetElementAsInt32("EMSX_START_AMOUNT") : 0;
                            string emsx_status = msg.HasElement("EMSX_STATUS") ? msg.GetElementAsString("EMSX_STATUS") : "";
                            string emsx_step_out_broker = msg.HasElement("EMSX_STEP_OUT_BROKER") ? msg.GetElementAsString("EMSX_STEP_OUT_BROKER") : "";
                            double emsx_stop_price = msg.HasElement("EMSX_STOP_PRICE") ? msg.GetElementAsFloat64("EMSX_STOP_PRICE") : 0;
                            int emsx_strategy_end_time = msg.HasElement("EMSX_STRATEGY_END_TIME") ? msg.GetElementAsInt32("EMSX_STRATEGY_END_TIME") : 0;
                            double emsx_strategy_part_rate1 = msg.HasElement("EMSX_STRATEGY_PART_RATE1") ? msg.GetElementAsFloat64("EMSX_STRATEGY_PART_RATE1") : 0;
                            double emsx_strategy_part_rate2 = msg.HasElement("EMSX_STRATEGY_PART_RATE2") ? msg.GetElementAsFloat64("EMSX_STRATEGY_PART_RATE2") : 0;
                            int emsx_strategy_start_time = msg.HasElement("EMSX_STRATEGY_START_TIME") ? msg.GetElementAsInt32("EMSX_STRATEGY_START_TIME") : 0;
                            string emsx_strategy_style = msg.HasElement("EMSX_STRATEGY_STYLE") ? msg.GetElementAsString("EMSX_STRATEGY_STYLE") : "";
                            string emsx_strategy_type = msg.HasElement("EMSX_STRATEGY_TYPE") ? msg.GetElementAsString("EMSX_STRATEGY_TYPE") : "";
                            string emsx_ticker = msg.HasElement("EMSX_TICKER") ? msg.GetElementAsString("EMSX_TICKER") : "";
                            string emsx_tif = msg.HasElement("EMSX_TIF") ? msg.GetElementAsString("EMSX_TIF") : "";
                            int emsx_time_stamp = msg.HasElement("EMSX_TIME_STAMP") ? msg.GetElementAsInt32("EMSX_TIME_STAMP") : 0;
                            int emsx_trad_uuid = msg.HasElement("EMSX_TRAD_UUID") ? msg.GetElementAsInt32("EMSX_TRAD_UUID") : 0;
                            string emsx_trade_desk = msg.HasElement("EMSX_TRADE_DESK") ? msg.GetElementAsString("EMSX_TRADE_DESK") : "";
                            string emsx_trader = msg.HasElement("EMSX_TRADER") ? msg.GetElementAsString("EMSX_TRADER") : "";
                            string emsx_trader_notes = msg.HasElement("EMSX_TRADER_NOTES") ? msg.GetElementAsString("EMSX_TRADER_NOTES") : "";
                            int emsx_ts_ordnum = msg.HasElement("EMSX_TS_ORDNUM") ? msg.GetElementAsInt32("EMSX_TS_ORDNUM") : 0;
                            string emsx_type = msg.HasElement("EMSX_TYPE") ? msg.GetElementAsString("EMSX_TYPE") : "";
                            string emsx_underlying_ticker = msg.HasElement("EMSX_UNDERLYING_TICKER") ? msg.GetElementAsString("EMSX_UNDERLYING_TICKER") : "";
                            double emsx_user_comm_amount = msg.HasElement("EMSX_USER_COMM_AMOUNT") ? msg.GetElementAsFloat64("EMSX_USER_COMM_AMOUNT") : 0;
                            double emsx_user_comm_rate = msg.HasElement("EMSX_USER_COMM_RATE") ? msg.GetElementAsFloat64("EMSX_USER_COMM_RATE") : 0;
                            double emsx_user_fees = msg.HasElement("EMSX_USER_FEES") ? msg.GetElementAsFloat64("EMSX_USER_FEES") : 0;
                            double emsx_user_net_money = msg.HasElement("EMSX_USER_NET_MONEY") ? msg.GetElementAsFloat64("EMSX_USER_NET_MONEY") : 0;
                            double emsx_user_work_price = msg.HasElement("EMSX_WORK_PRICE") ? msg.GetElementAsFloat64("EMSX_WORK_PRICE") : 0;
                            int emsx_working = msg.HasElement("EMSX_WORKING") ? msg.GetElementAsInt32("EMSX_WORKING") : 0;
                            string emsx_yellow_key = msg.HasElement("EMSX_YELLOW_KEY") ? msg.GetElementAsString("EMSX_YELLOW_KEY") : "";

                            System.Console.WriteLine("ORDER MESSAGE: CorrelationID(" + msg.CorrelationID + ") EMSX_SEQUENCE: " + emsx_sequence.ToString() + "\t EVENT_STATUS: " + event_status + "\tEMSX_STATUS: " + emsx_status);

                            /*
                            System.Console.WriteLine("API_SEQ_NUM: " + api_seq_num);
                            System.Console.WriteLine("EMSX_ACCOUNT: " + emsx_account);
                            System.Console.WriteLine("EMSX_AMOUNT: " + emsx_amount);
                            System.Console.WriteLine("EMSX_ARRIVAL_PRICE: " + emsx_arrival_price);
                            System.Console.WriteLine("EMSX_ASSET_CLASS: " + emsx_asset_class);
                            System.Console.WriteLine("EMSX_ASSIGNED_TRADER: " + emsx_assigned_trader);
                            System.Console.WriteLine("EMSX_AVG_PRICE: " + emsx_avg_price);
                            System.Console.WriteLine("EMSX_BASKET_NAME: " + emsx_basket_name);
                            System.Console.WriteLine("EMSX_BASKET_NUM: " + emsx_basket_num);
                            System.Console.WriteLine("EMSX_BROKER: " + emsx_broker);
                            System.Console.WriteLine("EMSX_BROKER_COMM: " + emsx_broker_comm);
                            System.Console.WriteLine("EMSX_BSE_AVG_PRICE: " + emsx_bse_avg_price);
                            System.Console.WriteLine("EMSX_BSE_FILLED: " + emsx_bse_filled);
                            System.Console.WriteLine("EMSX_CFD_FLAG: " + emsx_cfd_flag);
                            System.Console.WriteLine("EMSX_COMM_DIFF_FLAG: " + emsx_comm_diff_flag);
                            System.Console.WriteLine("EMSX_COMM_RATE: " + emsx_comm_rate);
                            System.Console.WriteLine("EMSX_CURRENCY_PAIR: " + emsx_currency_pair);
                            System.Console.WriteLine("EMSX_DATE: " + emsx_date);
                            System.Console.WriteLine("EMSX_DAY_AVG_PRICE: " + emsx_day_avg_price);
                            System.Console.WriteLine("EMSX_DAY_FILL: " + emsx_day_fill);
                            System.Console.WriteLine("EMSX_DIR_BROKER_FLAG: " + emsx_dir_broker_flag);
                            System.Console.WriteLine("EMSX_EXCHANGE: " + emsx_exchange);
                            System.Console.WriteLine("EMSX_EXCHANGE_DESTINATION: " + emsx_exchange_destination);
                            System.Console.WriteLine("EMSX_EXEC_INSTRUCTION: " + emsx_exec_instruction);
                            System.Console.WriteLine("EMSX_FILL_ID: " + emsx_fill_id);
                            System.Console.WriteLine("EMSX_FILLED: " + emsx_filled);
                            System.Console.WriteLine("EMSX_GTD_DATE: " + emsx_gtd_date);
                            System.Console.WriteLine("EMSX_HAND_INSTRUCTION: " + emsx_hand_instruction);
                            System.Console.WriteLine("EMSX_IDLE_AMOUNT: " + emsx_idle_amount);
                            System.Console.WriteLine("EMSX_INVESTOR_ID: " + emsx_investor_id);
                            System.Console.WriteLine("EMSX_ISIN: " + emsx_isin);
                            System.Console.WriteLine("EMSX_LIMIT_PRICE: " + emsx_limit_price);
                            System.Console.WriteLine("EMSX_NOTES: " + emsx_notes);
                            System.Console.WriteLine("EMSX_NSE_AVG_PRICE: " + emsx_nse_avg_price);
                            System.Console.WriteLine("EMSX_NSE_FILLED: " + emsx_nse_filled);
                            System.Console.WriteLine("EMSX_ORD_REF_ID: " + emsx_ord_ref_id);
                            System.Console.WriteLine("EMSX_ORDER_TYPE: " + emsx_order_type);
                            System.Console.WriteLine("EMSX_ORIGINATE_TRADER: " + emsx_originate_trader);
                            System.Console.WriteLine("EMSX_ORIGINATE_TRADER_FIRM: " + emsx_originate_trader_firm);
                            System.Console.WriteLine("EMSX_PERCENT_REMAIN: " + emsx_percent_remain);
                            System.Console.WriteLine("EMSX_PM_UUID: " + emsx_pm_uuid);
                            System.Console.WriteLine("EMSX_PORT_MGR: " + emsx_port_mgr);
                            System.Console.WriteLine("EMSX_PORT_NAME: " + emsx_port_name);
                            System.Console.WriteLine("EMSX_PORT_NUM: " + emsx_port_num);
                            System.Console.WriteLine("EMSX_POSITION: " + emsx_position);
                            System.Console.WriteLine("EMSX_PRINCIPAL: " + emsx_principle);
                            System.Console.WriteLine("EMSX_PRODUCT: " + emsx_product);
                            System.Console.WriteLine("EMSX_QUEUED_DATE: " + emsx_queued_date);
                            System.Console.WriteLine("EMSX_QUEUED_TIME: " + emsx_queued_time);
                            System.Console.WriteLine("EMSX_REASON_CODE: " + emsx_reason_code);
                            System.Console.WriteLine("EMSX_REASON_DESC: " + emsx_reason_desc);
                            System.Console.WriteLine("EMSX_REMAIN_BALANCE: " + emsx_remain_balance);
                            System.Console.WriteLine("EMSX_ROUTE_ID: " + emsx_route_id);
                            System.Console.WriteLine("EMSX_ROUTE_PRICE: " + emsx_route_price);
                            System.Console.WriteLine("EMSX_SEC_NAME: " + emsx_sec_name);
                            System.Console.WriteLine("EMSX_SEDOL: " + emsx_sedol);
                            System.Console.WriteLine("EMSX_SEQUENCE: " + emsx_sequence);
                            System.Console.WriteLine("EMSX_SETTLE_AMOUNT: " + emsx_settle_amount);
                            System.Console.WriteLine("EMSX_SETTLE_DATE: " + emsx_settle_date);
                            System.Console.WriteLine("EMSX_SIDE: " + emsx_side);
                            System.Console.WriteLine("EMSX_START_AMOUNT: " + emsx_start_amount);
                            System.Console.WriteLine("EMSX_STATUS: " + emsx_status);
                            System.Console.WriteLine("EMSX_STEP_OUT_BROKER: " + emsx_step_out_broker);
                            System.Console.WriteLine("EMSX_STOP_PRICE: " + emsx_stop_price);
                            System.Console.WriteLine("EMSX_STRATEGY_END_TIME: " + emsx_strategy_end_time);
                            System.Console.WriteLine("EMSX_STRATEGY_PART_RATE1: " + emsx_strategy_part_rate1);
                            System.Console.WriteLine("EMSX_STRATEGY_PART_RATE2: " + emsx_strategy_part_rate2);
                            System.Console.WriteLine("EMSX_STRATEGY_START_TIME: " + emsx_strategy_start_time);
                            System.Console.WriteLine("EMSX_STRATEGY_STYLE: " + emsx_strategy_style);
                            System.Console.WriteLine("EMSX_STRATEGY_TYPE: " + emsx_strategy_type);
                            System.Console.WriteLine("EMSX_TICKER: " + emsx_ticker);
                            System.Console.WriteLine("EMSX_TIF: " + emsx_tif);
                            System.Console.WriteLine("EMSX_TIME_STAMP: " + emsx_time_stamp);
                            System.Console.WriteLine("EMSX_TRAD_UUID: " + emsx_trad_uuid);
                            System.Console.WriteLine("EMSX_TRADE_DESK: " + emsx_trade_desk);
                            System.Console.WriteLine("EMSX_TRADER: " + emsx_trader);
                            System.Console.WriteLine("EMSX_TRADER_NOTES: " + emsx_trader_notes);
                            System.Console.WriteLine("EMSX_TS_ORDNUM: " + emsx_ts_ordnum);
                            System.Console.WriteLine("EMSX_TYPE: " + emsx_type);
                            System.Console.WriteLine("EMSX_UNDERLYING_TICKER: " + emsx_underlying_ticker);
                            System.Console.WriteLine("EMSX_USER_COMM_AMOUNT: " + emsx_user_comm_amount);
                            System.Console.WriteLine("EMSX_USER_COMM_RATE: " + emsx_user_comm_rate);
                            System.Console.WriteLine("EMSX_USER_FEES: " + emsx_user_fees);
                            System.Console.WriteLine("EMSX_USER_NET_MONEY: " + emsx_user_net_money);
                            System.Console.WriteLine("EMSX_WORK_PRICE: " + emsx_user_work_price);
                            System.Console.WriteLine("EMSX_WORKING: " + emsx_working);
                            System.Console.WriteLine("EMSX_YELLOW_KEY: " + emsx_yellow_key);
                            */
                        }
                        else if (msg_sub_type == "R")
                        {
                            long api_seq_num = msg.HasElement("API_SEQ_NUM") ? msg.GetElementAsInt64("API_SEQ_NUM") : 0;
                            int emsx_amount = msg.HasElement("EMSX_AMOUNT") ? msg.GetElementAsInt32("EMSX_AMOUNT") : 0;
                            double emsx_avg_price = msg.HasElement("EMSX_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_AVG_PRICE") : 0;
                            string emsx_broker = msg.HasElement("EMSX_BROKER") ? msg.GetElementAsString("EMSX_BROKER") : "";
                            double emsx_broker_comm = msg.HasElement("EMSX_BROKER_COMM") ? msg.GetElementAsFloat64("EMSX_BROKER_COMM") : 0;
                            double emsx_bse_avg_price = msg.HasElement("EMSX_BSE_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_BSE_AVG_PRICE") : 0;
                            int emsx_bse_filled = msg.HasElement("EMSX_BSE_FILLED") ? msg.GetElementAsInt32("EMSX_BSE_FILLED") : 0;
                            string emsx_clearing_account = msg.HasElement("EMSX_CLEARING_ACCOUNT") ? msg.GetElementAsString("EMSX_CLEARING_ACCOUNT") : "";
                            string emsx_clearing_firm = msg.HasElement("EMSX_CLEARING_FIRM") ? msg.GetElementAsString("EMSX_CLEARING_FIRM") : "";
                            string emsx_comm_diff_flag = msg.HasElement("EMSX_COMM_DIFF_FLAG") ? msg.GetElementAsString("EMSX_COMM_DIFF_FLAG") : "";
                            double emsx_comm_rate = msg.HasElement("EMSX_COMM_RATE") ? msg.GetElementAsFloat64("EMSX_COMM_RATE") : 0;
                            string emsx_currency_pair = msg.HasElement("EMSX_CURRENCY_PAIR") ? msg.GetElementAsString("EMSX_CURRENCY_PAIR") : "";
                            string emsx_custom_account = msg.HasElement("EMSX_CUSTOM_ACCOUNT") ? msg.GetElementAsString("EMSX_CUSTOM_ACCOUNT") : "";
                            double emsx_day_avg_price = msg.HasElement("EMSX_DAY_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_DAY_AVG_PRICE") : 0;
                            int emsx_day_fill = msg.HasElement("EMSX_DAY_FILL") ? msg.GetElementAsInt32("EMSX_DAY_FILL") : 0;
                            string emsx_exchange_destination = msg.HasElement("EMSX_EXCHANGE_DESTINATION") ? msg.GetElementAsString("EMSX_EXCHANGE_DESTINATION") : "";
                            string emsx_exec_instruction = msg.HasElement("EMSX_EXEC_INSTRUCTION") ? msg.GetElementAsString("EMSX_EXEC_INSTRUCTION") : "";
                            string emsx_execute_broker = msg.HasElement("EMSX_EXECUTE_BROKER") ? msg.GetElementAsString("EMSX_EXECUTE_BROKER") : "";
                            int emsx_fill_id = msg.HasElement("EMSX_FILL_ID") ? msg.GetElementAsInt32("EMSX_FILL_ID") : 0;
                            int emsx_filled = msg.HasElement("EMSX_FILLED") ? msg.GetElementAsInt32("EMSX_FILLED") : 0;
                            int emsx_gtd_date = msg.HasElement("EMSX_GTD_DATE") ? msg.GetElementAsInt32("EMSX_GTD_DATE") : 0;
                            string emsx_hand_instruction = msg.HasElement("EMSX_HAND_INSTRUCTION") ? msg.GetElementAsString("EMSX_HAND_INSTRUCTION") : "";
                            int emsx_is_manual_route = msg.HasElement("EMSX_IS_MANUAL_ROUTE") ? msg.GetElementAsInt32("EMSX_IS_MANUAL_ROUTE") : 0;
                            int emsx_last_fill_date = msg.HasElement("EMSX_LAST_FILL_DATE") ? msg.GetElementAsInt32("EMSX_LAST_FILL_DATE") : 0;
                            int emsx_last_fill_time = msg.HasElement("EMSX_LAST_FILL_TIME") ? msg.GetElementAsInt32("EMSX_LAST_FILL_TIME") : 0;
                            string emsx_last_market = msg.HasElement("EMSX_LAST_MARKET") ? msg.GetElementAsString("EMSX_LAST_MARKET") : "";
                            double emsx_last_price = msg.HasElement("EMSX_LAST_PRICE") ? msg.GetElementAsFloat64("EMSX_LAST_PRICE") : 0;
                            int emsx_last_shares = msg.HasElement("EMSX_LAST_SHARES") ? msg.GetElementAsInt32("EMSX_LAST_SHARES") : 0;
                            double emsx_limit_price = msg.HasElement("EMSX_LIMIT_PRICE") ? msg.GetElementAsFloat64("EMSX_LIMIT_PRICE") : 0;
                            double emsx_misc_fees = msg.HasElement("EMSX_MISC_FEES") ? msg.GetElementAsFloat64("EMSX_MISC_FEES") : 0;
                            int emsx_ml_leg_quantity = msg.HasElement("EMSX_ML_LEG_QUANTITY") ? msg.GetElementAsInt32("EMSX_ML_LEG_QUANTITY") : 0;
                            int emsx_ml_num_legs = msg.HasElement("EMSX_ML_NUM_LEGS") ? msg.GetElementAsInt32("EMSX_ML_NUM_LEGS") : 0;
                            double emsx_ml_percent_filled = msg.HasElement("EMSX_ML_PERCENT_FILLED") ? msg.GetElementAsFloat64("EMSX_ML_PERCENT_FILLED") : 0;
                            double emsx_ml_ratio = msg.HasElement("EMSX_ML_RATIO") ? msg.GetElementAsFloat64("EMSX_ML_RATIO") : 0;
                            double emsx_ml_remain_balance = msg.HasElement("EMSX_ML_REMAIN_BALANCE") ? msg.GetElementAsFloat64("EMSX_ML_REMAIN_BALANCE") : 0;
                            string emsx_ml_strategy = msg.HasElement("EMSX_ML_STRATEGY") ? msg.GetElementAsString("EMSX_ML_STRATEGY") : "";
                            int emsx_ml_total_quantity = msg.HasElement("EMSX_ML_TOTAL_QUANTITY") ? msg.GetElementAsInt32("EMSX_ML_TOTAL_QUANTITY") : 0;
                            string emsx_notes = msg.HasElement("EMSX_NOTES") ? msg.GetElementAsString("EMSX_NOTES") : "";
                            double emsx_nse_avg_price = msg.HasElement("EMSX_NSE_AVG_PRICE") ? msg.GetElementAsFloat64("EMSX_NSE_AVG_PRICE") : 0;
                            int emsx_nse_filled = msg.HasElement("EMSX_NSE_FILLED") ? msg.GetElementAsInt32("EMSX_NSE_FILLED") : 0;
                            string emsx_order_type = msg.HasElement("EMSX_ORDER_TYPE") ? msg.GetElementAsString("EMSX_ORDER_TYPE") : "";
                            string emsx_p_a = msg.HasElement("EMSX_P_A") ? msg.GetElementAsString("EMSX_P_A") : "";
                            double emsx_percent_remain = msg.HasElement("EMSX_PERCENT_REMAIN") ? msg.GetElementAsFloat64("EMSX_PERCENT_REMAIN") : 0;
                            double emsx_principal = msg.HasElement("EMSX_PRINCIPAL") ? msg.GetElementAsFloat64("EMSX_PRINCIPAL") : 0;
                            int emsx_queued_date = msg.HasElement("EMSX_QUEUED_DATE") ? msg.GetElementAsInt32("EMSX_QUEUED_DATE") : 0;
                            int emsx_queued_time = msg.HasElement("EMSX_QUEUED_TIME") ? msg.GetElementAsInt32("EMSX_QUEUED_TIME") : 0;
                            string emsx_reason_code = msg.HasElement("EMSX_REASON_CODE") ? msg.GetElementAsString("EMSX_REASON_CODE") : "";
                            string emsx_reason_desc = msg.HasElement("EMSX_REASON_DESC") ? msg.GetElementAsString("EMSX_REASON_DESC") : "";
                            double emsx_remain_balance = msg.HasElement("EMSX_REMAIN_BALANCE") ? msg.GetElementAsFloat64("EMSX_REMAIN_BALANCE") : 0;
                            int emsx_route_create_date = msg.HasElement("EMSX_ROUTE_CREATE_DATE") ? msg.GetElementAsInt32("EMSX_ROUTE_CREATE_DATE") : 0;
                            int emsx_route_create_time = msg.HasElement("EMSX_ROUTE_CREATE_TIME") ? msg.GetElementAsInt32("EMSX_ROUTE_CREATE_TIME") : 0;
                            int emsx_route_id = msg.HasElement("EMSX_ROUTE_ID") ? msg.GetElementAsInt32("EMSX_ROUTE_ID") : 0;
                            int emsx_route_last_update_time = msg.HasElement("EMSX_ROUTE_LAST_UPDATE_TIME") ? msg.GetElementAsInt32("EMSX_ROUTE_LAST_UPDATE_TIME") : 0;
                            double emsx_route_price = msg.HasElement("EMSX_ROUTE_PRICE") ? msg.GetElementAsFloat64("EMSX_ROUTE_PRICE") : 0;
                            int emsx_sequence = msg.HasElement("EMSX_SEQUENCE") ? msg.GetElementAsInt32("EMSX_SEQUENCE") : 0;
                            double emsx_settle_amount = msg.HasElement("EMSX_SETTLE_AMOUNT") ? msg.GetElementAsFloat64("EMSX_SETTLE_AMOUNT") : 0;
                            int emsx_settle_date = msg.HasElement("EMSX_SETTLE_DATE") ? msg.GetElementAsInt32("EMSX_SETTLE_DATE") : 0;
                            string emsx_status = msg.HasElement("EMSX_STATUS") ? msg.GetElementAsString("EMSX_STATUS") : "";
                            double emsx_stop_price = msg.HasElement("EMSX_STOP_PRICE") ? msg.GetElementAsFloat64("EMSX_STOP_PRICE") : 0;
                            int emsx_strategy_end_time = msg.HasElement("EMSX_STRATEGY_END_TIME") ? msg.GetElementAsInt32("EMSX_STRATEGY_END_TIME") : 0;
                            double emsx_strategy_part_rate1 = msg.HasElement("EMSX_STRATEGY_PART_RATE1") ? msg.GetElementAsFloat64("EMSX_STRATEGY_PART_RATE1") : 0;
                            double emsx_strategy_part_rate2 = msg.HasElement("EMSX_STRATEGY_PART_RATE2") ? msg.GetElementAsFloat64("EMSX_STRATEGY_PART_RATE2") : 0;
                            int emsx_strategy_start_time = msg.HasElement("EMSX_STRATEGY_START_TIME") ? msg.GetElementAsInt32("EMSX_STRATEGY_START_TIME") : 0;
                            string emsx_strategy_style = msg.HasElement("EMSX_STRATEGY_STYLE") ? msg.GetElementAsString("EMSX_STRATEGY_STYLE") : "";
                            string emsx_strategy_type = msg.HasElement("EMSX_STRATEGY_TYPE") ? msg.GetElementAsString("EMSX_STRATEGY_TYPE") : "";
                            string emsx_tif = msg.HasElement("EMSX_TIF") ? msg.GetElementAsString("EMSX_TIF") : "";
                            int emsx_time_stamp = msg.HasElement("EMSX_TIME_STAMP") ? msg.GetElementAsInt32("EMSX_TIME_STAMP") : 0;
                            string emsx_type = msg.HasElement("EMSX_TYPE") ? msg.GetElementAsString("EMSX_TYPE") : "";
                            int emsx_urgency_level = msg.HasElement("EMSX_URGENCY_LEVEL") ? msg.GetElementAsInt32("EMSX_URGENCY_LEVEL") : 0;
                            double emsx_user_comm_amount = msg.HasElement("EMSX_USER_COMM_AMOUNT") ? msg.GetElementAsFloat64("EMSX_USER_COMM_AMOUNT") : 0;
                            double emsx_user_comm_rate = msg.HasElement("EMSX_USER_COMM_RATE") ? msg.GetElementAsFloat64("EMSX_USER_COMM_RATE") : 0;
                            double emsx_user_fees = msg.HasElement("EMSX_USER_FEES") ? msg.GetElementAsFloat64("EMSX_USER_FEES") : 0;
                            double emsx_user_net_money = msg.HasElement("EMSX_USER_NET_MONEY") ? msg.GetElementAsFloat64("EMSX_USER_NET_MONEY") : 0;
                            int emsx_working = msg.HasElement("EMSX_WORKING") ? msg.GetElementAsInt32("EMSX_WORKING") : 0;

                            System.Console.WriteLine("ROUTE MESSAGE: CorrelationID(" + msg.CorrelationID + ") EMSX_SEQUENCE: " + emsx_sequence.ToString() + "\tROUTE ID: " + emsx_route_id.ToString() + "\t EVENT_STATUS: " + event_status + "\tEMSX_STATUS: " + emsx_status);

                            /*
                            System.Console.WriteLine("API_SEQ_NUM: " + api_seq_num);
                            System.Console.WriteLine("EMSX_AMOUNT: " + emsx_amount);
                            System.Console.WriteLine("EMSX_AVG_PRICE: " + emsx_avg_price);
                            System.Console.WriteLine("EMSX_BROKER: " + emsx_broker);
                            System.Console.WriteLine("EMSX_BROKER_COMM: " + emsx_broker_comm);
                            System.Console.WriteLine("EMSX_BSE_AVG_PRICE: " + emsx_bse_avg_price);
                            System.Console.WriteLine("EMSX_BSE_FILLED: " + emsx_bse_filled);
                            System.Console.WriteLine("EMSX_CLEARING_ACCOUNT: " + emsx_clearing_account);
                            System.Console.WriteLine("EMSX_CLEARING_FIRM: " + emsx_clearing_firm);
                            System.Console.WriteLine("EMSX_COMM_DIFF_FLAG: " + emsx_comm_diff_flag);
                            System.Console.WriteLine("EMSX_COMM_RATE: " + emsx_comm_rate);
                            System.Console.WriteLine("EMSX_CURRENCY_PAIR: " + emsx_currency_pair);
                            System.Console.WriteLine("EMSX_CUSTOM_ACCOUNT: " + emsx_custom_account);
                            System.Console.WriteLine("EMSX_DAY_AVG_PRICE: " + emsx_day_avg_price);
                            System.Console.WriteLine("EMSX_DAY_FILL: " + emsx_day_fill);
                            System.Console.WriteLine("EMSX_EXCHANGE_DESTINATION: " + emsx_exchange_destination);
                            System.Console.WriteLine("EMSX_EXEC_INSTRUCTION: " + emsx_exec_instruction);
                            System.Console.WriteLine("EMSX_EXECUTE_BROKER: " + emsx_execute_broker);
                            System.Console.WriteLine("EMSX_FILL_ID: " + emsx_fill_id);
                            System.Console.WriteLine("EMSX_FILLED: " + emsx_filled);
                            System.Console.WriteLine("EMSX_GTD_DATE: " + emsx_gtd_date);
                            System.Console.WriteLine("EMSX_HAND_INSTRUCTION: " + emsx_hand_instruction);
                            System.Console.WriteLine("EMSX_IS_MANUAL_ROUTE: " + emsx_is_manual_route);
                            System.Console.WriteLine("EMSX_LAST_FILL_DATE: " + emsx_last_fill_date);
                            System.Console.WriteLine("EMSX_LAST_FILL_TIME: " + emsx_last_fill_time);
                            System.Console.WriteLine("EMSX_LAST_MARKET: " + emsx_last_market);
                            System.Console.WriteLine("EMSX_LAST_PRICE: " + emsx_last_price);
                            System.Console.WriteLine("EMSX_LAST_SHARES: " + emsx_last_shares);
                            System.Console.WriteLine("EMSX_LIMIT_PRICE: " + emsx_limit_price);
                            System.Console.WriteLine("EMSX_MISC_FEES: " + emsx_misc_fees);
                            System.Console.WriteLine("EMSX_ML_LEG_QUANTITY: " + emsx_ml_leg_quantity);
                            System.Console.WriteLine("EMSX_ML_NUM_LEGS: " + emsx_ml_num_legs);
                            System.Console.WriteLine("EMSX_ML_PERCENT_FILLED: " + emsx_ml_percent_filled);
                            System.Console.WriteLine("EMSX_ML_RATIO: " + emsx_ml_ratio);
                            System.Console.WriteLine("EMSX_ML_REMAIN_BALANCE: " + emsx_ml_remain_balance);
                            System.Console.WriteLine("EMSX_ML_STRATEGY: " + emsx_ml_strategy);
                            System.Console.WriteLine("EMSX_ML_TOTAL_QUANTITY: " + emsx_ml_total_quantity);
                            System.Console.WriteLine("EMSX_NOTES: " + emsx_notes);
                            System.Console.WriteLine("EMSX_NSE_AVG_PRICE: " + emsx_nse_avg_price);
                            System.Console.WriteLine("EMSX_NSE_FILLED: " + emsx_nse_filled);
                            System.Console.WriteLine("EMSX_ORDER_TYPE: " + emsx_order_type);
                            System.Console.WriteLine("EMSX_P_A: " + emsx_p_a);
                            System.Console.WriteLine("EMSX_PERCENT_REMAIN: " + emsx_percent_remain);
                            System.Console.WriteLine("EMSX_PRINCIPAL: " + emsx_principal);
                            System.Console.WriteLine("EMSX_QUEUED_DATE: " + emsx_queued_date);
                            System.Console.WriteLine("EMSX_QUEUED_TIME: " + emsx_queued_time);
                            System.Console.WriteLine("EMSX_REASON_CODE: " + emsx_reason_code);
                            System.Console.WriteLine("EMSX_REASON_DESC: " + emsx_reason_desc);
                            System.Console.WriteLine("EMSX_REMAIN_BALANCE: " + emsx_remain_balance);
                            System.Console.WriteLine("EMSX_ROUTE_CREATE_DATE: " + emsx_route_create_date);
                            System.Console.WriteLine("EMSX_ROUTE_CREATE_TIME: " + emsx_route_create_time);
                            System.Console.WriteLine("EMSX_ROUTE_ID: " + emsx_route_id);
                            System.Console.WriteLine("EMSX_ROUTE_LAST_UPDATE_TIME: " + emsx_route_last_update_time);
                            System.Console.WriteLine("EMSX_ROUTE_PRICE: " + emsx_route_price);
                            System.Console.WriteLine("EMSX_SEQUENCE: " + emsx_sequence);
                            System.Console.WriteLine("EMSX_SETTLE_AMOUNT: " + emsx_settle_amount);
                            System.Console.WriteLine("EMSX_SETTLE_DATE: " + emsx_settle_date);
                            System.Console.WriteLine("EMSX_STATUS: " + emsx_status);
                            System.Console.WriteLine("EMSX_STOP_PRICE: " + emsx_stop_price);
                            System.Console.WriteLine("EMSX_STRATEGY_END_TIME: " + emsx_strategy_end_time);
                            System.Console.WriteLine("EMSX_STRATEGY_PART_RATE1: " + emsx_strategy_part_rate1);
                            System.Console.WriteLine("EMSX_STRATEGY_PART_RATE2: " + emsx_strategy_part_rate2);
                            System.Console.WriteLine("EMSX_STRATEGY_START_TIME: " + emsx_strategy_start_time);
                            System.Console.WriteLine("EMSX_STRATEGY_STYLE: " + emsx_strategy_style);
                            System.Console.WriteLine("EMSX_STRATEGY_TYPE: " + emsx_strategy_type);
                            System.Console.WriteLine("EMSX_TIF: " + emsx_tif);
                            System.Console.WriteLine("EMSX_TIME_STAMP: " + emsx_time_stamp);
                            System.Console.WriteLine("EMSX_TYPE: " + emsx_type);
                            System.Console.WriteLine("EMSX_URGENCY_LEVEL: " + emsx_urgency_level);
                            System.Console.WriteLine("EMSX_USER_COMM_AMOUNT: " + emsx_user_comm_amount);
                            System.Console.WriteLine("EMSX_USER_COMM_RATE: " + emsx_user_comm_rate);
                            System.Console.WriteLine("EMSX_USER_FEES: " + emsx_user_fees);
                            System.Console.WriteLine("EMSX_USER_NET_MONEY: " + emsx_user_net_money);
                            System.Console.WriteLine("EMSX_WORKING: " + emsx_working);
                            */
                        }
                    }
                }
                else
                {
                    System.Console.Error.WriteLine("Error: Unexpected message");
                }

            }
        }

        private void processResponseEvent(Event evt, Session session)
        {
            System.Console.WriteLine("Processing " + evt.Type);

            foreach (Message msg in evt)
            {
                if (msg.CorrelationID == refdata_req_id)
                {
                    System.Console.WriteLine("RefData RESPONSE received...");

                    Element securities = msg.GetElement(SECURITY_DATA);
                    Element security = securities.GetValueAsElement(0);
                    Element fields = security.GetElement(FIELD_DATA);
                    String ticker_desc = fields.GetElementAsString("PARSEKYABLE_DES_SOURCE");
                    
                    System.Console.WriteLine("Ticker Description: " + ticker_desc);

                }
                else
                {

                    CorrelationID matchCorID = msg.CorrelationID;

                    if (msg.MessageType.Equals(ERROR_INFO))
                    {
                        int errorCode = msg.GetElementAsInt32("ERROR_CODE");
                        string errorMessage = msg.GetElementAsString("ERROR_MESSAGE");

                        System.Console.WriteLine("RESPONSE ERROR\nERROR CODE: " + errorCode + "\tERROR MESSAGE: " + errorMessage);
                    }
                    else if (msg.MessageType.Equals(CREATE_ORDER_AND_ROUTE))
                    {
                        System.Console.WriteLine("CREATE_ORDER_AND_ROUTE Response received...");

                        int emsx_sequence = msg.GetElementAsInt32("EMSX_SEQUENCE");
                        int emsx_route_id = msg.GetElementAsInt32("EMSX_ROUTE_ID");
                        string message = msg.GetElementAsString("MESSAGE");

                        System.Console.WriteLine("EMSX_SEQUENCE: " + emsx_sequence.ToString() + "\tEMSX_ROUTE_ID: " + emsx_route_id.ToString() + "\tMESSAGE: " + message);

                        IDSequenceMap matched = GetMapEntry(matchCorID);

                        if (!matched.Equals(null))
                        {
                            System.Console.WriteLine("Request ID recognised(" + matched.requestID + ")...assining SEQUENCE and ROUTE ID");
                            matched.sequenceNo = emsx_sequence;
                            matched.routeID = emsx_route_id;
                        }
                        else
                        {
                            System.Console.WriteLine("Request ID not recognised(" + matchCorID + ")...ignoring");
                        }
                    }
                }
            }
        }

        private IDSequenceMap GetMapEntry(CorrelationID id)
        {
            foreach (IDSequenceMap find in idMapping)
            {
                if (find.requestID.Equals(id))
                {
                    return find;
                }
            }

            return null;
        }

        private void processMiscEvent(Event evt, Session session)
        {
            System.Console.WriteLine("Processing " + evt.Type);
            foreach (Message msg in evt)
            {
                System.Console.WriteLine("MESSAGE: " + msg);
            }
        }

        private bool parseCommandLine(string[] args)
        {
            if (args.Length < 4)
            {
                System.Console.WriteLine("Error: Missing required parameters\n");
                printUsage();
                return false;
            }
            else
            {
                for (int i = 0; i < args.Length; i++)
                {
                    if (isArg(args[i], "SELLAT")) sellat = System.Convert.ToDouble(getArgValue(args[i]));
                    else if (isArg(args[i], "BUYAT")) buyat = System.Convert.ToDouble(getArgValue(args[i]));
                    else if (isArg(args[i], "AMOUNT")) amount = System.Convert.ToInt32(getArgValue(args[i]));
                    else if (isArg(args[i], "TICKER")) ticker = getArgValue(args[i]);
                    else System.Console.WriteLine("Warning>> Unknown parameter:" + args[i]);
                }

                showParameters();
            }
            return true;
        }

        private void showParameters()
        {
            System.Console.WriteLine("Parameter List:-");
            System.Console.WriteLine("SELLAT: " + sellat);
            System.Console.WriteLine("BUYAT: " + buyat);
            System.Console.WriteLine("AMOUNT: " + amount);
            System.Console.WriteLine("TICKER: " + ticker);

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
            System.Console.WriteLine("Usage:");
            System.Console.WriteLine("EMSXCreateOrderAsyncRequest SELLAT=<value> BUYAT=<value> AMOUNT=<value> TICKER=\"<value>\"");
        }
    }
}
