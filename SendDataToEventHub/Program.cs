using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
//using System.Threading.Tasks
using System.Threading;
using Microsoft.ServiceBus.Messaging;
using System.Web.Script.Serialization;
using System.Linq;
using Newtonsoft.Json.Linq;


namespace SendDataToEventHub
{
    class Program
    {
        private string filePathjson;
        private string filePathcsv;
        static string eventHubName = "testpoceventhub";
        static string connectionString = "Endpoint=sb://testpocns.servicebus.windows.net/;SharedAccessKeyName=testpocPolicy;SharedAccessKey=vj1DZyYn5UIxmZqh2RMZAyweCFH9xjj8ji2U/fGhPBk=";

        public void sendMsg(string filePath)
        {


            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);
            //var reader = new StreamReader(File.OpenRead(filePath))
            StreamReader reader = new StreamReader(File.OpenRead(filePath));
            //JavaScriptSerializer serializer = new JavaScriptSerializer();
            //List<string> searchList = new List<string>();
            //while (!reader.EndOfStream)
            //{
            //var line = reader.ReadLine();
            //var jsonArray = JsonArray.Parse(st);


            try
            {
               StreamReader sr = new StreamReader(filePath);
                //var csv = new List<string[]>(); // or, List<YourClass>
                
               var csvnew = new List<string>();
               //var lines = System.IO.File.ReadAllLines(filePath);
               string headerLine = sr.ReadLine();
               string line;
               while ((line = sr.ReadLine()) != null)
               {
                    //Console.WriteLine(line);
                    string[] csv = line.Split(',');
                    var dictionary = new Dictionary<string, string>();
                    dictionary.Add("dispatching_base_number",csv[0]);
                    dictionary.Add("available_vehicles", csv[2]);
                    dictionary.Add("vehicles_in_trips", csv[3]);
                    dictionary.Add("Cancellations", csv[4]);
                    string jsonN = new System.Web.Script.Serialization.JavaScriptSerializer().Serialize(dictionary);
                    Console.WriteLine("Sending message: {0}",jsonN);
                    eventHubClient.Send(new EventData(Encoding.UTF8.GetBytes(jsonN)));
                    // csv.Add(line.Split(',')); // or, populate YourClass    
                    // Console.WriteLine(csv[0],csv[1]);

                    // string val = "dispatching_base_number"+ " : " + csv[0][0] + " available_vehicles"+" : " + csv[0][2] + " vehicles_in_trips "+ ": " + csv[0][3] + " Cancellations "+ " : " + csv[0][4];
                    //csvnew.Add(" dispatching_base_number : " + csv[0][0]);
                    //csvnew.Add(" available_vehicles : " + csv[0][2]);
                    //csvnew.Add(" vehicles_in_trips : " + csv[0][3]);
                    //csvnew.Add(" Cancellations : " + csv[0][3]);


                    //Console.WriteLine(val);
                    // string jsonN = new System.Web.Script.Serialization.JavaScriptSerializer().Serialize(val);
                    // Console.WriteLine("Sending message: {0}",jsonN);
                    //eventHubClient.Send(new EventData(Encoding.UTF8.GetBytes(jsonN)));
                    //csv.Clear();
                    //csvnew.Clear();
                   Thread.Sleep(200);
               }

                 string json = reader.ReadToEnd();
                 Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, json);
                 eventHubClient.Send(new EventData(Encoding.UTF8.GetBytes(json)));
                 Thread.Sleep(200);

            }
            catch (Exception exception)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
                Console.ResetColor();
            }
                   
            Console.WriteLine("All records processed");
            Console.ReadLine();

        }

    
        static void Main(string[] args)
        {
            Program p = new Program();
           // p.filePathjson= "C:\\Users\\rijosh\\OneDrive - Microsoft\\learnings\\StreamAnalytics-POC\\UberData.json";
            p.filePathcsv = "C:\\Users\\rijosh\\OneDrive - Microsoft\\learnings\\StreamAnalytics-POC\\CabData.csv";
            p.sendMsg(p.filePathcsv);

        }
    }
}
