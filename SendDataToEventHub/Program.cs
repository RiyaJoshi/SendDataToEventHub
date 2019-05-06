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
        static string eventHubName = "<eventhub name>";
        static string connectionString = "Endpoint=sb://<eventhub name>/;SharedAccessKeyName=<policy>;SharedAccessKey=<access key>";

        public void sendMsg(string filePath)
        {


            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);
            StreamReader reader = new StreamReader(File.OpenRead(filePath));



            try
            {
               StreamReader sr = new StreamReader(filePath);
 
               var csvnew = new List<string>();

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
            p.filePathcsv = "C:\\Users\\*\\OneDrive - Microsoft\\*\\StreamAnalytics-POC\\CabData.csv"; // file path
            p.sendMsg(p.filePathcsv);

        }
    }
}
