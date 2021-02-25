using System;
using CsvHelper;
using System.Collections;
using System.Globalization;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using StackExchange.Redis;


namespace Company.Function
{
    public static class BlobTriggerImportCSV
    {
        [FunctionName("BlobTriggerImportCSV")]
        public static void Run([BlobTrigger("csv/{name}", Connection = "myblobtrial_STORAGE")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            var records = new List<Person>();
            using var tr = new StreamReader(myBlob);
            using var csv = new CsvReader(tr, CultureInfo.InvariantCulture);
            if (csv.Read())
            {
                log.LogInformation("Reading CSV");
                csv.ReadHeader();
                while (csv.Read())
                {
                    var record = new Person
                    {
                        id = csv.GetField("id"),
                        phone = csv.GetField("phone"),
                        date = csv.GetField("date")
                    };
                    // log.LogInformation($"read new record from csv file: {record}, hashcode:{record.GetHashCode()}");
                    records.Add(record);
                }
            }

            log.LogInformation($"loading {records.Count} record hashcodes to redis ...");
            var redConStr = Environment.GetEnvironmentVariable("redis_constr");
            // log.LogInformation(redConStr);


            var redis = ConnectionMultiplexer.Connect(redConStr);
            IDatabase db = redis.GetDatabase();

            foreach (Person p in records)
            {
                var hashCode = p.ToString().GetStableHashCode();
                db.StringSetAsync(hashCode.ToString(), hashCode);
                log.LogInformation($"{hashCode} was set to redis.");
            }
            log.LogInformation($"done.");

        }   
    }
    
    public class Person
    {
        public string id { get; set; }
        public string phone { get; set; }
        public string date { get; set; }

        public override string ToString()
        {
            return $"Person(id:{id}, phone:{phone}, date:{date})";
        }
    }  

    public static class StringExtensionMethods
    {
        public static int GetStableHashCode(this string str)
        {
            unchecked
            {
                int hash1 = 5381;
                int hash2 = hash1;

                for(int i = 0; i < str.Length && str[i] != '\0'; i += 2)
                {
                    hash1 = ((hash1 << 5) + hash1) ^ str[i];
                    if (i == str.Length - 1 || str[i+1] == '\0')
                        break;
                    hash2 = ((hash2 << 5) + hash2) ^ str[i+1];
                }

                return hash1 + (hash2*1566083941);
            }
        }
    }  
}
