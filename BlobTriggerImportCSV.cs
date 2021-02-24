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
                db.StringSetAsync(p.GetHashCode().ToString(), p.GetHashCode());
                log.LogInformation($"{p.GetHashCode()} was set.");
            }
            log.LogInformation($"done.");

        }   
    }
    
    public class Person
    {
        public string id { get; set; }
        public string phone { get; set; }
        public string date { get; set; }

        public override bool Equals(object obj)
        {
            return obj is Person person &&
                   id == person.id &&
                   phone == person.phone &&
                   date == person.date;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(id, phone, date);
        }

        // public override string ToString()
        // {
        //     return $"Person(id:{id}, phone:{phone}, date:{date})";
        // }
    }  
}
