using Microsoft.AspNet.SignalR.Client;
using Microsoft.AspNet.SignalR.Client.Transports;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace HpcPackRestEventBvt
{
    class Program
    {
        class ApiError : Exception
        {
            public HttpStatusCode Code { get; set; }

            public new string Message { get; set; }

            public ApiError(HttpStatusCode code, string message)
            {
                Code = code;
                Message = message;
            }

            public override string ToString()
            {
                return $"ApiError: Code = {Code}, Message = {Message}";
            }
        }

        static async Task CheckHttpErrorAsync(HttpResponseMessage response)
        {
            if (!response.IsSuccessStatusCode)
            {
                string message = await response.Content.ReadAsStringAsync();
                throw new ApiError(response.StatusCode, message);
            }
        }

        class AssertionError : Exception
        {
            public AssertionError(string msg) : base(msg) { }
        }

        static void Assert(bool value, string msg = null)
        {
            if (!value)
            {
                throw new AssertionError(msg);
            }
        }

        static string BasicAuthHeader(string username, string password)
        {
            string credentials = $"{username}:{password}";
            byte[] bytes = Encoding.ASCII.GetBytes(credentials);
            string base64 = Convert.ToBase64String(bytes);
            return $"Basic {base64}";
        }

        static HttpClient CreateHttpClient(string hostname, string username, string password)
        {
            string apiBase = $"https://{hostname}";
            var handler = new HttpClientHandler();
            var httpClient = new HttpClient(handler) { BaseAddress = new Uri(apiBase) };
            httpClient.DefaultRequestHeaders.Add("Authorization", BasicAuthHeader(username, password));
            return httpClient;
        }

        static async Task Test(string hostname, string username, string password)
        {
            var httpClient = CreateHttpClient(hostname, username, password);
            string url = "/hpc/jobs/jobFile";
            string xmlJob = @"
<Job Name=""SimpleJob"">
  <Tasks>
    <Task CommandLine=""echo Hello"" MinCores=""1"" MaxCores=""1"" />
  </Tasks>
</Job>
";
            Console.WriteLine($"Create a job from XML:\n{xmlJob}");
            var response = await httpClient.PostAsync(url, new StringContent(JsonConvert.SerializeObject(xmlJob), Encoding.UTF8, "application/json"));
            await CheckHttpErrorAsync(response);
            string result = await response.Content.ReadAsStringAsync();
            var jobId = int.Parse(result);


            url = $"{httpClient.BaseAddress}/hpc";
            var hubConnection = new HubConnection(url); // { TraceLevel = TraceLevels.All, TraceWriter = Console.Error };
            hubConnection.Headers.Add("Authorization", BasicAuthHeader(username, password));
            hubConnection.Error += ex => Console.WriteLine($"HubConnection Exception:\n{ex}");

            var jobEventHubProxy = hubConnection.CreateHubProxy("JobEventHub");
            string prevJobState = "Configuring";
            jobEventHubProxy.On("JobStateChange", (int id, string state, string previousState) =>
            {
                Console.WriteLine($"Job: {id}, State: {state}, Previous State: {previousState}");
                Assert(id == jobId);
                Assert(string.Equals(previousState, prevJobState, StringComparison.OrdinalIgnoreCase));
                prevJobState = state;
            });

            var taskEventHubProxy = hubConnection.CreateHubProxy("TaskEventHub");
            string prevTaskState = "Submitted";
            taskEventHubProxy.On("TaskStateChange", (int id, int taskId, int instanceId, string state, string previousState) =>
            {
                Console.WriteLine($"Job: {id}, Task: {taskId}, State: {state}, Previous State: {previousState}");
                Assert(id == jobId);
                Assert(string.Equals(previousState, prevTaskState, StringComparison.OrdinalIgnoreCase));
                prevTaskState = state;
            });

            Console.WriteLine($"Connecting to {url} ...");
            try
            {
                await hubConnection.Start(new WebSocketTransport());
            }
            catch (Exception ex)
            {
                throw new Exception($"Exception on starting:\n{ex}");
            }

            Console.WriteLine($"Begin to listen...");
            try
            {
                await jobEventHubProxy.Invoke("BeginListen", jobId);
                await taskEventHubProxy.Invoke("BeginListen", jobId);
            }
            catch (Exception ex)
            {
                throw new Exception($"Exception on invoking server method:\n{ex}");
            }

            Console.WriteLine($"Submit job {jobId}");
            url = $"/hpc/jobs/{jobId}/submit";
            response = await httpClient.PostAsync(url, new StringContent(""));
            await CheckHttpErrorAsync(response);

            await Task.Delay(30 * 1000);

            Assert(string.Equals(prevJobState, "Finished", StringComparison.OrdinalIgnoreCase));
            Assert(string.Equals(prevTaskState, "Finished", StringComparison.OrdinalIgnoreCase));
        }

        static async Task MainAsync(string[] args)
        {
            // This disables enforcement of certificat trust chains which enables the use of self-signed certs.
            ServicePointManager.ServerCertificateValidationCallback = (obj, cert, chain, err) => true;

            string hostname = Environment.GetEnvironmentVariable("bvt_hostname");   
            string username = Environment.GetEnvironmentVariable("bvt_username");   
            string password = Environment.GetEnvironmentVariable("bvt_password");   

            if (string.IsNullOrWhiteSpace(hostname) || string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
            {
                throw new Exception("Environment variables bvt_hostname, bvt_username and bvt_password must be specified!");
            }

            try
            {
                await Test(hostname, username, password);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }

        static void Main(string[] args)
        {
            MainAsync(args).Wait();
        }
    }
}
