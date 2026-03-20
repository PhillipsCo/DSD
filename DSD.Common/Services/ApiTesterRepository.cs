using DSD.Common.Models;
using Newtonsoft.Json;
using RestSharp;
using System;
using System.Threading.Tasks;

namespace DSD.Common.Services
{
    public class ApiTesterRepository
    {
        public async Task<TokenInfo> GetAccessTokenAsync(
            string url,
            string grantType,
            string clientId,
            string clientSecret,
            string scope)
        {
            var client = new RestClient();

            var request = new RestRequest(url, Method.Post);

            // OAuth token endpoints expect form-url-encoded
            request.AddHeader("Content-Type", "application/x-www-form-urlencoded");

            // ✅ Correct OAuth parameter names
            request.AddParameter("grant_type", grantType);
            request.AddParameter("client_id", clientId);
            request.AddParameter("client_secret", clientSecret);
            request.AddParameter("scope", scope);

            var response = await client.ExecuteAsync(request);

            if (!response.IsSuccessful || string.IsNullOrWhiteSpace(response.Content))
            {
                throw new InvalidOperationException(
                    $"Token request failed. Status: {(int)response.StatusCode} {response.StatusDescription}");
            }

            var token = JsonConvert.DeserializeObject<TokenInfo>(response.Content);

            if (token == null || string.IsNullOrWhiteSpace(token.access_token))
            {
                throw new InvalidOperationException("Token response was invalid or missing access_token.");
            }

            return token;
        }
    }
}