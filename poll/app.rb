require "uri"
require "net/http"
require 'digest'
require 'json'
require 'aws-sdk-dynamodb'

DDB_TABLE = 'investec-poller-InvestectTransactions-1S32PO69M23II'

INVESTEC = {
  client_id:      ENV['INVESTEC_CLIENT_ID'],
  secret:         ENV['INVESTEC_SECRET'],
  account_number: ENV['INVESTEC_ACCOUNT_NUMBER'],
}


def lambda_handler(event:, context:)
  transactions(INVESTEC).each { |tx| push_to_ddb(tx) }
  true
end

def push_to_ddb(tx)

  item   = tx.merge(id: Digest::MD5.hexdigest(tx.to_json))

  params = {
    table_name:                    DDB_TABLE,
    condition_expression:          'attribute_not_exists(id)',
    item:                          item,
  }

  puts params
  dynamodb = Aws::DynamoDB::Client.new
  dynamodb.put_item(params)
  
end


def transactions(config)
  token    = access_token(config[:client_id], config[:secret])
  accounts = accounts(token)
  account_id = accounts.select { |account| account['accountNumber']==config[:account_number] }.first['accountId']
  download_transactions(token, account_id)
end

def download_transactions(token, account_id)
  url                      = URI("https://openapi.investec.com/za/pb/v1/accounts/#{account_id}/transactions")
  https                    = Net::HTTP.new(url.host, url.port);
  https.use_ssl            = true
  request                  = Net::HTTP::Get.new(url)
  request["Content-Type"]  = "application/x-www-form-urlencoded"
  request["Authorization"] = "Bearer #{token}"
  response                 = https.request(request)
  JSON.parse(response.read_body)['data']['transactions']
end

def accounts(token)
  url           = URI("https://openapi.investec.com/za/pb/v1/accounts")
  https         = Net::HTTP.new(url.host, url.port);
  https.use_ssl = true
  request       = Net::HTTP::Get.new(url)
  request["Content-Type"] = "application/x-www-form-urlencoded"
  request["Authorization"] = "Bearer #{token}"
  response = https.request(request)
  JSON.parse(response.read_body)['data']['accounts']
end

def access_token(client_id, secret)
  url                     = URI("https://openapi.investec.com/identity/v2/oauth2/token")
  https                   = Net::HTTP.new(url.host, url.port);
  https.use_ssl           = true
  request                 = Net::HTTP::Post.new(url)
  request["Content-Type"] = "application/x-www-form-urlencoded"
  request.body            = "grant_type=client_credentials&scope=accounts"
  request.basic_auth(client_id, secret)
  JSON.parse(https.request(request).read_body)['access_token']
end