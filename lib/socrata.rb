require "rubygems"
require "httparty"
require "json"
require 'httmultiparty'

class Socrata
  include HTTParty
  include HTTMultiParty
  #debug_output $stderr

  attr_reader :batching

  def initialize(username, password, app_token, domain = "opendata.socrata.com")
    self.class.base_uri "https://#{domain}/api"
    if username && password
      self.class.basic_auth username, password
    end

    self.class.headers "X-App-Token" => app_token, "Content-type" => "application/json"

    # For batching
    @batching = false
    @batch_queue = []
  end

  def get(url)
    if @batching
      @batch_queue << {:url => url, :requestType => "GET"}
    else
      response = self.class.get(url)
      check_response(response)
      return response
    end
  end

  def post(url, params = {})
    if @batching
      @batch_queue << {:url => url, :body => params[:body], :requestType => "POST"}
    else
      response = self.class.post(url, params)
      check_response(response)
      return response
    end
  end

  def form_post(url, params = {})
    params[:headers] = self.class.headers.merge("Content-type" => "multipart/form-data")
    response = self.class.post(url, params)
    check_response(response)
    return response
  end

  def put(url, params = {})
    if @batching
      @batch_queue << {:url => url, :body => params[:body], :requestType => "PUT"}
    else
      response = self.class.put(url, params)
      check_response(response)
      return response
    end
  end

  def delete(url)
    if @batching
      @batch_queue << {:url => url, :requestType => "DELETE"}
    else
      response = self.class.delete(url)
      check_response(response)
      return response
    end
  end

  def batch()
    @batching = true
    @batch_queue = []
    yield
    @batching = false
    flush_batch_queue();
  end

  protected
    def check_response(response)
      if response.code != 200 && response.code != 202
        raise "Error calling SODA API: #{response.message}"
      end
    end

    def flush_batch_queue
      if !@batch_queue.empty?
        result = self.post('/batches', :body => {:requests => @batch_queue}.to_json)
        results_parsed = JSON.parse(result.body)
        if results_parsed.is_a? Array
          results_parsed.each_with_index do |result, i|
            if result['error']
              raise "Received error in batch response for operation " +
                @batch_queue[i][:requestType] + " " + @batch_queue[i][:url] + ". Error: " +
                result['errorCode'] + " - " + result['errorMessage']
            end
          end
        else
          raise "Expected array response from a /batches request, and didn't get one."
        end
        @batch_queue.clear
      end
      return results_parsed
    end
end
