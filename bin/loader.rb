#!/usr/bin/env ruby

require 'rubygems'

# Load helper libraries
['utils', 'socrata'].each do |lib|
  require File.join(File.dirname(__FILE__), "../lib/#{lib}")
end

# We take one argument, which is our config file
config = YAML.load_file(ARGV[0]).recursively_symbolize_keys!

if !config[:pre_command].nil?
  # Run our pre-command if we have one
  output = `#{config[:pre_command]}`
  if $?.success?
    Logger.debug "Pre-command ran successfully: #{output}"
  else
    Logger.critical "Error running pre-command: #{output}"
    exit 1
  end
end

# Create our client
socrata = Socrata.new(config[:socrata][:username],
                      config[:socrata][:password],
                      config[:socrata][:app_token],
                      config[:socrata][:domain])

# Use the publishing workflow (http://dev.socrata.com/publisher/workflow)
Logger.debug "Creating draft copy via #{config[:method] == "replace" ? "copySchema" : "copy"}..."
draft_copy = socrata.post("/views/#{config[:uid]}/publication.json?method=#{config[:method] == "replace" ? "copySchema" : "copy"}")
while(draft_copy.code == 202)
  Logger.debug "Waiting for draft copy..."
  sleep config[:delay]
  draft_copy = socrata.post("/views/#{config[:uid]}/publication.json?method=#{config[:method] == "replace" ? "copySchema" : "copy"}")
end

if draft_copy["id"].nil?
  Logger.critical "It doesn't look like we got back a valid draft copy: #{draft_copy.inspect}"
  exit 1
else
  Logger.debug "Created draft copy via #{config[:method] == "replace" ? "copySchema" : "copy"} with UID #{draft_copy["id"]}"
end

# Scan our file
Logger.debug "Uploading and scanning #{config[:filename]} (#{File.size(config[:filename])} bytes)"
file = socrata.post("/imports2?method=scan", :query => {
  :file => File.new(config[:filename])
})
if file["fileId"].nil?
  Logger.critical "Received an error while scanning the file for replacement: #{file}"
  exit 1
else
  Logger.debug "Got fileId #{file["fileId"]} for #{config[:filename]}"
end

# Perform our replace or append
params = {
  :name => File.basename(config[:filename]),
  :skip => config[:skip],
  :viewUid => draft_copy["id"],
  :fileId => file["fileId"]
}.reject { |k,v| v.nil? }.collect {|k,v| "#{k}=#{v}"}.join "&"

Logger.debug "Kicking off #{config[:method]}"
results = socrata.form_post("/imports2?method=#{config[:method]}&#{params}")
if results["id"]
  # We already got an updated dataset, it must have been small
  Logger.debug "Update already complete for #{results["id"]}"
elsif !results["ticket"].nil?
  # We've been sidelined, stay around until we're done
  ticket = results["ticket"]

  Logger.debug "Waiting on ticket #{ticket}"
  while(results["id"].nil?)
    sleep config[:delay] || 60
    results = socrata.get("/imports2.json?ticket=#{ticket}")
    Logger.debug "Still waiting on #{ticket}"
  end

  Logger.debug "Replace operation complete for http://#{config[:domain]}/d/#{draft_copy["id"]}}"
else
  # Something has gone awry!
  Logger.critical "Something went wrong with the append or replace: #{results}"
  exit 1
end

# Publish the resulting updates
if config[:publish]
  results = socrata.post("/views/#{draft_copy["id"]}/publication.json")
  if results["id"] != config[:uid]
    Logger.critical "Something went wrong in publication, UIDs don't match: #{results}"
  else
    Logger.debug "Publication was successful for http://#{config[:socrata][:domain]}/d/#{config[:uid]}!"
  end
end
