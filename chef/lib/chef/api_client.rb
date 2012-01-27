#
# Author:: Adam Jacob (<adam@opscode.com>)
# Author:: Nuo Yan (<nuo@opscode.com>)
# Copyright:: Copyright (c) 2008 Opscode, Inc.
# License:: Apache License, Version 2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'chef/config'
require 'chef/mixin/params_validate'
require 'chef/mixin/from_file'
require 'chef/db'
require 'chef/certificate'
require 'chef/index_queue'
require 'chef/mash'
require 'chef/json_compat'
require 'chef/search/query'

class Chef
  class ApiClient

    include Chef::Mixin::FromFile
    include Chef::Mixin::ParamsValidate
    include Chef::IndexQueue::Indexable

    INDEX_OBJECT_TYPE = 'client'.freeze

    def self.index_object_type
      INDEX_OBJECT_TYPE
    end

    attr_accessor :id, :db

    DB = Chef::DB.new(nil, "client")

    # Create a new Chef::ApiClient object.
    def initialize(db=nil)
      @name = ''
      @public_key = nil
      @private_key = nil
      @id = nil
      @admin = false
      @db = (db || DB)
    end

    # Gets or sets the client name.
    #
    # @params [Optional String] The name must be alpha-numeric plus - and _.
    # @return [String] The current value of the name.
    def name(arg=nil)
      set_or_return(
        :name,
        arg,
        :regex => /^[\-[:alnum:]_\.]+$/
      )
    end

    # Gets or sets whether this client is an admin.
    #
    # @params [Optional True/False] Should be true or false - default is false.
    # @return [True/False] The current value
    def admin(arg=nil)
      set_or_return(
        :admin,
        arg,
        :kind_of => [ TrueClass, FalseClass ]
      )
    end

    # Gets or sets the public key.
    #
    # @params [Optional String] The string representation of the public key.
    # @return [String] The current value.
    def public_key(arg=nil)
      set_or_return(
        :public_key,
        arg,
        :kind_of => String
      )
    end

    # Gets or sets the private key.
    #
    # @params [Optional String] The string representation of the private key.
    # @return [String] The current value.
    def private_key(arg=nil)
      set_or_return(
        :private_key,
        arg,
        :kind_of => String
      )
    end

    # Creates a new public/private key pair, and populates the public_key and
    # private_key attributes.
    #
    # @return [True]
    def create_keys
      results = Chef::Certificate.gen_keypair(self.name)
      self.public_key(results[0].to_s)
      self.private_key(results[1].to_s)
      true
    end

    # The hash representation of the object.  Includes the name and public_key,
    # but never the private key.
    #
    # @return [Hash]
    def to_hash
      h = to_json_obj
      h["chef_type"] = "client"
      h
    end

    def to_json_obj
      {
        "name" => @name,
        "public_key" => @public_key,
        "admin" => @admin,
        'json_class' => self.class.name,
        "chef_type" => "client"
      }
    end

    # The JSON representation of the object.
    #
    # @return [String] the JSON string.
    def to_json(*a)
      to_hash.to_json(*a)
    end

    def self.json_create(o)
      client = Chef::ApiClient.new
      client.name(o["name"] || o["clientname"])
      client.public_key(o["public_key"])
      client.admin(o["admin"])
      client.id = o["_id"]
      client.index_id = client.id
      client
    end

    # List all the Chef::ApiClient objects in the DB.  If inflate is set
    # to true, you will get the full list of all ApiClients, fully inflated.
    def self.cdb_list(inflate=false, db=nil)
      db ||= DB

      # TODO: confirm if not showing _id is really the desired behavior
      opt = 
        if inflate then
          {}
        else
          { :fields => { :name => true, :_id => false }}
        end

      db.list(opt)
    end

    def self.list(inflate=false)
      if inflate
        response = Hash.new
        Chef::Search::Query.new.search(:client) do |n|
          n = self.json_create(n) if n.instance_of?(Hash)
          response[n.name] = n
        end
        response
      else
        Chef::REST.new(Chef::Config[:chef_server_url]).get_rest("clients")
      end
    end

    # Load a client by name from DB
    #
    # @params [String] The name of the client to load
    # @return [Chef::ApiClient] The resulting Chef::ApiClient object
    def self.cdb_load(name, db=nil)
      (db || DB).load(name)
    end

    # Load a client by name via the API
    def self.load(name)
      response = Chef::REST.new(Chef::Config[:chef_server_url]).get_rest("clients/#{name}")
      if response.kind_of?(Chef::ApiClient)
        response
      else
        client = Chef::ApiClient.new
        client.name(response['clientname'])
        client
      end
    end

    # Remove this client from the DB
    #
    # @params [String] The name of the client to delete
    # @return [Chef::ApiClient] The last version of the object
    def cdb_destroy
      @db.delete(@name)
    end

    # Remove this client via the REST API
    def destroy
      Chef::REST.new(Chef::Config[:chef_server_url]).delete_rest("clients/#{@name}")
    end

    # Save this client to the DB
    def cdb_save
      @db.store(to_json_obj)
    end

    # Save this client via the REST API, returns a hash including the private key
    def save(new_key=false, validation=false)
      if validation
        r = Chef::REST.new(Chef::Config[:chef_server_url], Chef::Config[:validation_client_name], Chef::Config[:validation_key])
      else
        r = Chef::REST.new(Chef::Config[:chef_server_url])
      end
      # First, try and create a new registration
      begin
        r.post_rest("clients", {:name => self.name, :admin => self.admin })
      rescue Net::HTTPServerException => e
        # If that fails, go ahead and try and update it
        if e.response.code == "409"
          r.put_rest("clients/#{name}", { :name => self.name, :admin => self.admin, :private_key => new_key })
        else
          raise e
        end
      end
    end

    # Create the client via the REST API
    def create
      Chef::REST.new(Chef::Config[:chef_server_url]).post_rest("clients", self)
    end

    # As a string
    def to_s
      "client[#{@name}]"
    end

  end
end

