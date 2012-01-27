#
# Author:: Adam Jacob (<adam@opscode.com>)
# Author:: Nuo Yan (<nuo@opscode.com>)
# Author:: Christopher Brown (<cb@opscode.com>)
# Copyright:: Copyright (c) 2009 Opscode, Inc.
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
require 'chef/data_bag_item'
require 'chef/index_queue'
require 'chef/mash'
require 'chef/json_compat'

class Chef
  class DataBag

    include Chef::Mixin::FromFile
    include Chef::Mixin::ParamsValidate
    include Chef::IndexQueue::Indexable

    VALID_NAME = /^[\-[:alnum:]_]+$/

    DB = Chef::DB.new(nil, "data_bag")

    def self.validate_name!(name)
      unless name =~ VALID_NAME
        raise Exceptions::InvalidDataBagName, "DataBags must have a name matching #{VALID_NAME.inspect}, you gave #{name.inspect}"
      end
    end

    attr_accessor :id, :db

    # Create a new Chef::DataBag
    def initialize(db=nil)
      @name = ''
      @id = nil
      @db = (db || DB)
    end

    def name(arg=nil)
      set_or_return(
                    :name,
                    arg,
                    :regex => VALID_NAME
                    )
    end

    def to_hash
      result = to_json_obj
      result["chef_type"] = "data_bag"
      result
    end

    def to_json_obj
      {
        "name" => @name,
        'json_class' => self.class.name,
      }
    end

    # Serialize this object as a hash
    def to_json(*a)
      to_hash.to_json(*a)
    end

    def chef_server_rest
      Chef::REST.new(Chef::Config[:chef_server_url])
    end

    def self.chef_server_rest
      Chef::REST.new(Chef::Config[:chef_server_url])
    end

    # Create a Chef::Role from JSON
    def self.json_create(o)
      bag = new
      bag.name(o["name"])
      bag.id = o["_id"] if o.has_key?("_id")
      bag.index_id = bag.id
      bag
    end

    # List all the Chef::DataBag objects in the DB.  If inflate is set to true, you will get
    # the full list of all Roles, fully inflated.
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
        # TODO: Check why this is needed -> docs are not embedded, but scattered?
        # Can't search for all data bags like other objects, fall back to N+1 :(
        list(false).inject({}) do |response, bag_and_uri|
          response[bag_and_uri.first] = load(bag_and_uri.first)
          response
        end
      else
        Chef::REST.new(Chef::Config[:chef_server_url]).get_rest("data")
      end
    end

    # Load a Data Bag by name from DB
    def self.cdb_load(name, db=nil)
      (db || DB).load(name)
    end

    # Load a Data Bag by name via either the RESTful API or local data_bag_path if run in solo mode
    def self.load(name)
      if Chef::Config[:solo]
        unless File.directory?(Chef::Config[:data_bag_path])
          raise Chef::Exceptions::InvalidDataBagPath, "Data bag path '#{Chef::Config[:data_bag_path]}' is invalid"
        end

        Dir.glob(File.join(Chef::Config[:data_bag_path], name, "*.json")).inject({}) do |bag, f|
          item = JSON.parse(IO.read(f))
          bag[item['id']] = item
          bag
        end
      else
        Chef::REST.new(Chef::Config[:chef_server_url]).get_rest("data/#{name}")
      end
    end

    # Remove this Data Bag from DB
    def cdb_destroy
      @db.delete(@name)
      
      # TODO: check why there can be multiple results, and why it needs to be deleted recursively
      #      rs["rows"].each do |row|
      #        row["doc"].couchdb = couchdb
      #        row["doc"].cdb_destroy
      #      end
    end

    def destroy
      chef_server_rest.delete_rest("data/#{@name}")
    end

    # Save this Data Bag to the DB
    def cdb_save
      @db.store(to_json_obj)
    end

    # Save the Data Bag via RESTful API
    def save
      begin
        chef_server_rest.put_rest("data/#{@name}", self)
      rescue Net::HTTPServerException => e
        raise e unless e.response.code == "404"
        chef_server_rest.post_rest("data", self)
      end
      self
    end

    #create a data bag via RESTful API
    def create
      chef_server_rest.post_rest("data", self)
      self
    end

    # List all the items in this Bag from DB
    # The self.load method does this through the REST API
    #
    # === Returns
    # TODO: flatten raw_data.id
    # cursor to the result. Note that if inflate == false, only the raw_data.id
    # field will be returned and this is nested.
    def list(inflate=false)
      # TODO: confirm if not showing _id is really the desired behavior
      opt = 
        if inflate then
          {}
        else
          { :fields => { "raw_data.id" => true, :_id => false }}
        end

      DB.list(opt)
    end

    # As a string
    def to_s
      "data_bag[#{@name}]"
    end

  end
end

