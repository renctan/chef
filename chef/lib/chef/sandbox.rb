#
# Author:: Tim Hinderliter (<tim@opscode.com>)
# Copyright:: Copyright (c) 2010 Opscode, Inc.
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

require 'chef/log'
require 'uuidtools'

class Chef
  class Sandbox
    # Notes: guid is now id

    attr_accessor :is_completed, :create_time
    alias_method :is_completed?, :is_completed
    attr_reader :guid
    
    alias :name :guid
    
    attr_accessor :db

    # list of checksum ids
    attr_accessor :checksums
    
    # Creates a new Chef::Sandbox object.  
    def initialize(guid=nil)
      @guid = guid || UUIDTools::UUID.random_create.to_s.gsub(/\-/,'').downcase
      @is_completed = false
      @create_time = Time.now.iso8601
      @checksums = Array.new
    end

    def self.get_default_db
      Chef::DB.new(nil, "sandbox")
    end

    def include?(checksum)
      @checksums.include?(checksum)
    end

    alias :member? :include?

    def to_json_obj(*a)
      {
        :guid => guid,
        :name => name,   # same as guid, used for id_map
        :checksums => checksums,
        :create_time => create_time,
        :is_completed => is_completed,
        :json_class => self.class.name,
      }
    end

    def to_json(*a)
      result = to_json_obj.merge({ :chef_type => "sandbox" })
      result.to_json(*a)
    end

    def self.json_create(o)
      sandbox = new(o['guid'])
      sandbox.checksums = o['checksums']
      sandbox.create_time = o['create_time']
      sandbox.is_completed = o['is_completed']

      if o.has_key?("_id")
        sandbox.id = o["_id"]
        o.delete("_id")
      end
      sandbox
    end

    def self.cdb_list(inflate=false, db=nil)
      db ||= get_default_db

      # TODO: confirm if not showing _id is really the desired behavior
      opt = 
        if inflate then
          {}
        else
          { :fields => { :name => true, :_id => false }}
        end

      db.list(inflate)
    end

    def self.cdb_load(guid, db=nil)
      (db || Sandbox::get_default_db).load(guid)
    end

    def cdb_destroy
      (db || Sandbox::get_default_db).delete(guid)
    end

    def cdb_save(db=nil)
      (db || Sandbox::get_default_db).store(guid, to_json_obj.merge(:_id => guid))
    end
  end
end
