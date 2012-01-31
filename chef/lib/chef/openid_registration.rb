#
# Author:: Adam Jacob (<adam@opscode.com>)
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
require 'chef/db'
require 'chef/index_queue'
require 'digest/sha1'
require 'chef/json_compat'

class Chef
  class OpenIDRegistration
    
    attr_accessor :name, :salt, :validated, :password, :admin
    
    include Chef::Mixin::ParamsValidate
    include Chef::IndexQueue::Indexable
    
    # Create a new Chef::OpenIDRegistration object.
    def initialize()
      @name = nil
      @salt = nil
      @password = nil
      @validated = false
      @admin = false
      @db = OpenIDRegistration::get_default_db
    end

    def self.get_default_db
      Chef::DB.new(nil, "openid_registration")
    end

    def name=(n)
      @name = n.gsub(/\./, '_')
    end
    
    # Set the password for this object.
    def set_password(password) 
      @salt = generate_salt
      @password = encrypt_password(@salt, password)      
    end
    
    def to_json_obj
      {
        'name' => @name,
        'json_class' => self.class.name,
        'salt' => @salt,
        'password' => @password,
        'validated' => @validated,
        'admin' => @admin,
      }
    end

    # Serialize this object as a hash 
    def to_json(*a)
      to_json_obj.to_json(*a)
    end
    
    # Create a Chef::Node from JSON
    def self.json_create(o)
      me = new
      me.name = o["name"]
      me.salt = o["salt"]
      me.password = o["password"]
      me.validated = o["validated"]
      me.admin = o["admin"]

      me
    end
    
    # List all the Chef::OpenIDRegistration objects in the DB.  If inflate is set to true, you will get
    # the full list of all registration objects.  Otherwise, you'll just get the IDs
    def self.list(inflate=false)
      # TODO: confirm if not showing _id is really the desired behavior
      opt = 
        if inflate then
          {}
        else
          { :fields => { :name => true, :_id => false }}
        end

      get_default_db.list(opt)
    end
    
    def self.cdb_list(*args)
      list(*args)
    end
    
    # Load an OpenIDRegistration by name from DB
    def self.load(name)
      get_default_db.load(name)
    end
    
    # Whether or not there is an OpenID Registration with this key.
    def self.has_key?(name)
      get_default_db.has_key?(name)
    end
    
    # Remove this OpenIDRegistration from the DB
    def destroy
      @db.delete(@name)
    end
    
    # Save this OpenIDRegistration to the DB
    def save
      @db.store(to_json_obj)
    end
    
    protected
    
      def generate_salt
        salt = Time.now.to_s
        chars = ("a".."z").to_a + ("A".."Z").to_a + ("0".."9").to_a
        1.upto(30) { |i| salt << chars[rand(chars.size-1)] }
        salt
      end
    
      def encrypt_password(salt, password)
        Digest::SHA1.hexdigest("--#{salt}--#{password}--")
      end
    
  end
end
