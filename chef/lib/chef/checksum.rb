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
require 'chef/checksum/storage'
require 'uuidtools'

class Chef
  # == Chef::Checksum
  # Checksum for an individual file; e.g., used for sandbox/cookbook uploading
  # to track which files the system already manages.
  class Checksum
    attr_accessor :checksum, :create_time
    attr_accessor :id

    attr_reader :storage

    # When a Checksum commits a sandboxed file to its final home in the checksum
    # repo, this attribute will have the original on-disk path where the file
    # was stored; it will be used if the commit is reverted to restore the sandbox
    # to the pre-commit state.
    attr_reader :original_committed_file_location

    DB = Chef::DB.new(nil, "checksum")
    
    # Creates a new Chef::Checksum object.
    # === Arguments
    # checksum::: the MD5 content hash of the file
    #
    # === Returns
    # object<Chef::Checksum>:: Duh. :)
    def initialize(checksum=nil)
      @create_time = Time.now.iso8601
      @checksum = checksum
      @original_committed_file_location = nil
      @storage = Storage::Filesystem.new(Chef::Config.checksum_path, checksum)
    end

    def to_json_obj
      {
        :checksum => checksum,
        :create_time => create_time,
        :json_class => self.class.name,
        :name => checksum
      }
    end
    
    def to_json(*a)
      to_json_obj.to_json(*a)
    end

    def self.json_create(o)
      checksum = new(o['checksum'])
      checksum.create_time = o['create_time']

      if o.has_key?("_id")
        checksum.id = o["_id"]
        o.delete("_id")
      end
      checksum
    end

    # Moves the given +sandbox_file+ into the checksum repo using the path
    # given by +file_location+ and saves the Checksum to the database
    def commit_sandbox_file(sandbox_file)
      @original_committed_file_location = sandbox_file
      Chef::Log.info("Commiting sandbox file: move #{sandbox_file} to #{@storage}")
      @storage.commit(sandbox_file)
      cdb_save
    end

    # Moves the checksum file back to its pre-commit location and deletes
    # the checksum object from the database, effectively undoing +commit_sandbox_file+.
    # Raises Chef::Exceptions::IllegalChecksumRevert if the original file location
    # is unknown, which is will be the case if commit_sandbox_file was not
    # previously called
    def revert_sandbox_file_commit
      unless original_committed_file_location
        raise Chef::Exceptions::IllegalChecksumRevert, "Checksum #{self.inspect} cannot be reverted because the original sandbox file location is not known"
      end

      Chef::Log.warn("Reverting sandbox file commit: moving #{@storage} back to #{original_committed_file_location}")
      @storage.revert(original_committed_file_location)
      cdb_destroy
    end

    # Removes the on-disk file backing this checksum object, then removes it
    # from the database
    def purge
      purge_file
      cdb_destroy
    end

    ##
    # DB
    ##

    def self.cdb_list(inflate=false, db=nil)
      db ||= DB

      # TODO: confirm if not showing _id is really the desired behavior
      opt = 
        if inflate then
          {}
        else
          { :fields => { :checksum => true, :_id => false }}
        end

      db.list(opt)
    end
    
    def self.cdb_all_checksums(db = nil)
      cursor = (db || DB).list({})

      hash_result = {}
      cursor.each do |doc|
        hash_result[doc["key"]] = 1
      end
    end

    def self.cdb_load(checksum, db=nil)
      (db || DB).load(checksum)
    end

    def cdb_destroy(db=nil)
      (db || DB).delete(checksum)
    end

    def cdb_save(db=nil)
      (db || Chef::DB.new).store(to_json_obj)
    end


    private

    def purge_file
      @storage.purge
    end

  end
end
