require 'chef/node'
require 'mongo'

class Chef
  # Originally CouchDB class
  #
  # TODO: decide how to handle conn error
  # TODO: Decide whether UUIDTools is **REALLY** needed or not since Mongo Ruby creates unique _id
  class DB
    include Chef::Mixin::ParamsValidate

    extend forwardable

    # Note: find replaces CouchDB#get_view, CouchDB#list
    def_delegators :@coll, :find, :find_one

    COLLECTION_NAME = "config"
    MAX_RETRY_ATTEMPTS = Chef::Config[:http_retry_count]
    RETRY_DELAY = Chef::Config[:http_retry_delay]

    def initialize(url, db_name, opts = {})
      url ||= Chef::Config[:couchdb_url]
      host, port = url.split(":")
      db_name ||= Chef::Config[:couchdb_database]
      @db = Mongo::DB.new(db_name, Mongo::Connection.new(host, port), opts)
      @coll = @db[COLLECTION_NAME]
    end

    # TODO: Should return namespace (ie, dbname + collname) instead?
    def database(arg=nil)
      @db = args || @db
      @db.name
    end

    def find_by_name(obj_type, name)
      result = find_one({obj_type => name})

      if result.nil? then
        raise Chef::Exceptions::CouchDBNotFound, "Cannot find #{obj_type} #{name} in DB!"
      end

      # TODO: Confirm this is correct
      # Use from_json to deserialize special fields into the original object
      Chef::JSONCompat.from_json(result)
    end

    def has_key?(obj_type, name)
      validate(
        {
          :obj_type => obj_type,
          :name => name,
        },
        {
          :obj_type => { :kind_of => String },
          :name => { :kind_of => String },
        }
      )

      begin
        find_by_name(obj_type, name)
        true
      rescue
        false
      end
    end

    # Save the object to db. Add to index if the object supports it.
    #
    # Returns the _id of the object
    def store(obj_type, name, object)
      validate(
        {
          :obj_type => obj_type,
          :name => name,
          :object => object,
        },
        {
          :object => { :respond_to => :to_json },
        }
      )

      id = @coll.update({ obj_type => name }, object, :upsert => true)

      if object.respond_to?(:add_to_index)
        Chef::Log.info("Sending #{obj_type}(#{id}) to the index queue for addition.")
        object.add_to_index(:database => database, :id => id, :type => obj_type)
      end

      id
    end

    def load(obj_type, name)
      validate(
        {
          :obj_type => obj_type,
          :name => name,
        },
        {
          :obj_type => { :kind_of => String },
          :name => { :kind_of => String },
        }
               )
      doc = find_by_name(obj_type, name)
      # TODO: confirm correct transform of doc.couchdb = self if doc.respond_to?(:couchdb)
      doc.db = self if doc.respond_to?(:db)
      doc
    end

    def delete(obj_type, name)
      validate(
        {
          :obj_type => obj_type,
          :name => name,
        },
        {
          :obj_type => { :kind_of => String },
          :name => { :kind_of => String },
        }
      )

      object = find_by_name(obj_type, name)
      @coll.remove({ obj_type => name })

      if object.respond_to?(:delete_from_index)
        Chef::Log.info("Sending #{obj_type}(#{id}) to the index queue for deletion..")
        object.delete_from_index(:database => database, :id => object["_id"], :type => obj_type)
      end
    end

    # TODO: determine whether BSON::ObjectId() wrapping is needed
    def bulk_get(*to_fetch)
      @coll.find({ "_id" => { "$in" => to_fetch } }).to_a
    end

    # Not needed in Mongo: 
    # create_db, create_design_document, create_id_map

    # Unused, other than in tests:
    # view_uri, server_stats, db_stats
  end
end

