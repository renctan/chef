require 'chef/node'
require 'mongo'

class Chef
  # Originally the CouchDB class. CouchDB databases directly map to one collection of a
  # MongoDB database.
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

    # Finds an object with the given chef_type and name from the database.
    #
    # === Returns
    # Object or a Hash
    #
    # === Raises
    # Chef::Exceptions::CouchDBNotFound if no match was found.
    def find_by_name(chef_type, name)
      result = @coll.find_one({ :chef_type => chef_type, :name => name })

      if result.nil? then
        raise Chef::Exceptions::CouchDBNotFound, "Cannot find #{chef_type} #{name} in DB!"
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

    # Saves the object to db. Add to index if the object supports it.
    #
    # === Returns
    # The _id of the object
    def store(chef_type, name, object)
      validate(
        {
          :obj_type => chef_type,
          :name => name,
          :object => object,
        },
        {
          :object => { :respond_to => :to_json },
        }
      )

      query_selector = { :chef_type => chef_type, :name => name }
      @coll.update(query_selector, object, :upsert => true)
      id = @coll.find_one(query_selector)["_id"]

      if object.respond_to?(:add_to_index)
        Chef::Log.info("Sending #{chef_type}(#{id}) to the index queue for addition.")
        object.add_to_index(:database => database, :id => id, :type => chef_type)
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

    def delete(chef_type, name)
      validate(
        {
          :obj_type => chef_type,
          :name => name,
        },
        {
          :obj_type => { :kind_of => String },
          :name => { :kind_of => String },
        }
      )

      object = find_by_name(chef_type, name)
      @coll.remove({ :chef_type => chef_type, :name => name })

      if object.respond_to?(:delete_from_index)
        Chef::Log.info("Sending #{chef_type}(#{id}) to the index queue for deletion..")
        object.delete_from_index(:database => database, :id => object["_id"], :type => chef_type)
      end
    end

    # TODO: determine whether BSON::ObjectId() wrapping is needed
    # TODO: determine whether it makes sense to just return the cursor to 
    #   potentially conserve memory and improve latency
    def bulk_get(*to_fetch)
      @coll.find({ "_id" => { "$in" => to_fetch.flatten } }).to_a
    end

    # Not needed in Mongo: 
    # create_db, create_design_document, create_id_map

    # Note: find replaces CouchDB#get_view, CouchDB#list

    # Unused, other than in tests:
    # view_uri, server_stats, db_stats
  end
end

