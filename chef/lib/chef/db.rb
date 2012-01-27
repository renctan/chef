require 'chef/node'
require 'chef/params_validate'
require 'mongo'
require 'forwardable'

class Chef
  # Originally the CouchDB class. CouchDB databases directly map to one collection of a
  # MongoDB collection. Instead of having a single monolithic database from the previous
  # design, each "major views" are mapped to a single Mongo collection. The "obj_type"
  # parameter of the old methods are replaced by passing the db_name at the constructor.
  # Data being sent to the Solr indexer is still the same as the original.
  #
  # TODO: decide how to handle conn error
  # TODO: Decide whether UUIDTools is **REALLY** needed or not since Mongo Ruby creates unique _id
  class DB
    include Chef::Mixin::ParamsValidate

    extend Forwardable

    # Note: find replaces CouchDB#get_view, CouchDB#list
    def_delegators :@coll, :find, :find_one

    MAX_RETRY_ATTEMPTS = Chef::Config[:http_retry_count]
    RETRY_DELAY = Chef::Config[:http_retry_delay]

    def initialize(url, collection_name, opts = {})
      @db_name = db_name

      url ||= Chef::Config[:couchdb_url]
      host, port = url.split(":")
      @db = Mongo::DB.new(Chef::Config[:couchdb_database],
                          Mongo::Connection.new(host, port), opts)
      @coll = @db[collection_name]
    end

    # TODO: Should return namespace (ie, dbname + collname) instead?
    def database(arg=nil)
      @db = args || @db
      @db.name
    end

    # Saves the object to db. Add to index if the object supports it.
    #
    # === Returns
    # The _id of the object
    def store(object)
      name = object["name"]

      query_selector = { :name => name }
      @coll.update(query_selector, object, :upsert => true)
      id = @coll.find_one(query_selector)["_id"]

      if object.respond_to?(:add_to_index)
        Chef::Log.info("Sending #{@db_name}(#{id}) to the index queue for addition.")
        object.add_to_index(:database => database, :id => id, :type => @db_name)
      end

      id
    end

    # Loads a document from the database
    def load(name)
      validate(
        {
          :name => name,
        },
        {
          :name => { :kind_of => String },
        }
      )

      doc = find_by_name(name)
      # TODO: confirm correct transform of doc.couchdb = self if doc.respond_to?(:couchdb)
      doc.db = self if doc.respond_to?(:db)
      doc
    end

    def delete(name)
      validate(
        {
          :name => name,
        },
        {
          :name => { :kind_of => String },
        }
      )

      object = find_by_name(name)
      @coll.remove({ :name => name })

      if object.respond_to?(:delete_from_index)
        Chef::Log.info("Sending #{@db_name}(#{id}) to the index queue for deletion..")
        object.delete_from_index(:database => database, :id => object["_id"], :type => @db_name)
      end
    end

    # Lists all entries from the database.
    #
    # === Arguments
    # opt::: please refer to the opt parameter of Mongo::Collection.find
    #
    # === Returns
    # The cursor to the result
    def list(opt = {})
      if inflate
        @coll.find
      else
        @coll.find({}, opt)
      end
    end

    def has_key?(name)
      validate(
        {
          :name => name,
        },
        {
          :name => { :kind_of => String },
        }
      )

      begin
        find_by_name(name)
        true
      rescue
        false
      end
    end

    # Finds an object with the given name from the database.
    #
    # === Returns
    # Object or a Hash
    #
    # === Raises
    # Chef::Exceptions::CouchDBNotFound if no match was found.
    def find_by_name(name)
      result = @coll.find_one({ :name => name })

      if result.nil? then
        raise Chef::Exceptions::CouchDBNotFound, "Cannot find #{name} in DB!"
      end

      # TODO: Confirm this is correct
      # Use from_json to deserialize special fields into the original object
      Chef::JSONCompat.from_json(result)
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

