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

require File.expand_path(File.join(File.dirname(__FILE__), "..", "spec_helper"))

describe Chef::OpenIDRegistration, "initialize" do
  it "should return a new Chef::OpenIDRegistration object" do
    Chef::OpenIDRegistration.new.should be_kind_of(Chef::OpenIDRegistration)
  end
end

describe Chef::OpenIDRegistration, "set_password" do
  it "should generate a salt for this object" do
    oreg = Chef::OpenIDRegistration.new
    oreg.salt.should eql(nil)
    oreg.set_password("foolio")
    oreg.salt.should_not eql(nil)
  end
  
  it "should encrypt the password with the salt and the plaintext password" do
    oreg = Chef::OpenIDRegistration.new
    oreg.set_password("foolio")
    oreg.password.should_not eql(nil)
  end
end

describe Chef::OpenIDRegistration, "to_json" do
  it "should serialize itself as json" do
    oreg = Chef::OpenIDRegistration.new
    oreg.set_password("monkey")
    json = oreg.to_json
    %w{json_class name salt password validated}.each do |verify|
      json.should =~ /#{verify}/
    end
  end
end

describe Chef::OpenIDRegistration, "from_json" do
  it "should serialize itself as json" do
    oreg = Chef::OpenIDRegistration.new()
    oreg.name = "foobar"
    oreg.set_password("monkey")
    oreg_json = oreg.to_json
    nreg = Chef::JSONCompat.from_json(oreg_json)
    nreg.should be_a_kind_of(Chef::OpenIDRegistration)
    %w{name salt password validated}.each do |verify|
      nreg.send(verify.to_sym).should eql(oreg.send(verify.to_sym))
    end
  end
end

describe Chef::OpenIDRegistration, "list" do  
  before(:each) do
    @mock_db = mock("Chef::DB")
    @mock_db.stub!(:list)
    Chef::DB.stub!(:new).and_return(@mock_db)
  end
  
  it "should retrieve a list of nodes from DB" do
    @mock_db.should_receive(:list).with({ :fields => { :name => true, :_id => false }})
    Chef::OpenIDRegistration.list
  end
  
  it "should return just the ids if inflate is false" do
    @mock_db.should_receive(:list).with({ :fields => { :name => true, :_id => false }})
    Chef::OpenIDRegistration.list(false)
  end
  
  it "should return the full objects if inflate is true" do
    @mock_db.should_receive(:list).with({})
    Chef::OpenIDRegistration.list(true)
  end
end

describe Chef::OpenIDRegistration, "load" do
  it "should load a registration from db by name" do
    @mock_db = mock("Chef::DB")
    Chef::DB.stub!(:new).and_return(@mock_db)
    @mock_db.should_receive(:load).with("coffee")
    Chef::OpenIDRegistration.load("coffee")
  end
end

describe Chef::OpenIDRegistration, "destroy" do
  it "should delete this registration from db" do
    @mock_db = mock("Chef::DB")
    @mock_db.should_receive(:delete).with("bob")
    Chef::DB.stub!(:new).and_return(@mock_db)
    reg = Chef::OpenIDRegistration.new
    reg.name = "bob"
    reg.destroy
  end
end

describe Chef::OpenIDRegistration, "save" do
  before(:each) do
    @mock_db = mock("Chef::DB")
    Chef::DB.stub!(:new).and_return(@mock_db)
    @reg = Chef::OpenIDRegistration.new
    @reg.name = "bob"
  end
  
  it "should save the registration to db" do
    @mock_db.should_receive(:store).with(@reg.to_json_obj)
    @reg.save
  end
end

describe Chef::OpenIDRegistration, "has_key?" do
  it "should check with DB for a registration with this key" do
    @mock_db = mock("Chef::DB")
    @mock_db.should_receive(:has_key?).with("bob")
    Chef::DB.stub!(:new).and_return(@mock_db)
    Chef::OpenIDRegistration.has_key?("bob")
  end
end

