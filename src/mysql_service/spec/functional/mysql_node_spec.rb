# Copyright (c) 2009-2011 VMware, Inc.
require 'spec_helper'
require 'mysql_service/node'
require 'mysql_service/mysql_error'
require 'mysql2'
require 'yajl'
require 'fileutils'


module VCAP
  module Services
    module Mysql
      class Node
        attr_reader :pools, :pool, :logger, :capacity, :provision_served, :binding_served, :use_warden

        # helper methods

        # check whether mysql has required innodb plugin installed.
        def check_innodb_plugin(instance)
          fetch_pool(instance).with_connection do |connection|
            res = connection.query("show tables from information_schema like 'INNODB_TRX'")
            return true if res.count > 0
          end
        rescue Mysql2::Error => e
          @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
          nil
        end

        def is_percona_server?(instance)
          fetch_pool(instance).with_connection do |connection|
            res = connection.query("show variables where variable_name like 'version_comment'")
            return res.count > 0 && res.to_a[0]["Value"] =~ /percona/i
          end
        rescue Mysql2::Error => e
          @logger.error("MySQL connection failed: [#{e.errno}] #{e.error}")
          nil
        end
      end
    end
  end
end

module VCAP
  module Services
    module Mysql
      class MysqlError
          attr_reader :error_code
      end
    end
  end
end

describe "Mysql server node", components: [:nats], hook: :all do
  include VCAP::Services::Mysql

  PASSWORD = 'default_password'

  before :each do
    @opts = getNodeTestConfig
    @opts.freeze
    @default_plan = "free"
    @default_version = @opts[:default_version]
    @default_opts = {"privileges" => ["FULL"]}
    @tmpfiles = []
    # Setup code must be wrapped in EM.run
    EM.run do
      @node = VCAP::Services::Mysql::Node.new(@opts)
      EM.add_timer(1) { EM.stop }
    end
  end

  def new_instance(node, cred_opts = {}, recreate_service_id = true, plan = nil, properties = {})
    @creds ||= {
        "name"     => 'pooltest',
        "user"     => 'user',
        "password" => PASSWORD

    }

    creds = @creds.merge(cred_opts)
    creds['service_id'] =  UUIDTools::UUID.random_create.to_s if recreate_service_id
    node.provision(plan || @default_plan, creds, @default_version, properties)
  end

  before :each do
    @test_dbs = {}# for cleanup
    # Create one db be default
    @db = new_instance(@node)
    @db.should_not == nil
    @db["service_id"].should be
    @db["name"].should be
    @db["host"].should be
    @db["host"].should == @db["hostname"]
    @db["port"].should be
    @db["user"].should == @db["username"]
    @test_dbs[@db] = []
    @db_instance = @node.mysqlProvisionedService.get(@db["service_id"])
  end


  it "should connect to mysql database" do
    EM.run do
      expect {@node.fetch_pool(@db['service_id']).with_connection{|connection| connection.query("SELECT 1")}}.to_not raise_error
      EM.stop
    end
  end

  it "should report inconsistency between mysql and local db" do
    EM.run do
      name, user = @db["name"], @db["user"]
      @node.fetch_pool(@db["service_id"]).with_connection do |conn|
        conn.query("delete from db where db='#{name}' and user='#{user}'")
      end
      result = @node.check_db_consistency
      result.include?([name, user]).should == true
      EM.stop
    end
  end

  it "should provison a database with correct credential" do
    EM.run do
      @db.should be_instance_of Hash
      conn = connect_to_mysql(@db, PASSWORD)
      expect {conn.query("SELECT 1")}.to_not raise_error
      EM.stop
    end
  end

  it "should be able to update credentials" do
    EM.run do
     new_password = '\'); drop user root@\'%\'; SET PASSWORD FOR \'root\'@\'localhost\' = PASSWORD(\')'

     @node.update_credentials(@db["service_id"], {'password' => new_password }  )
     conn = connect_to_mysql(@db, new_password)
     expect {conn.query("Select 1")}.to_not raise_error
     EM.stop
    end
  end

  it "should calculate both table and index as database size" do
    EM.run do
      @node.fetch_pool(@db["service_id"]).with_connection do |conn|
        conn.query("use #{@db['name']}")
        # should calculate table size
        conn.query("CREATE TABLE test(id INT)")
        conn.query("INSERT INTO test VALUE(10)")
        conn.query("INSERT INTO test VALUE(20)")
        table_size = @node.dbs_size(conn)[@db["name"]]
        table_size.should be > 0
        # should also calculate index size
        conn.query("CREATE INDEX id_index on test(id)")
        # force table status update
        conn.query("analyze table test")
        all_size = @node.dbs_size(conn)[@db["name"]]
        all_size.should > table_size
      end
      EM.stop
    end
  end

  it "should enforce database size quota" do
    EM.run do
      opts = @opts.dup
      # reduce storage quota to 256KB.
      extra_size = {}
      @node.fetch_pool(@db["service_id"]).with_connection { |conn| extra_size = @node.dbs_size(conn) }
      opts[:max_db_size] = 256.0/1024 + extra_size[@db["name"]].to_f / 1024 / 1024
      node = new_node(opts)
      EM.add_timer(1) do
        binding = node.bind(@db["service_id"],  @default_opts, {'password' => PASSWORD})
        @test_dbs[@db] << binding
        conn = connect_to_mysql(binding, PASSWORD)
        conn.query("create table test(data text)")
        c =  [('a'..'z'),('A'..'Z')].map{|i| Array(i)}.flatten
        256.times do
          # The content string costs mysql 1K to save it.
          # the text data type has 2 bytes overhead.
          content = (0..1022).map{ c[rand(c.size)] }.join
          conn.query("insert into test value('#{content}')")
        end
        # force table status update
        conn.close
        conn = connect_to_mysql(binding, PASSWORD)
        conn.query("analyze table test")

        EM.add_timer(3) do
          expect {conn.query('SELECT 1')}.to raise_error
          conn.close
          conn = connect_to_mysql(binding, PASSWORD)
          # write privilege should be rovoked.
          expect{ conn.query("insert into test value('test')")}.to raise_error(Mysql2::Error)
          conn = connect_to_mysql(@db, PASSWORD)
          expect{ conn.query("insert into test value('test')")}.to raise_error(Mysql2::Error)
          # new binding's write privilege should also be revoked.
          new_binding = node.bind(@db["service_id"], @default_opts, {'password' => PASSWORD})
          @test_dbs[@db] << new_binding
          new_conn = connect_to_mysql(new_binding, PASSWORD)
          expect { new_conn.query("insert into test value('new_test')")}.to raise_error(Mysql2::Error)
          EM.add_timer(3) do
            expect {conn.query('SELECT 1')}.to raise_error
            conn.close
            conn = connect_to_mysql(binding, PASSWORD)
            conn.query("truncate table test")
            # write privilege should restore
            EM.add_timer(2) do
              conn = connect_to_mysql(binding, PASSWORD)
              expect{ conn.query("insert into test value('test')")}.to_not raise_error
              256.times do
                content = (0..1022).map{ c[rand(c.size)] }.join
                conn.query("insert into test value('#{content}')")
              end

              # force table status update
              conn.close
              conn = connect_to_mysql(binding, PASSWORD)
              conn.query("analyze table test")

              EM.add_timer(3) do
                expect { conn.query('SELECT 1') }.to raise_error
                conn.close
                conn = connect_to_mysql(binding, PASSWORD)
                expect{ conn.query("insert into test value('test')") }.to raise_error(Mysql2::Error)
                conn.query("drop table test")
                EM.add_timer(2) do
                  conn = connect_to_mysql(binding, PASSWORD)
                  expect { conn.query("create table test(data text)") }.to_not raise_error
                  expect { conn.query("insert into test value('test')") }.to_not raise_error
                  EM.stop
                end
              end
            end
          end
        end
      end
    end
  end

  it "should able to handle orphan instances when enforce storage quota." do
    begin
      # forge an orphan instance, which is not exist in mysql
      klass = @node.mysqlProvisionedService
      DataMapper.setup(:default, @opts[:local_db])
      DataMapper::auto_upgrade!
      service_id = 'test-'+ UUIDTools::UUID.random_create.to_s
      name = 'test-'+ UUIDTools::UUID.random_create.to_s
      user = "test"
      password = "test"
      port = @node.new_port
      service = klass.create(port, service_id, name, user, password, @default_version)
      if not service.save
        raise "Failed to forge orphan instance: #{service.errors.inspect}"
      end
      EM.run do
        expect { @node.enforce_storage_quota }.to_not raise_error
        EM.stop
      end
    ensure
      @node.use_warden ? service.delete : service.destroy
    end
  end

  it "should return correct instances & binding list" do
    EM.run do
      before_ins_list = @node.all_instances_list
      tmp_db = new_instance(@node)
      @test_dbs[tmp_db] = []
      after_ins_list = @node.all_instances_list
      before_ins_list << tmp_db["service_id"]
      (before_ins_list.sort == after_ins_list.sort).should be_true

      before_bind_list = @node.all_bindings_list
      tmp_credential = @node.bind(tmp_db["service_id"],  @default_opts)
      @test_dbs[tmp_db] << tmp_credential
      after_bind_list = @node.all_bindings_list
      before_bind_list << tmp_credential
      a,b = [after_bind_list,before_bind_list].map do |list|
        list.map{|item| item["username"]}.sort
      end
      (a == b).should be_true

      EM.stop
    end
  end

  it "should not create db or send response if receive a malformed request" do
    EM.run do
      @node.fetch_pool(@db["service_id"]).with_connection do |connection|
        db_num = connection.query("show databases;").count
        mal_plan = "not-a-plan"
        db = nil
        expect {
          db = new_instance(@node, { "name" => "malformed_request" }, false, mal_plan)
        }.to raise_error(VCAP::Services::Mysql::MysqlError, /Invalid plan .*/)
        db.should == nil
        db_num.should == connection.query("show databases;").count
      end
      EM.stop
    end
  end

  it "should support over provisioning" do
    EM.run do
      opts = @opts.dup
      opts[:capacity] = 10
      opts[:max_db_size] = 20
      node = new_node(opts)
      EM.add_timer(1) do
        expect {
          db = new_instance(node, { "name" => "overprovision" })
          @test_dbs[db] = []
        }.to_not raise_error
        EM.stop
      end
    end
  end

  it "should not allow old credential to connect if service is unprovisioned" do
    EM.run do
      conn = connect_to_mysql(@db, PASSWORD)
      expect {conn.query("SELECT 1")}.to_not raise_error
      msg = Yajl::Encoder.encode(@db)
      @node.unprovision(@db["service_id"], [])
      expect {connect_to_mysql(@db, PASSWORD)}.to raise_error
      error = nil
      EM.stop
    end
  end

  it "should return proper error if unprovision a not existing instance" do
    EM.run do
      expect {
        @node.unprovision("not-existing", [])
      }.to raise_error(VCAP::Services::Mysql::MysqlError, /Mysql configuration .* not found/)
      # nil input handle
      @node.unprovision(nil, []).should == nil
      EM.stop
    end
  end

  it "should change variables back after unprovision" do
    EM.run do
      class << @node
        attr_reader :free_ports
      end if @node.use_warden

      pool_size = @node.pools.size
      free_port_size = @node.free_ports.size if @node.use_warden

      db = new_instance(@node, { "name" => "valChange" })
      @node.pools.size.should == (pool_size + 1)
      @node.pools.should have_key(db["service_id"])
      @node.mysqlProvisionedService.get(db["service_id"]).should_not == nil
      if @node.use_warden
        @node.free_ports.should_not include(db["port"])
        @node.free_ports.size.should == (free_port_size - 1)
      end

      @node.unprovision(db["service_id"], [])
      @node.pools.size.should == pool_size
      @node.pools.should_not have_key(db["service_id"])
      @node.mysqlProvisionedService.get(db["service_id"]).should == nil
      if @node.use_warden
        @node.free_ports.size.should == free_port_size
        @node.free_ports.should include(db["port"])
      end
      EM.stop
    end
  end

  it "should not be possible to access one database using null or wrong credential" do
    EM.run do
      db2 = new_instance(@node, {'name' => 'anotherdb'})
      @test_dbs[db2] = []
      fake_creds = []
      3.times {fake_creds << @db.clone}
      # try to login other's db
      fake_creds[0]["name"] = db2["name"]
      # try to login using null credential
      fake_creds[1]["password"] = nil
      # try to login using root account
      fake_creds[2]["user"] = "root"
      fake_creds.each do |creds|
        expect{connect_to_mysql(creds)}.to raise_error
      end
      EM.stop
    end
  end

  it "should kill long transaction" do
    if @opts[:max_long_tx] > 0 and (@node.check_innodb_plugin @db["service_id"])
      EM.run do
        opts = @opts.dup
        # reduce max_long_tx to accelerate test
        opts[:max_long_tx] = 1
        node = new_node(opts)
        EM.add_timer(1) do
          conn = connect_to_mysql(@db, PASSWORD)
          # prepare a transaction and not commit
          conn.query("create table a(id int) engine=innodb")
          conn.query("insert into a value(10)")
          conn.query("begin")
          conn.query("select * from a for update")
          old_killed = node.varz_details[:long_transactions_killed]
          EM.add_timer(opts[:max_long_tx] * 5) {
            expect {conn.query("select * from a for update")}.to raise_error(Mysql2::Error)
            conn.close
            node.varz_details[:long_transactions_killed].should be > old_killed

            node.instance_variable_set(:@kill_long_tx, false)
            conn = connect_to_mysql(@db, PASSWORD)
            # prepare a transaction and not commit
            conn.query("begin")
            conn.query("select * from a for update")
            old_counter = node.varz_details[:long_transactions_count]
            EM.add_timer(opts[:max_long_tx] * 5) {
              expect {conn.query("select * from a for update")}.to_not raise_error(Mysql2::Error)
              node.varz_details[:long_transactions_count].should be > old_counter
              old_counter = node.varz_details[:long_transactions_count]
              EM.add_timer(opts[:max_long_tx] * 5) {
                #counter should not double-count the same long transaction
                node.varz_details[:long_transactions_count].should == old_counter
                conn.close
                EM.stop
              }
            }
          }
        end
      end
    else
      pending "long transaction killer is disabled."
    end
  end

  it "should kill long queries" do
    pending "Disable for non-Percona server since the test behavior varies on regular Mysql server." unless @node.is_percona_server?(@db["service_id"])
    EM.run do
      db = new_instance(@node)
      @test_dbs[db] = []
      opts = @opts.dup
      opts[:max_long_query] = 1
      conn = connect_to_mysql(db, PASSWORD)
      node = new_node(opts)
      EM.add_timer(1) do
        conn.query('create table test(id INT) engine innodb')
        conn.query('insert into test value(10)')
        conn.query('begin')
        # lock table test
        conn.query('select * from test where id = 10 for update')
        old_counter = node.varz_details[:long_queries_killed]

        conn2 = connect_to_mysql(db, PASSWORD)
        err = nil
        t = Thread.new do
          begin
            # conn2 is blocked by conn, we use lock to simulate long queries
            conn2.query("select * from test for update")
          rescue => e
            err = e
          ensure
            conn2.close
          end
        end

        EM.add_timer(opts[:max_long_query] * 5){
          err.should_not == nil
          err.message.should =~ /interrupted/
          # counter should also be updated
          node.varz_details[:long_queries_killed].should be > old_counter
          EM.stop
        }
      end
    end
  end

  it "should create a new credential when binding" do
    pending 'binding is undesigned in vchs yet'
    EM.run do
      binding = @node.bind(@db["service_id"],  @default_opts)
      binding["service_id"].should == @db["service_id"]
      binding["name"].should == @db["name"]
      binding["host"].should be
      binding["host"].should == binding["hostname"]
      binding["port"].should be
      binding["user"].should == binding["username"]
      binding["password"].should be
      binding["uri"].should eq("mysql://#{binding["username"]}:#{binding["password"]}@#{binding["host"]}:#{binding["port"]}/#{@db["name"]}")
      @test_dbs[@db] << binding
      conn = connect_to_mysql(binding)
      expect {conn.query("Select 1")}.to_not raise_error
      EM.stop
    end
  end

  it "should allow access with different binding options" do
    pending 'binding is undesigned in vchs yet'
    EM.run do
      binding_opts1 = { "privileges" => ["FULL"] }
      binding_opts2 = { "privileges" => ["READ_ONLY"] }
      binding1 = @node.bind(@db["service_id"], binding_opts1)
      connection1 = connect_to_mysql(binding1)
      expect do
        connection1.query("create table example (id INT, data VARCHAR(100))")
        connection1.query("insert into example (id,data) VALUES(2,'data2')")
        connection1.query("select * from example")
      end.to_not raise_error
      binding2 = @node.bind(@db["service_id"], binding_opts2)
      connection2 = connect_to_mysql(binding2)
      expect { connection2.query("insert into example (id,data) VALUES(3,'data3')") }.to raise_error(Mysql2::Error, /command denied/)
      expect { connection2.query("select * from example") }.to_not raise_error
      EM.stop
    end
  end

  it "should forbid access for wrong binding options" do
    pending 'binding is undesigned in vchs yet'
    EM.run do
      binding_opts1 = ["FULL"]
      expect { @node.bind(@db["service_id"], binding_opts1) }.to raise_error(RuntimeError, /Invalid binding options format/)
      binding_opts2 = { "privileges" => "FULL" }
      expect { @node.bind(@db["service_id"], binding_opts2) }.to raise_error(RuntimeError, /Invalid binding privileges type/)
      binding_opts3 = { "privileges" => ["READ-ONLY"] }
      expect { @node.bind(@db["service_id"], binding_opts3) }.to raise_error(RuntimeError, /Unknown binding privileges/)
      EM.stop
    end
  end

  it "should supply different credentials when binding evoked with the same input" do
    pending 'binding is undesigned in vchs yet'
    EM.run do
      binding = @node.bind(@db["service_id"], @default_opts)
      binding2 = @node.bind(@db["service_id"], @default_opts)
      @test_dbs[@db] << binding
      @test_dbs[@db] << binding2
      binding.should_not == binding2
      EM.stop
    end
  end

  it "should delete credential after unbinding" do
    pending 'binding is undesigned in vchs yet'
    EM.run do
      binding = @node.bind(@db["service_id"], @default_opts)
      @test_dbs[@db] << binding
      conn = nil
      expect {conn = connect_to_mysql(binding)}.to_not raise_error
      res = @node.unbind(binding)
      res.should be true
      expect {connect_to_mysql(binding)}.to raise_error
      # old session should be killed
      expect {conn.query("SELECT 1")}.to raise_error(Mysql2::Error)
      EM.stop
    end
  end

  it "should delete all bindings if service is unprovisioned" do
    EM.run do
      bindings = []
      3.times { bindings << @node.bind(@db["service_id"], @default_opts)}
      @test_dbs[@db] = bindings
      conn = nil
      @node.unprovision(@db["service_id"], bindings)
      bindings.each { |binding| expect {connect_to_mysql(binding)}.to raise_error }
      EM.stop
    end
  end

  it "should retain instance data after node restart" do
    EM.run do
      node = new_node(@opts)
      EM.add_timer(1) do
        db = new_instance(node, { "name" => "retain_test"})
        @test_dbs[db] = []
        conn = connect_to_mysql(db, PASSWORD)
        conn.query('create table test(id int)')
        # simulate we restart the node
        node.shutdown
        node = new_node(@opts)
        EM.add_timer(1) do
          conn2 = connect_to_mysql(db, PASSWORD)
          result = conn2.query('show tables')
          result.count.should == 1
          EM.stop
        end
      end
    end
  end

  it "should able to generate varz." do
    EM.run do
      node = new_node(@opts)
      #node = VCAP::Services::Mysql::Node.new(@opts)
      EM.add_timer(1) do
        varz = node.varz_details
        varz.should be_instance_of Hash
        varz[:queries_since_startup].should be > 0
        varz[:queries_per_second].should be >= 0
        varz[:database_status].should be_instance_of Array
        varz[:max_capacity].should be > 0
        varz[:available_capacity].should be >= 0
        varz[:used_capacity].should == (varz[:max_capacity] - varz[:available_capacity] )
        varz[:long_queries_killed].should be >= 0
        varz[:long_transactions_killed].should be >= 0
        varz[:provision_served].should be >= 0
        varz[:binding_served].should be >= 0
        varz[:pools].should be_instance_of Hash
        EM.stop
      end
    end
  end

  it "should handle Mysql error in varz" do
    pending "This test is not capatiable with mysql2 conenction pool."
    EM.run do
      node = new_node(@opts)
      EM.add_timer(1) do
        # drop mysql connection
        node.pool.close
        varz = nil
        expect {varz = node.varz_details}.to_not raise_error
        varz.should == {}
        EM.stop
      end
    end
  end

  it "should provide provision/binding served info in varz" do
    EM.run do
      v1 = @node.varz_details
      db = new_instance(@node)
      binding = @node.bind(db["service_id"], @default_opts)
      @test_dbs[db] = [binding]
      v2 = @node.varz_details
      (v2[:provision_served] - v1[:provision_served]).should == 1
      (v2[:binding_served] - v1[:binding_served]).should == 1
      EM.stop
    end
  end

  it "should report instance disk size in varz" do
    EM.run do
      v = @node.varz_details
      instance = v[:database_status].find {|d| d[:service_id] == @db["service_id"]}
      instance.should_not be_nil
      instance[:size].should be >= 0
      EM.stop
    end
  end

  it "should report node instance status in varz" do
    pending "This test is not capatiable with mysql2 conenction pool."
    EM.run do
      varz = @node.varz_details
      varz[:instances].each do |service_id, status|
        status.shoud  == "ok"
      end
      node = new_node(@opts)
      EM.add_timer(1) do
        node.pool.close
        varz = node.varz_details
        varz[:instances].each do |service_id, status|
          status.should == "ok"
        end
        EM.stop
      end
    end
  end

  it "should report instance status in varz" do
    EM.run do
      varz = @node.varz_details()
      instance = @db["service_id"]
      database = @db["name"]
      varz[:instances].each do |service_id, value|
        if service_id == instance.to_sym
          value.should == "ok"
        end
      end
      @node.fetch_pool(instance).with_connection do |connection|
        connection.query("Drop database #{database}")
        sleep 1
        varz = @node.varz_details()
        varz[:instances].each do |service_id, value|
          if service_id == instance.to_sym
            value.should == "fail"
          end
        end
        # restore db so cleanup code doesn't complain.
        connection.query("create database #{database}")
      end
      EM.stop
    end
  end

  it "should report pool size in varz" do
    EM.run do
      varz = @node.varz_details
      if @node.use_warden
        varz[:pools].should have_key(@db["service_id"])
      else
        @opts[:mysql].keys.each { |key| varz[:pools].should have_key(key) }
      end
      EM.stop
    end
  end

  it "should be thread safe" do
    EM.run do
      provision_served = @node.provision_served
      binding_served = @node.binding_served
      # Set concurrent threads to pool size. Prevent pool is empty error.
      NUM = @node.fetch_pool(@db['service_id']).size
      threads = []
      NUM.times do
        threads << Thread.new do
          db = new_instance(@node)
          binding = @node.bind(db["service_id"], @default_opts)
          @node.unprovision(db["service_id"], [binding])
        end
      end
      threads.each {|t| t.join}
      provision_served.should == @node.provision_served - NUM
      binding_served.should == @node.binding_served - NUM
      EM.stop
    end
  end

  it "should enforce max connection limitation per user account" do
    EM.run do
      opts = @opts.dup
      opts[:max_user_conns] = 1 # easy for testing
      node = new_node(opts)
      EM.add_timer(1) do
        db = new_instance(node)
        binding = node.bind(db["service_id"],  @default_opts, {'password' => PASSWORD})
        @test_dbs[db] = [binding]
        expect { conn = connect_to_mysql(db, PASSWORD) }.to_not raise_error
        expect { conn = connect_to_mysql(binding, PASSWORD) }.to_not raise_error
        EM.stop
      end
    end
  end

  it "should add timeout option to all management mysql connection" do
    EM.run do
      opts = @opts.dup
      origin_timeout = Mysql2::Client.default_timeout
      timeout = 1
      opts[:connection_wait_timeout] = timeout
      node = new_node(opts)

      EM.add_timer(2) do
        begin
          # server side timeout
          node.fetch_pool(@db['service_id']).with_connection do |conn|
            # simulate connection idle
            sleep(timeout * 5)
            expect{ conn.query("select 1") }.to raise_error(Mysql2::Error, /MySQL server has gone away/)
          end
          # client side timeout
          node.fetch_pool(@db['service_id']).with_connection do |conn|
            # override server side timeout
            conn.query("set @@wait_timeout=10")
            expect{ conn.query("select sleep(5)") }.to raise_error(Mysql2::Error, /Timeout/)
          end
        ensure
          # restore original timeout
          Mysql2::Client.default_timeout = origin_timeout
          EM.stop
        end
      end
    end
  end

  it "should works well if timeout is disabled for management mysql connection" do
    EM.run do
      opts = @opts.dup
      origin_timeout = Mysql2::Client.default_timeout
      opts.delete :connection_wait_timeout
      node = new_node(opts)

      EM.add_timer(2) do
        begin
          # server side timeout
          node.fetch_pool(@db['service_id']).with_connection do |conn|
            sleep(5)
            expect{ conn.query("select 1") }.to_not raise_error
          end
          # client side timeout
          node.fetch_pool(@db['service_id']).with_connection do |conn|
            expect{ conn.query("select sleep(5)") }.to_not raise_error
          end
        ensure
          # restore original timeout
          Mysql2::Client.default_timeout = origin_timeout
          EM.stop
        end
      end
    end
  end

  #TODO: Need move above since we already use given credential now
  it "should provision instance according to the provided credential" do
    EM.run do
      node = new_node(@opts)
      creds = {
        "service_id" => "testid",
        "name" => "testdbname",
        "user" => "testuser",
        "port" => 25555,
      }
      EM.add_timer(1) do
        db = new_instance(node, creds.merge({"password" => "testpass"}), false)
        @test_dbs[db] = []
        creds.keys.each do |k|
          db[k].should eq creds[k]
        end
        EM.stop
      end
    end
  end

  it "should always take recipe as valid in case of port conflict" do
    EM.run do
      EM.add_timer(1) do
        db = new_instance(@node, { "port" => @db['port'], "password" => "new_password" } )
        expect { conn = connect_to_mysql(@db, PASSWORD) }.to raise_error
        expect { conn = connect_to_mysql(db, "new_password") }.to_not raise_error
        @test_dbs[db] = []
        EM.stop
      end
    end
  end

  it "should not provision instance if port can't be freed" do
    EM.run do
      node = new_node(@opts)
      creds = {
        "port" => 25555,
      }
      EM.add_timer(1) do
        node.new_port(creds["port"])
        expect{  new_instance(node, creds) }
          .to raise_error(VCAP::Services::Base::Error::ServiceError,
                          /port.*in use/)
        EM.stop
      end
    end
  end

  it "should support provision with restored data in warden" do
    pending "This case is for warden only" unless @node.use_warden
    EM.run do
      creds = {
        "service_id" => "new_instance",
        "name"       => @db["name"],
        "user"       => @db["user"],
        "port"       => 25555,
      }

      conn = connect_to_mysql(@db, PASSWORD)
      conn.query("CREATE TABLE test(id INT)")
      conn.query("INSERT INTO test VALUE(10)")
      conn.query("INSERT INTO test VALUE(20)")
      conn.close

      origin_path = File.join(@opts[:base_dir], @db["service_id"], "data")
      dest_path = File.join(@opts[:base_dir], creds["service_id"])
      FileUtils.mkdir_p(dest_path)
      FileUtils.cp_r(origin_path, dest_path)

      properties = {"is_restoring" => true}

      db = new_instance(@node,  creds, false, @default_plan, properties)
      @test_dbs[db] = []
      conn = connect_to_mysql(db, PASSWORD)
      conn.query("select * from test").each(:symbolize_keys => true) do |row|
        [10, 20].should include(row[:id])
      end

      EM.stop
    end
  end

  after :each do
    EM.run do
      @node.create_missing_pools if @node.use_warden
      @test_dbs.keys.each do |db|
        begin
          service_id = db["service_id"]
          @node.unprovision(service_id, @test_dbs[db])
          @node.logger.info("Clean up temp database: #{service_id}")
        rescue => e
          @node.logger.info("Error during cleanup #{e}")
        end
      end if @test_dbs
      @tmpfiles.each do |tmpfile|
        FileUtils.rm_r tmpfile
      end
      EM.stop
    end
  end

  after :all do
    FileUtils.rm_rf getNodeTestConfig[:node_tmp_dir]
  end
end
