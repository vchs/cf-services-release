require "spec_helper"

describe "vCHS mysql service", components: [:nats, :sc, :sc_mysql], hook: :all do
  include ScClient

  context "Service registration" do
    let(:svcs) { sc_get("/api/v1/services") }

    it "have 1 service" do
      svcs.fetch("total_results").should eq 1
    end

    it "is mysql service" do
      svcs.fetch("resources").first.fetch("metadata")
        .fetch("guid").should eq "mysql_service_unique_id"
    end
  end

  context "Plan registration" do
    let(:plans) { sc_get("/api/v1/service_plans") }

    it "have 1 plan" do
      plans.fetch("total_results").should eq 1
    end

    it "is mysql 200" do
      plans.fetch("resources").first.fetch("metadata")
        .fetch("guid").should eq "core_mysql_200"
    end
  end

  context "Instance management" do
    before(:all){ @instance = sc_create_instance }

    let(:instances) { sc_get("/api/v1/service_instances") }
    let(:guid) { @instance.fetch("metadata").fetch("guid") }
    let(:properties) { JSON.parse(@instance.fetch("entity").fetch("properties")) }
    let(:credentials) { properties.fetch("credentials") }

    it "have 1 instance" do
      instances.fetch("total_results").should eq 1
    end

    it "can list instance" do
      sc_get("/api/v1/service_instances/#{guid}").fetch("resources").should be
    end

    it "can connect to instance" do
      # Let Sequel to use mysql2 gem
      uri = credentials.fetch("uri").gsub(/^mysql:/, "mysql2:")
      expect_statement_allowed!(uri, 'show tables')
    end

    after(:all) { sc_delete_instance(guid) }
  end
end
