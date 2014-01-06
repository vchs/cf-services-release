require "spec_helper"
require "sc_sdk"

USER = "a@b.c"
PASS = "abc"
TARGET = "http://localhost:3000"

describe "vCHS mysql service", components: [:nats, :sc, :sc_mysql, :sc_uaa],
  hook: :all do
#describe "test" do
  before :all do
    tokens = ServicesController::SDK.login(TARGET, USER, PASS)
    @client = ServicesController::SDK.new(TARGET, tokens["access_token"])
  end

  context "Service registration" do
    let(:svcs) { @client.service.list[:response] }

    it "have 1 service" do
      svcs.fetch("total_results").should eq 1
    end

    it "is mysql service" do
      svcs.fetch("resources").first.fetch("metadata")
        .fetch("guid").should eq "mysql_service_unique_id"
    end
  end

  context "Plan registration" do
    let(:plans) { @client.service_plan.list[:response] }

    it "have 1 plan" do
      plans.fetch("total_results").should eq 1
    end

    it "is mysql 200" do
      plans.fetch("resources").first.fetch("metadata")
        .fetch("guid").should eq "core_mysql_200"
    end
  end

  context "Instance management" do
    before(:all) do
      config = {
        "service_plan_id" => "core_mysql_200",
        "status" => "stopped",
        "owner_email" => "admin@example.com",
        "description" => "MySQL 5.6",
        "properties" => {
          "size" => "1024MB",
          "deployment_mode" => "shared",
        }
      }
      @instance = @client.service_instance.create(config)[:response]
    end

    let(:instances) { @client.service_instance.list[:response] }
    let(:guid) { @instance.fetch("metadata").fetch("guid") }
    let(:properties) { JSON.parse(@instance.fetch("entity").fetch("properties")) }
    let(:credentials) { properties.fetch("credentials") }

    it "have 1 instance" do
      instances.fetch("total_results").should eq 1
    end

    it "is the test instance" do
      instances.fetch("resources")[0].fetch("metadata")
        .fetch("guid").should eq guid
    end

    it "can connect to instance" do
      # Let Sequel to use mysql2 gem
      uri = credentials.fetch("uri").gsub(/^mysql:/, "mysql2:")
      expect_statement_allowed!(uri, 'show tables')
    end

    after(:all) { @client.service_instance.delete(guid) }
  end
end
