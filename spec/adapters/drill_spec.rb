require 'spec_helper'

ENV['DRILL_WORKSPACE']    = 'tmp' # workspace for imported Parquet files
ENV['HDFS_HOST']          = 'drill' # HDFS hostname (Docker hostname)

unless defined?(DRILL)
  DRILL_URL = 'drill://drill:8047' unless defined? DRILL_URL
  DRILL_DB = Sequel.connect(ENV['DRILL_URL']||DRILL_URL)
end

describe '#connect' do

  it 'should set read_timeout' do
    conn = Sequel.connect("#{ENV['DRILL_URL']||DRILL_URL}", read_timeout: 9999)
    conn.synchronize do |c|
      expect(c.read_timeout).to eq 9999
    end
  end

  it 'should keep default read_timeout when not set' do
    conn = Sequel.connect("#{ENV['DRILL_URL']||DRILL_URL}")
    conn.synchronize do |c|
      expect(c.read_timeout).to eq 60
    end
  end

  it 'should call authenticate method' do

    expect(Sequel::Drill::Database).to receive(:authenticate!)

    conn = Sequel.connect("#{ENV['DRILL_URL']||DRILL_URL}", user: "user", password: "123")
    conn.synchronize do |c|
      expect(c.read_timeout).to eq 60
    end
  end
end

describe "import dataset" do
  before do
    @db = DRILL_DB
  end

  specify "upload CSV dataset via WebHDFS" do
    conn = Sequel.connect("#{ENV['DRILL_URL']||DRILL_URL}")
    conn.synchronize do |raw_conn|
      webhdfs = WebHDFS::Client.new("#{ENV['HDFS_HOST']}", 50070)
      upload = webhdfs.create("/#{ENV['DRILL_WORKSPACE']}/test.csv", File.open("spec/adapters/test.csv", "r"), :overwrite => true)
    end
  end

  specify "create parquet file out of CSV" do
    expect(@db.run("DROP TABLE IF EXISTS dfs.#{ENV['DRILL_WORKSPACE']}.\`test\`")).to eq(nil)
    expect(@db.run("CREATE TABLE dfs.#{ENV['DRILL_WORKSPACE']}.\`test\` AS SELECT CAST(columns[0] AS VARCHAR(65000)) AS \`c1\`, CAST(columns[1] AS FLOAT) AS \`c2\`, CAST(columns[2] AS VARCHAR(65000)) AS \`c3\` FROM dfs.tmp.\`test.csv\`")).to eq(nil)
  end

  specify "verify import, cleanup" do
    expect(DRILL_DB[:test].select(:c1).first[:c1]).to eq("foo")
  end

  specify "cleanup original CSV" do
    expect(@db.run("DROP TABLE IF EXISTS dfs.#{ENV['DRILL_WORKSPACE']}.\`test.csv\`")).to eq(nil)
  end
end

describe "A drill dataset" do
  before do
    @d = DRILL_DB[:test]
  end

  specify "Drill workaround: override != operators with <>" do
    expect(@d.where(c1:10).invert.sql).to eq( \
    'SELECT * FROM "test" WHERE "c1" <> 10'
    )
  end

  specify "Drill workaround: aggregate methods column display names should be escaped with backticks using Drill workspace" do
    # generated query should be "SELECT count(name) AS `count` FROM dfs.tmp.`test` LIMIT 1"
    expect(@d.count(:c1)).to eq("3")

    # test the other methods just for good measure
    expect(@d.max(:c2)).to eq("66.0")
    expect(@d.min(:c2)).to eq("11.0")
    expect(@d.sum(:c2)).to eq("99.0")
    expect(@d.avg(:c2)).to eq("33.0")
  end

  specify "quotes columns and tables using double quotes if quoting identifiers" do
    expect(@d.select(:name).sql).to eq( \
      'SELECT "name" FROM "test"'
    )

    expect(@d.select(Sequel.lit('COUNT(*)')).sql).to eq( \
      'SELECT COUNT(*) FROM "test"'
    )

    expect(@d.select(:max.sql_function(:value)).sql).to eq( \
      'SELECT max("value") FROM "test"'
    )

    expect(@d.select(:NOW.sql_function).sql).to eq( \
    'SELECT NOW() FROM "test"'
    )

    expect(@d.select(:max.sql_function(:items__value)).sql).to eq( \
      'SELECT max("items"."value") FROM "test"'
    )

    expect(@d.order(:name.desc).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" DESC'
    )

    expect(@d.select(Sequel.lit('test.name AS item_name')).sql).to eq( \
      'SELECT test.name AS item_name FROM "test"'
    )

    expect(@d.select(Sequel.lit('"name"')).sql).to eq( \
      'SELECT "name" FROM "test"'
    )

    expect(@d.select(Sequel.lit('max(test."name") AS "max_name"')).sql).to eq( \
      'SELECT max(test."name") AS "max_name" FROM "test"'
    )
  end

  specify "quotes fields correctly when reversing the order if quoting identifiers" do
    expect(@d.reverse_order(:name).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" DESC'
    )

    expect(@d.reverse_order(:name.desc).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" ASC'
    )

    expect(@d.reverse_order(:name, :test.desc).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" DESC, "test" ASC'
    )

    expect(@d.reverse_order(:name.desc, :test).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" ASC, "test" DESC'
    )
  end
end

describe "authenticate!" do

  before do
    response = Net::HTTPResponse.new({}, 303, {})

    allow(response).to receive(:get_fields).and_return "JSESSIONID=123123;Path=\\"
    expect(Net::HTTP).to receive(:post_form).and_return(response)
  end

  it 'should create session token' do
    Sequel::Drill::Database.authenticate!("u1", "p1", "drill", 8047)
    expect(Session.instance.get("u1:p1").first.cookie_value).to eq "JSESSIONID=123123"
  end
end
