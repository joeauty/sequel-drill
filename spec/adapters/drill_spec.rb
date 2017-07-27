require 'spec_helper'

ENV['DRILL_WORKSPACE']    = 'tmp' # workspace for imported Parquet files
ENV['HDFS_HOST']          = 'drill' # HDFS hostname (Docker hostname)

unless defined?(DRILL)
  DRILL_URL = 'drill://drill:8047' unless defined? DRILL_URL
  DRILL_DB = Sequel.connect(ENV['DRILL_URL']||DRILL_URL)
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
    #@d.delete if @d.count > 0 # Vertica will throw an error if the table has just been created and does not have a super projection yet.
  end

  specify "quotes columns and tables using double quotes if quoting identifiers" do
    expect(@d.select(:name).sql).to eq( \
      'SELECT "name" FROM "test"'
    )

    expect(@d.select(Sequel.lit('COUNT(*)')).sql).to eq( \
      'SELECT COUNT(*) FROM "test"'
    )
    
=begin
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
=end
    
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

=begin
    expect(@d.reverse_order(:name.desc).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" ASC'
    )

    expect(@d.reverse_order(:name, :test.desc).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" DESC, "test" ASC'
    )

    expect(@d.reverse_order(:name.desc, :test).sql).to eq( \
      'SELECT * FROM "test" ORDER BY "name" ASC, "test" DESC'
    )
=end
  end
end

