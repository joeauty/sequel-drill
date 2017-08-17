require "sequel"
require "json"
require "net/http"

module Sequel
  extension :core_extensions
  module Drill

    class Database < Sequel::Database

      set_adapter_scheme :drill
      
      HEADERS = {
        "Content-Type" => "application/json",
        "Accept" => "application/json"
      }

      def connect(server)
        opts = server_opts(server)
        # save object for future re-use (build query using provided Drill workspace)
        @connect_opts = opts
        
        @uri = URI.parse("http://#{opts[:host]}:#{opts[:port]}/query.json")
        Net::HTTP.new(@uri.host, @uri.port)
      end

      def execute(sql, opts = {}, &block)
        res = nil
        
        data = {
          queryType: "sql",
          query: sql
        }
        
        synchronize(opts[:server]) do |conn|
          # TODO: change to log_connection_yield
          res = log_yield(sql) {
            conn.post(@uri.request_uri, data.to_json, HEADERS)
          }
          
          res = JSON.parse(res.body)
          if res["errorMessage"].nil?
            
            # discard column listing to follow Sequel convention
            res = res["rows"]
            
            # return empty array for empty data sets to follow more common conventions
            if res.to_json == "[{}]"
              res = []
            end
          end
          res.each(&block)
        end
        res
      rescue Exception => e
        raise_error(e)
      end

      alias_method :execute_dui, :execute

      def supports_create_table_if_not_exists?
        false
      end

      def supports_drop_table_if_exists?
        true
      end

      def supports_transaction_isolation_levels?
        false
      end

      def identifier_input_method_default
        nil
      end

      def identifier_output_method_default
        nil
      end

      def create_table_generator_class
        # we aren't using Sequel to import tables, yet... Leave this method available for debugging
      end
    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self
      LESS_THAN = '<'.freeze
      GREATER_THAN = '>'.freeze
      
      def fetch_rows(sql)
        # hacks for Sequel functions without adapter methods intended to be extended/overridden
      
        # replace quotation marks with backticks for proper Drill support
        sql.gsub!('"', '`')
        # aggregate functions should include backticks
        sql.gsub!(/([[:alpha:]]+\(.*`?[A-Za-z0-9_*\s]+`?\)) AS ([A-Za-z0-9_]+)/, '\1 AS `\2`')
        
        # convert Sequel table names to Drill workspace + file
        workspace = ENV['DRILL_WORKSPACE'] ||= "tmp"
        unless sql.match(/.+dfs.#{workspace}.`[A-Za-z0-9_]`.+/) # namespace already attached, do nothing
          if sql.start_with?("SELECT ")
            sql.sub!(/^SELECT (.+) FROM `([A-Za-z0-9_]+)`/, "SELECT \\1 FROM dfs.#{workspace}.`\\2`")
          elsif query_string.start_with?("DROP TABLE ")
            sql.sub!(/^DROP TABLE (IF EXISTS )?`([A-Za-z0-9_]+)`$/, "DROP TABLE IF EXISTS dfs.#{workspace}.`\\2`")
          end
        end
        
        puts sql
        execute(sql) do |row|
          puts row
          # TODO: possible hack to cast numbers recorded as JSON strings to numbers?
          yield row.to_h.inject({}) { |a, (k,v)| a[k.to_sym] = v; a }
        end
      end

      def supports_regexp?
        false
      end

      def supports_window_functions?
        false
      end
      
      def complex_expression_sql_append(sql, op, args)
        case op
        when :'!='
          # Apache Drill doesn't support != as an ne operator, use <> instead
          literal_append(sql, args.at(0))
          sql << " " << LESS_THAN << GREATER_THAN << " "
          literal_append(sql, args.at(1))
        else
          super
        end
      end

    end
  end
end
