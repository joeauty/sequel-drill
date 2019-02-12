require "sequel"
require "json"
require "net/http"
require "http-cookie"
require_relative "../helpers/session"

module Sequel
  extension :core_extensions
  module Drill

    class AuthError < StandardError; end
    class DrillInternalError < Sequel::DatabaseError; end

    class Database < Sequel::Database

      def dataset_class_default
        Dataset
      end

      set_adapter_scheme :drill

      HEADERS = {
        "Content-Type" => "application/json",
        "Accept" => "application/json"
      }.freeze

      def connect(server)
        opts = server_opts(server)

        # save object for future re-use (build query using provided Drill workspace)
        @connect_opts = opts

        # authenticates on drill if user field is present and no sessions are present for the provided user.
        if session_cookie_for_user.nil? and !opts[:user].nil?
          Database.authenticate!(opts[:user], opts[:password], opts[:host], opts[:port])
        end

        @query_uri = URI.parse("http://#{opts[:host]}:#{opts[:port]}/query.json")

        connection = Net::HTTP.new(@query_uri.host, @query_uri.port)
        connection.read_timeout = opts[:read_timeout] unless opts[:read_timeout].nil?
        connection
      end

      # drill authentication must be enabled. https://drill.apache.org/docs/creating-custom-authenticators/
      #
      # when a sucessfull login is made, drill returns 303 status code, with set-cookie header.
      # when login fails drill returns 200 with an html containing "invalid credentials" string
      def self.authenticate!(user, password, host, port)

        login_uri = URI("http://#{host}:#{port}/j_security_check")

        params = {
          j_username: user,
          j_password: password
        }

        res = Net::HTTP.post_form(login_uri, params)

        case res.code.to_i

        # invalid creds will return a 200 status code with an error html.
        when 200
          raise AuthError.new("Invalid Credentials")
        when 303

          jar = HTTP::CookieJar.new
          # store received session cookie
          Session.instance.set("#{user}:#{password}", jar.parse(res.get_fields('Set-Cookie').first, login_uri))
        end
      end

      def session_cookie_for_user
        Session.instance.get("#{@connect_opts[:user]}:#{@connect_opts[:password]}")
      end

      # if user is authenticated
      # append session cookies to headers
      def headers
        cookie = session_cookie_for_user
        if cookie.nil?
          HEADERS
        else
          { "Cookie" => HTTP::Cookie.cookie_value(cookie) }.merge(HEADERS)
        end
      end

      def execute(sql, opts = {}, &block)
        res = nil

        data = {
          queryType: "SQL",
          query: sql
        }

        # replace quotation marks with backticks for proper Drill support
        sql.gsub!('"', '')

        # append workspace to all table names
        sql.gsub!(/FROM ([A-Za-z0-9_]+)/, "FROM dfs.#{workspace}.\\1")

        if sql.start_with?("DROP TABLE ")
          sql.sub!(/^DROP TABLE (IF EXISTS )?`([A-Za-z0-9_\.]+)`$/, "DROP TABLE IF EXISTS dfs.#{workspace}.`\\2`")
        end

        synchronize(opts[:server]) do |conn|
          # TODO: change to log_connection_yield

          res = log_yield(sql) {
            conn.post(@query_uri.request_uri, data.to_json, headers)
          }

          # TODO: move to a better rest api error handling based on error codes
          case res.code.to_i
          # when drill returns 307, it means authentication is enabled but we don't have a session
          when 307
            raise AuthError.new("Drill returned a temporary redirect")
          when 500
            raise DrillInternalError.new("#{JSON.parse(res.body)['errorMessage']}")
          else
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
        end

        res
      # raise drill error with vanilla `raise` instead of raise_error here
      # since it makes it easier to rescue on user-land applications.
      rescue AuthError, DrillInternalError => e
        raise(e)

      rescue Exception => e
        # unexpected errors are thrown by raise_error method
        raise_error(e)
      end

      def workspace
        # get Drill workspace from traditional Sequel database connection string
        URI(uri).path[1..-1] || ENV['DRILL_WORKSPACE']
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
      LESS_THAN = '<'.freeze
      GREATER_THAN = '>'.freeze

      def fetch_rows(sql)
        # hacks for Sequel functions without adapter methods intended to be extended/overridden
        # aggregate functions should include backticks
        sql.gsub!(/([[:alpha:]]+\(.*`?[A-Za-z0-9_*\s]+`?\)) AS ([A-Za-z0-9_]+)/, '\1 AS `\2`')

        execute(sql) do |row|
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
