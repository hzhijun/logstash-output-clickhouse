# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/json"
require "logstash/util/shortname_resolver"
require "uri"
require "stud/buffer"
require "logstash/plugin_mixins/http_client"
require "securerandom"
require "json"
require 'fileutils'
require 'date'
require 'time'

class LogStash::Outputs::ClickHouse < LogStash::Outputs::Base
  include LogStash::PluginMixins::HttpClient
  include Stud::Buffer

  concurrency :single

  config_name "clickhouse"

  config :http_hosts, :validate => :array, :required => true

  config :table, :validate => :string, :required => true

  # Custom headers to use
  # format is `headers => ["X-My-Header", "%{host}"]`
  config :headers, :validate => :hash

  config :flush_size, :validate => :number, :default => 50

  config :idle_flush_time, :validate => :number, :default => 5

  config :pool_max, :validate => :number, :default => 50

  config :save_on_failure, :validate => :boolean, :default => true

  config :save_dir, :validate => :string, :default => "/tmp"

  config :save_file, :validate => :string, :default => "failed.json"

  config :request_tolerance, :validate => :number, :default => 5

  config :backoff_time, :validate => :number, :default => 3

  config :automatic_retries, :validate => :number, :default => 3

  config :mutation_path, :validate => :string, :default => ""

  config :mutations, :validate => :hash, :default => {}

  config :host_resolve_ttl_sec, :validate => :number, :default => 120

  def print_plugin_info()
    @@plugins = Gem::Specification.find_all{|spec| spec.name =~ /logstash-output-clickhouse/ }
    @plugin_name = @@plugins[0].name
    @plugin_version = @@plugins[0].version
    @logger.info("Running #{@plugin_name} version #{@plugin_version}")

    @logger.info("Initialized clickhouse with settings",
      :flush_size => @flush_size,
      :idle_flush_time => @idle_flush_time,
      :request_tokens => @pool_max,
      :http_hosts => @http_hosts,
      :http_query => @http_query,
      :headers => request_headers)
  end

  def register
    if @save_dir.end_with?("/")
        time = Time.new
        raise "[#{time.inspect}][ERROR][logstash.outputs.clickhouse] save_dir 不能以 / 结尾"
     end
    if @mutations.empty? == false
        puts "使用 mutations 配置的内容"
    elsif @mutation_path != ""
        puts "mutations 字段为空，使用 #{mutation_path} 配置文件内容"
        mutation_str = File.read(@mutation_path)
        @mutations = JSON.parse(mutation_str)
    else
        puts "mutations、mutation_path 都为空，使用默认值 {}"
    end

    if @mutations.length == 0
      raise "[#{time.inspect}][ERROR][logstash.outputs.clickhouse] mutation_path、mutation 必须包含其中之一"
    end

    mutations_tmp = {}
    types = ['string','integer','float','date','datetime','datetime64']
    @mutations.each_pair do |dstkey, source|
      case source
        when Array then
          if source.length != 2
            raise "[#{time.inspect}][ERROR][logstash.outputs.clickhouse]  #{dstkey} 类型为数组，但长度不配，数组长度必须等于二  "
          end
          srctype = source[1].downcase
          if types.include?(srctype) == false
            mutations_tmp[dstkey] = source
          end
      end
    end

    if mutations_tmp.length > 0
       raise "[#{time.inspect}][ERROR][logstash.outputs.clickhouse]  不支持的数据类型验证，not in (string、integer、float)，#{mutations_tmp.to_json()} "
    end

    if File.directory?("#{save_dir}") == false
        FileUtils.mkdir_p("#{save_dir}")
    end
    # Handle this deprecated option. TODO: remove the option
    #@ssl_certificate_validation = @verify_ssl if @verify_ssl

    # We count outstanding requests with this queue
    # This queue tracks the requests to create backpressure
    # When this queue is empty no new requests may be sent,
    # tokens must be added back by the client on success
    @request_tokens = SizedQueue.new(@pool_max)
    @pool_max.times {|t| @request_tokens << true }
    @requests = Array.new
    @http_query = "/?query=INSERT%20INTO%20#{table}%20FORMAT%20JSONEachRow"

    @hostnames_pool =
      parse_http_hosts(http_hosts,
        ShortNameResolver.new(ttl: @host_resolve_ttl_sec, logger: @logger))

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )

    print_plugin_info()
  end # def register

  private

  def parse_http_hosts(hosts, resolver)
    ip_re = /^[\d]+\.[\d]+\.[\d]+\.[\d]+$/

    lambda {
      hosts.flat_map { |h|
        scheme = URI(h).scheme
        host = URI(h).host
        port = URI(h).port
        path = URI(h).path

        if ip_re !~ host
          resolver.get_addresses(host).map { |ip|
            "#{scheme}://#{ip}:#{port}#{path}"
          }
        else
          [h]
        end
      }
    }
  end

  private

  def get_host_addresses()
    begin
      @hostnames_pool.call
    rescue Exception => ex
      @logger.error('Error while resolving host', :error => ex.to_s)
    end
  end

  # This module currently does not support parallel requests as that would circumvent the batching
  def receive(event)
    buffer_receive(event)
  end

  def mutate( event )
    src = event.to_hash()
    res = {}

    # 类型错误的字段
    type_error_field = {}
    # 包含类型错误字段的行数据
    type_error_field_row = {}

    # 多余的字段
    unknown_field = event.to_hash()

    # 包含多余字段的行数据
    unknown_field_row = {}

    return [res, type_error_field_row, unknown_field_row] if @mutations.empty?
    @mutations.each_pair do |dstkey, source|
      case source
        when String then
          srckey = source
          next unless src.key?(srckey)
          unknown_field.delete(srckey)
          res[dstkey] = src[srckey]
        when Array then
          srckey = source[0]
          next unless src.key?(srckey)
          unknown_field.delete(srckey)
          tartype = source[1].downcase
          src_class_name = event.get(srckey).class.name.downcase
          srcvalue = src[srckey]
          if tartype == src_class_name or (tartype =='float' and src_class_name == 'integer')
            res[dstkey] = srcvalue
          # 如果类型不匹配，这儿先做一次基础数据类型转换。
          else
            # 对日期和时间类型的数据做校验
            isok = false
            srcvalue_new = srcvalue
            dateTypes = ['date','datetime','datetime64']
            msg = ''
            if dateTypes.include?(tartype) == true
              dateTypeFormat = {date:"YYYY-mm-dd",datetime:"YYYY-mm-dd HH:MM:SS",datetime64:"YYYY-mm-dd HH:MM:SS.LLL"}
              isok = date_data_validate(src_class_name, srcvalue, tartype)
              if !isok
                format = dateTypeFormat[:"#{tartype}"]
                msg = "数据格式错误，规定的类型是 #{tartype}，格式需满足 #{format}"
              end
            else
              # 这儿的 isok 代表数据是否强转成功，无论如何需要记录日志类型不匹配。
              srcvalue_new, isok = data_conversion(event, srckey, srcvalue, tartype, src_class_name)
              msg = "规定的类型 #{tartype}，实际的类型 #{src_class_name}"
            end

            if isok
               res[dstkey] = srcvalue_new
            end
            if msg != ''
               type_error_field[srckey] = msg
            end
          end
      end
    end

    if type_error_field.empty? == false
      type_error_field_row, errors_str = assembly_error_message(src, type_error_field, 'type_not_match')
    end
    if unknown_field.empty? == false
      unknown_field_row, errors_str = assembly_error_message(src, unknown_field, 'not_included_field')
      if @mutations.key?("unknown_fields") == true
        # 没有quirks_mode: true，会得到荒谬的“JSON::GeneratorError: only generation of JSON objects or arrays allowed”。
        res["unknown_fields"] = unknown_field.to_json()
      end
    end
    [res, type_error_field_row, unknown_field_row]
  end

# 数据类型转换
  def data_conversion(event, srckey, srcvalue, tartype, src_class_name)
    srcvalue_new = ''

    # 判断下数据类型是否是基础数据类型
    types = ['string','integer','float','bigdecimal']
    if types.include?(src_class_name) == false
     return srcvalue_new, false
    end

    # 只有基础数据类型可以转换
    if tartype == 'string'
      srcvalue_new = srcvalue.to_s
    elsif tartype == 'integer'
      srcvalue_new = srcvalue.to_i
    elsif tartype == 'float'
      srcvalue_new = srcvalue.to_f
    else
      srcvalue_new = srcvalue
    end
    event.set(srckey, srcvalue_new)
    class_name = event.get(srckey).class.name.downcase
    isok = tartype == class_name
    return srcvalue_new, isok

  end

  ## 日期相关的数据校验 'date','datetime','datetime64'
  def date_data_validate(src_class_name, srcvalue, tartype)
    if src_class_name == 'integer' or src_class_name == 'float'
      return true
    elsif src_class_name != 'string'
      return false
    end
    isok = false
    if tartype == 'date'
       isok = validate_date(srcvalue)
    elsif tartype == 'datetime'
       isok = validate_datetime(srcvalue)
    elsif tartype == 'datetime64'
       isok = validate_datetime64(srcvalue)
    end
    return isok
  end

  # 日期格式校验 2020-01-01
  def validate_date(str)
    format_ok = str.match(/^\d{4}-\d{2}-\d{2}$/)
    parseable = Date.strptime(str, '%Y-%m-%d') rescue false
    if format_ok and parseable
      return true
    end
    return false
  end

  # 时间格式校验 2020-01-01 01:01:01
  def validate_datetime(str)
    format_ok = str.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/)
    parseable = Time.strptime(str, '%Y-%m-%d %H:%M:%S') rescue false
    if format_ok and parseable
      return true
    end
    return false
  end

  # 时间格式校验 2020-01-01 01:01:01.100
  def validate_datetime64(str)
    format_ok = str.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{3}$/)
    parseable = Time.strptime(str, '%Y-%m-%d %H:%M:%S.%L') rescue false
    if format_ok and parseable
      return true
    end
    return false
  end

# 统一做数据的组装
  def assembly_error_message(source_data, error_msg, sub_type)
    ems = {}
    ems['random_key'] = SecureRandom.hex(20)
    ems['error_msg'] = error_msg
    ems['log_type'] = 'output_clickhouse_prog'
    ems['sub_type'] = sub_type
    ems['log_date'] = Time.new.strftime("%Y-%m-%d")
    ems['log_time'] = Time.new.strftime("%Y-%m-%d %H:%M:%S")
    ems['table_name'] = @table
    ems['source_data'] = source_data
    ems_str = "#{ems.to_json()}\n"
    return ems, ems_str
  end

  public
  def flush(events, close=false)
    documents = ""  #this is the string of hashes that we push to Fusion as documents
    type_error_documents = ""  #this is the string of hashes that we push to Fusion as documents
    unknown_field_documents = ""  #this is the string of hashes that we push to Fusion as documents
    events.each do |event|
        res, type_error_field_row, unknown_field_row = mutate( event )
        if res.empty? == false
          documents << LogStash::Json.dump( res ) << "\n"
        end
        if type_error_field_row.empty? == false
          type_error_documents << LogStash::Json.dump( type_error_field_row ) << "\n"
        end
        if unknown_field_row.empty? == false
          unknown_field_documents << LogStash::Json.dump( unknown_field_row ) << "\n"
        end
    end
    if type_error_documents != ""
      puts type_error_documents
      save_to_disk(type_error_documents,"参数类型不匹配")
    end

    if unknown_field_documents != ""
      puts unknown_field_documents
      save_to_disk(unknown_field_documents,"未被解析字段")
    end

    if documents != ""
      hosts = get_host_addresses()
      make_request(documents, hosts, @http_query, 1, 1, hosts.sample)
    end
  end

  private

  def save_to_disk(documents, dirName)
    begin
      time = Time.new
      day =  time.strftime("%Y-%m-%d")
      file_dir = "#{save_dir}/#{day}/#{dirName}"
      # 目录不存在就创建
      if File.directory?("#{file_dir}") == false
         FileUtils.mkdir_p("#{file_dir}")
      end
      hour = time.strftime("%H")
      file = File.open("#{file_dir}/#{hour}h##{table}.json", "a")
      file.write(documents)

    rescue IOError => e
      log_failure("An error occurred while saving file to disk: #{e}",
                    :file_name => file_name)
    ensure
      file.close unless file.nil?
    end
  end

  def delay_attempt(attempt_number, delay)
    # sleep delay grows roughly as k*x*ln(x) where k is the initial delay set in @backoff_time param
    attempt = [attempt_number, 1].max
    timeout = lambda { |x| [delay*x*Math.log(x), 1].max }
    # using rand() to pick final sleep delay to reduce the risk of getting in sync with other clients writing to the DB
    sleep_time = rand(timeout.call(attempt)..timeout.call(attempt+1))
    sleep sleep_time
  end

  def single_make_request(documents, hosts, query, host, response)
    docList = documents.split(/\n/)
    if docList.length > 1
      docList.each do |doc|
        make_request("#{doc}\n", hosts, query, 1, 1, host)
      end
    else
      if @save_on_failure
        new_doc, new_doc_str = assembly_error_message(documents, response.body, 'insert_table_fail')
        puts new_doc_str
        save_to_disk(new_doc_str, "入库失败数据")
      end
    end
  end

  private

  def make_request(documents, hosts, query, con_count = 1, req_count = 1, host = "", uuid = SecureRandom.hex)

    if host == ""
      host = hosts.pop
    end

    url = host+query

    # Block waiting for a token
    #@logger.info("Requesting token ", :tokens => request_tokens.length())
    token = @request_tokens.pop
    @logger.debug("Got token", :tokens => @request_tokens.length)

    # Create an async request
    begin
      request = client.send(:post, url, :body => documents, :headers => request_headers, :async => true)
    rescue Exception => e
      @logger.warn("An error occurred while indexing: #{e.message}")
    end

    request.on_success do |response|
      # Make sure we return the token to the pool
      @request_tokens << token

      if response.code == 200
        @logger.debug("Successfully submitted",
          :size => documents.length,
          :response_code => response.code,
          :uuid => uuid)
      else
        if req_count >= @request_tolerance
          log_failure(
              "Encountered non-200 HTTP code #{response}",
              :response_code => response.code,
              :message => response.message,
              :response => response.body,
              :url => url,
              :size => documents.length,
              :uuid => uuid)
          single_make_request(documents, hosts, query, host, response)
        else
          @logger.info("Retrying request", :url => url, :message => response.message, :response => response.body, :uuid => uuid)
          delay_attempt(req_count, @backoff_time)
          make_request(documents, hosts, query, con_count, req_count+1, host, uuid)
        end
      end
    end

    request.on_failure do |exception|
      # Make sure we return the token to the pool
      @request_tokens << token

      if hosts.length == 0
          log_failure("Could not access URL",
            :url => url,
            :method => @http_method,
            :headers => headers,
            :message => exception.message,
            :class => exception.class.name,
            :backtrace => exception.backtrace,
            :size => documents.length,
            :uuid => uuid)
          if @save_on_failure
            docList = documents.split(/\n/)
            if docList.length > 0
              docList.each do |doc|
                new_doc, new_doc_str =  assembly_error_message(doc, "Could not access URL url=#{url},message=#{exception.message}", 'insert_table_fail')
                save_to_disk(new_doc_str, "入库失败数据")
              end
            end
          end
          return
      end

      if con_count >= @automatic_retries
        host = ""
        con_count = 0
      end

      @logger.info("Retrying connection", :url => url, :uuid => uuid)
      delay_attempt(con_count, @backoff_time)
      make_request(documents, hosts, query, con_count+1, req_count, host, uuid)
    end

    client.execute!
  end

  # This is split into a separate method mostly to help testing
  def log_failure(message, opts)
    @logger.error("[HTTP Output Failure] #{message}", opts)
  end

  def request_headers()
    headers = @headers || {}
    headers["Content-Type"] ||= "application/json"
    headers
  end

end
