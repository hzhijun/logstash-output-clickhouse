# 用法

    output {
      clickhouse {
        headers => {
          "X-ClickHouse-User" => "username"
          "X-ClickHouse-Key" => "password"
        }
        http_hosts => ["http://your.clickhouse1", "http://your.clickhouse2"]
        table => "table_name"
        mutations => {
          "to1" => "from1"
          "to2" => [ "from2", "string"]
        }
      }
    }

# 参数列表
### 必填参数
* `http_hosts`: array，主机地址，可以填写多个。
* `table`: string，要插入的表名称，格式：db_name.table_name 。
### 选填参数
* `headers`: hash，添加额外的头信息，如添加 clickhouse 安全认证。
* `mutations`: hash，日志中的字段与 clickhouse 表字段关系映射。
      
      示例如下：
      mutations => {
            "to1" => "from1"
            "to2" => [ "from2", "string"]
      }
      to1：clickhouse 表字段
      from1：日志中的字段

      to2：clickhouse 表字段
      from2：日志中的字段
      string：字段类型，如果校验失败，会将具体失败原因记录到 {save_dir}/{错误类型}/{小时}#{table}.json 文件中
  
      支持动态验证的类型有：'string','integer','float','date','datetime','datetime64'
      
      时间类型特殊说明：
      date:"YYYY-mm-dd"
      datetime:"YYYY-mm-dd HH:MM:SS"
      datetime64:"YYYY-mm-dd HH:MM:SS.LLL"

      mutations 内容转换到 mutation_path 文件中
      {
        "to1": "from1",
        "to2": ["from2","string"]
      }
  
* `mutation_path`: string，配置映射关系的外部文件（绝对路径），与 mutations 功能相同，当 mutations 存在时，优先使用 mutations。
* `flush_size`: integer，默认 50，批量处理数据的大小，。
* `idle_flush_time`: integer，默认 5，最多发送等待时间。
* `pool_max`: integer，
* `save_dir`: string，默认 /tmp，失败的请求正文将被保存的目录。
* `request_tolerance`: integer，默认 5，如果响应状态代码不是 200，则 http 请求发送重试的次数。
* `backoff_time`: integer，默认 3， 等待下一次连接或请求重试的时间。
* `automatic_retries`: integer，默认 1，尝试连接每个主机的次数 http_hosts。
* `host_resolve_ttl_sec`: integer，默认 120，每个主机解析的时间。

Default batch size is 50, with a wait of at most 5 seconds per send. These can be tweaked with the parameters `flush_size` and `idle_flush_time` respectively.

# 使用 Docker 构建 

克隆当前项目到本地，并到本地项目根目录执行命令: `docker build -t logstash-output-clickhouse .`

以下是 Dockerfile 文件内容
    
    FROM logstash:6.8.0
    
    WORKDIR /data/logstash-output-clickhouse/
    
    COPY ./ ./
    
    # 使用 root 用户访问
    USER root
    
    # 安装 rubygem（ruby 包管理器）
    RUN  yum install rubygems -y \
    # 删除默认的 rubygem 源地址
    && gem sources -r https://rubygems.org/ \
    # 添加 rubygem 国内源地址
    && gem sources -a http://gems.ruby-china.com/ \
    # 更新源的缓存
    && gem sources -u \
    && gem build logstash-output-clickhouse.gemspec \
    && logstash-plugin install logstash-output-clickhouse-0.1.0.gem \
    && rm -f /etc/localtime \
    # 设置时区
    && ln -sv /usr/share/zoneinfo/Asia/Chongqing /etc/localtime \
    && echo "Asia/Chongqing" > /etc/timezone

# 贡献

这个插件是 funcmike-logstash-output-clickhouse 的修改版本，该插件可[在此处获得](https://github.com/funcmike/logstash-output-clickhouse).