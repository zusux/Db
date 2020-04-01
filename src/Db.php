<?php
namespace zusux;
use Exception;
class Db{

    protected static $queryTimes=0;
    protected static $executeTimes=0;

    protected $config = [
        // 数据库类型
        'type'            => 'mysql',
        // 服务器地址
        'hostname'        => '',
        // 数据库名
        'database'        => '',
        // 用户名
        'username'        => '',
        // 密码
        'password'        => '',
        // 端口
        'hostport'        => '3306',
        // 连接dsn
        'dsn'             => '',
        // 数据库连接参数
        'params'          => [],
        // 数据库编码默认采用utf8
        'charset'         => 'utf8',
        // 数据库表前缀
        'prefix'          => '',
        // 数据库调试模式
        'debug'           => false,
        // 数据库部署方式:0 集中式(单一服务器),1 分布式(主从服务器)
        'deploy'          => 0,
        // 数据库读写是否分离 主从式有效
        'rw_separate'     => false,
        // 读写分离后 主服务器数量
        'master_num'      => 1,
        // 指定从服务器序号
        'slave_no'        => '',
        // 模型写入后自动读取主服务器
        'read_master'     => false,
        // 是否严格检查字段是否存在
        'fields_strict'   => true,
        // 数据返回类型
        'result_type'     => \PDO::FETCH_ASSOC,
        // 数据集返回类型
        'resultset_type'  => 'array',
        // 自动写入时间戳字段
        'auto_timestamp'  => false,
        // 时间字段取出后的默认时间格式
        'datetime_format' => 'Y-m-d H:i:s',
        // 是否需要进行SQL性能分析
        'sql_explain'     => false,
        // 是否需要断线重连
        'break_reconnect' => false,
    ];

    // PDO连接参数
    protected $params = [
        \PDO::ATTR_CASE              => \PDO::CASE_NATURAL,
        \PDO::ATTR_ERRMODE           => \PDO::ERRMODE_EXCEPTION,
        \PDO::ATTR_ORACLE_NULLS      => \PDO::NULL_NATURAL,
        \PDO::ATTR_STRINGIFY_FETCHES => false,
        \PDO::ATTR_EMULATE_PREPARES  => false,
        \PDO::ATTR_PERSISTENT => true //持久连接
    ];
    protected $fetchSql=false;
    protected $table = "";
    protected $alias = "";
    protected $distinct = "";
    protected $where = [];
    protected $whereCond = 'and'; //全局连接条件
    protected $group = [];
    protected $join = [];
    protected $order = [];
    protected $field = "*";
    protected $data = [];
    protected $having = [];
    protected $havingCond = 'and';
    protected $limit = "";
    protected $force = "";
    protected $union = "";
    protected $lock = "";
    protected $comment = "";

    protected $whereBind = [];
    protected $havingBind = [];
    protected $tempCount = [];
    protected $dataBind = [];

    // 事务指令数
    protected $transTimes = 0;

    private $links=[];
    /** @var PDO 当前连接ID */
    protected $linkID;
    protected $linkRead;
    protected $linkWrite;
    // 查询结果类型
    protected $fetchType = \PDO::FETCH_ASSOC;
    // 字段属性大小写
    protected $attrCase = \PDO::CASE_LOWER;

    protected $dsn="mysql:host=%host%;dbname=%dbname%";
    // SQL表达式
    protected $selectSql    = 'SELECT%DISTINCT% %FIELD% FROM %TABLE%%FORCE%%JOIN%%WHERE%%GROUP%%HAVING%%UNION%%ORDER%%LIMIT%%LOCK%%COMMENT%';
    protected $insertSql    = 'INSERT INTO %TABLE% (%FIELD%) VALUES (%DATA%) %COMMENT%';
    protected $insertAllSql = 'INSERT INTO %TABLE% (%FIELD%) VALUES %DATA% %COMMENT%';
    protected $updateSql    = 'UPDATE %TABLE% SET %SET% %JOIN% %WHERE% %ORDER%%LIMIT% %LOCK%%COMMENT%';
    protected $deleteSql    = 'DELETE FROM %TABLE% %USING% %JOIN% %WHERE% %ORDER%%LIMIT% %LOCK%%COMMENT%';

    /** @var PDOStatement PDO操作实例 */
    protected $PDOStatement;

    /** @var string 当前SQL指令 */
    protected $queryStr = '';
    // 返回或者影响记录数
    protected $numRows = 0;
    // 绑定参数
    protected $bind = [];


    public function __construct(array $config = [])
    {
        if (!empty($config)) {
            $this->config = array_merge($this->config, $config);
        }
        $this->reset();
        //$this->connect();
        //$this->linkID = $this->getConnection();

    }

    public function reset(){
        $this->fetchSql = false;
        $this->whereCond = 'and';
        $this->havingCond = 'and';
        $this->alias = "";
        $this->distinct = "";
        $this->field = "*";
        $this->limit = "";
        $this->force = "";
        $this->comment = "";
        $this->lock = "";

        $this->where = [];
        $this->group = [];
        $this->join = [];
        $this->order = [];
        $this->having = [];

        $this->data = [];
        $this->havingBind = [];
        $this->whereBind = [];
        $this->tempCount =[];
    }


    /**
     * 连接数据库方法
     * @access public
     * @param array         $config 连接参数
     * @param integer       $linkNum 连接序号
     * @param array|bool    $autoConnection 是否自动连接主数据库（用于分布式）
     * @return PDO
     * @throws Exception
     */
    public function connect(array $config = [], $linkNum = 0, $autoConnection = false)
    {
        if (!isset($this->links[$linkNum])) {
            if (!$config) {
                $config = $this->config;
            } else {
                $config = array_merge($this->config, $config);
            }
            // 连接参数
            if (isset($config['params']) && is_array($config['params'])) {
                $params = $config['params'] + $this->params;
            } else {
                $params = $this->params;
            }
            // 记录当前字段属性大小写设置
            $this->attrCase = $params[\PDO::ATTR_CASE];

            // 数据返回类型
            if (isset($config['result_type'])) {
                $this->fetchType = $config['result_type'];
            }
            try {
                if (empty($config['dsn'])) {
                    $config['dsn'] = $this->parseDsn($config);
                }
                $this->links[$linkNum] = new \PDO($config['dsn'], $config['username'], $config['password'], $params);
            } catch (\PDOException $e) {
                if ($autoConnection) {
                    return $this->connect($autoConnection, $linkNum);
                } else {
                    throw $e;
                }
            }
        }
        return $this->links[$linkNum];
    }

    /**
     * 初始化数据库连接
     * @access protected
     * @param boolean $master 是否主服务器
     * @return void
     */
    protected function initConnect($master = true)
    {
        if (!empty($this->config['deploy'])) {
            // 采用分布式数据库
            if ($master || $this->transTimes) {
                if (!$this->linkWrite) {
                    $this->linkWrite = $this->multiConnect(true);
                }
                $this->linkID = $this->linkWrite;
            } else {
                if (!$this->linkRead) {
                    $this->linkRead = $this->multiConnect(false);
                }
                $this->linkID = $this->linkRead;
            }
        } elseif (!$this->linkID) {
            // 默认单数据库
            $this->linkID = $this->connect();
        }
    }

    /**
     * 连接分布式服务器
     * @access protected
     * @param boolean $master 主服务器
     * @return PDO
     */
    protected function multiConnect($master = false)
    {
        $_config = [];
        // 分布式数据库配置解析
        foreach (['username', 'password', 'hostname', 'hostport', 'database', 'dsn', 'charset'] as $name) {
            $_config[$name] = explode(',', $this->config[$name]);
        }

        // 主服务器序号
        $m = floor(mt_rand(0, $this->config['master_num'] - 1));

        if ($this->config['rw_separate']) {
            // 主从式采用读写分离
            if ($master) // 主服务器写入
            {
                $r = $m;
            } elseif (is_numeric($this->config['slave_no'])) {
                // 指定服务器读
                $r = $this->config['slave_no'];
            } else {
                // 读操作连接从服务器 每次随机连接的数据库
                $r = floor(mt_rand($this->config['master_num'], count($_config['hostname']) - 1));
            }
        } else {
            // 读写操作不区分服务器 每次随机连接的数据库
            $r = floor(mt_rand(0, count($_config['hostname']) - 1));
        }
        $dbMaster = false;
        if ($m != $r) {
            $dbMaster = [];
            foreach (['username', 'password', 'hostname', 'hostport', 'database', 'dsn', 'charset'] as $name) {
                $dbMaster[$name] = isset($_config[$name][$m]) ? $_config[$name][$m] : $_config[$name][0];
            }
        }
        $dbConfig = [];
        foreach (['username', 'password', 'hostname', 'hostport', 'database', 'dsn', 'charset'] as $name) {
            $dbConfig[$name] = isset($_config[$name][$r]) ? $_config[$name][$r] : $_config[$name][0];
        }
        return $this->connect($dbConfig, $r, $r == $m ? false : $dbMaster);
    }

    public function getConnection($linkNum=0){
        return $this->links[$linkNum] ?? false;
    }


    protected function parseDsn($config){
        return str_replace(['%mysql%','%host%','%port%','%dbname%'],[$config['type'],$config['hostname'],$config['hostport'],$config['database']],"%mysql%:host=%host%;port=%port%;dbname=%dbname%");
    }

    public function table(string $table){
        $this->table = $table;
        return $this;
    }

    public function alias(string $alias){
        $this->alias = $alias;
        return $this;
    }

    public function  distinct(string $distinct){
        $this->distinct = $distinct;
        return $this;
    }

    public function field(array $fields=[]){
        if($fields){
            $temp = [];
            foreach($fields as $field){
                if(trim($field) != '*'){
                    $temp[] = '`'.trim($field).'`';
                }else{
                    $temp[] = trim($field);
                }
            }
            $this->field = implode(',',$temp);
        }
        return $this;
    }

    public function join(string $table,string $condition,string $type='inner'){

        $this->join[] =  implode(' ',[$type,'join',$table,$condition]);
        return $this;
    }

    /**
     * @param array $whereS =[ ['print',5,'='],['print',6,'='] ]
     * @param string $condition ='or|and'
     * @return $this
     */
    public function whereGroup(array $whereS,$condition='and'){
        $tempWhere = [];
        foreach($whereS as $k=>$where){
            $bindField = 'w_'.$where[0];
            $time = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField] = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField]++;
            $fullBindField = $bindField.$time;
            $tempWhere[] = implode(' ',['`'.$where[0].'`',$where[2] ?? '=',':'.$fullBindField]);
            $this->whereBind[$fullBindField] = $where[1];
        }
        $this->where[] = ' ( '.implode(' '.$condition.' ',$tempWhere).' ) ';
        return $this;
    }
    public function whereCond(string $cond='and'){
        $this->whereCond = $cond;
        return $this;
    }
    public function havingCond(string $cond='and'){
        $this->havingCond = $cond;
        return $this;
    }
    public function where(string $field,string $value,string $condition='='){
        $bindField = 'w_'.$field;
        $time = $this->tempCount[$bindField] ?? 0;
        $this->tempCount[$bindField] = $this->tempCount[$bindField] ?? 0;
        $this->tempCount[$bindField]++;
        $fullBindField = $bindField.$time;

        $this->where[] = implode(' ',['`'.$field.'`',$condition,':'.$fullBindField]);
        $this->whereBind[$fullBindField] = $value;
        return $this;
    }

    /**
     * @param array $whereS =[ ['print',5,'='],['print',6,'='] ]
     * @param string $condition ='or|and'
     * @return $this
     */
    public function havingGroup(array $havingS,$condition='and'){
        $tempHaving = [];
        foreach($havingS as $k=>$having){
            $bindField = 'h_'.$having[0];
            $time = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField] = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField]++;
            $fullBindField = $bindField.$time;
            $tempHaving[] = implode(' ',['`'.$having[0].'`',$having[2] ?? '=',':'.$fullBindField]);
            $this->havingBind[$fullBindField] = $having[1];
        }
        $this->having[] = ' ( '.implode(' '.$condition.' ',$tempHaving).' ) ';
        return $this;
    }

    public function  having(string $field,string $value,string $condition='='){

        $bindField = 'h_'.$field;
        $time = $this->tempCount[$bindField] ?? 0;
        $this->tempCount[$bindField] = $this->tempCount[$bindField] ?? 0;
        $this->tempCount[$bindField]++;
        $fullBindField = $bindField.$time;

        $this->having[] = implode(' ',['`'.$field.'`',$condition,':'.$fullBindField]);
        $this->havingBind[$fullBindField] = $value;
        return $this;
    }

    public function order(string $field,string $order='asc'){
        $this->order[] =  implode(' ',['`'.$field.'`',$order]);
        return $this;
    }

    public function group(string $field){
        $this->group[] = '`'.$field.'`';
        return $this;
    }

    public function select(){
        $sql = $this->buildSelectSql();
        if($this->fetchSql){
            $result = $this->getRealSql($sql, array_merge($this->whereBind,$this->havingBind));
        }else{
            $result = $this->query($sql,array_merge($this->whereBind,$this->havingBind));
        }
        $this->reset();
        return $result;
    }
    protected function buildSelectSql(){

    $whereStr = implode(' '.$this->whereCond.' ',$this->where);
    $havingStr = implode(' '.$this->havingCond.' ',$this->having);
    $groupStr = implode(',',$this->group);
    $orderStr = implode(',',$this->order);

    $sql = str_replace(
        [
            '%DISTINCT%',
            '%FIELD%',
            '%TABLE%',
            '%FORCE%',
            '%JOIN%',
            '%WHERE%',
            '%GROUP%',
            '%HAVING%',
            '%UNION%',
            '%ORDER%',
            '%LIMIT%',
            '%LOCK%',
            '%COMMENT%'
        ],
        [
            $this->distinct,
            $this->field,
            $this->table.' '.$this->alias,
            $this->force,
            implode(' ',$this->join),
            $whereStr? ' where '.$whereStr : " ",
            $groupStr? ' group by '.$groupStr: " ",
            $havingStr? ' having '.$havingStr: " ",
            $this->union,
            $orderStr? ' order by '.$orderStr:" ",
            $this->limit,
            $this->lock,
            $this->comment,

        ],
        $this->selectSql
    );
    return $sql;
}


    /**
     * 生成update SQL
     * @access public
     * @param array     $data 数据
     * @param array     $options 表达式
     * @return string
     */
    public function update($data)
    {
        $setArr = [];
        $dataBind = [];
        foreach($data as $k=>$v){
            $bindField = 'd_'.$k;
            $time = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField] = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField]++;
            $fullBindField = $bindField.$time;
            $setArr[] = "`$k` = :$fullBindField";
            $dataBind[$fullBindField] = $v;
        }
        $sql = $this->buildUpdateSql($setArr);

        if($this->fetchSql){
            $result = $this->getRealSql($sql, array_merge($dataBind,$this->whereBind));
        }else{
            $result = $this->execute($sql,array_merge($dataBind,$this->whereBind));
        }


        $this->reset();
        return $result;
    }
    protected function buildUpdateSql(array $setArr){
        $whereStr = implode(' '.$this->whereCond.' ',$this->where);
        $orderStr = implode(',',$this->order);
        $sql = str_replace(
            [
                '%TABLE%',
                '%SET%',
                '%JOIN%',
                '%WHERE%',
                '%ORDER%',
                '%LIMIT%',
                '%LOCK%',
                '%COMMENT%'
            ],
            [
                $this->table.' '.$this->alias,
                $setArr? implode(' , ',$setArr):"",
                implode(' ',$this->join),
                $whereStr? ' where '.$whereStr : " ",
                $orderStr? 'order by '.$orderStr:" ",
                $this->limit,
                $this->lock,
                $this->comment,

            ],
            $this->updateSql
        );
        return $sql;
    }


    protected function buildInsertSql(array $data,$replace = false){

        $fields = array_keys($data);
        $values = array_values($data);

        $sql = str_replace(
            [
                '%INSERT%',
                '%TABLE%',
                '%FIELD%',
                '%DATA%',
                '%COMMENT%'
            ],
            [
                $replace ? 'REPLACE' : 'INSERT',
                $this->table.' '.$this->alias,
                implode(' , ', $fields),
                implode(' , ', $values),
                $this->comment,

            ],
            $this->insertSql
        );
        return $sql;
    }


    public function insert(array $data,$replace = false){

        $setArr = [];
        $dataBind = [];
        foreach($data as $k=>$v){
            $bindField = 'd_'.$k;
            $time = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField] = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField]++;
            $fullBindField = $bindField.$time;
            $setArr['`'.$k.'`'] = ":".$fullBindField;
            $dataBind[$fullBindField] = $v;
        }
        $sql = $this->buildInsertSql($setArr,$replace);

        if($this->fetchSql){
            $result = $this->getRealSql($sql, array_merge($dataBind));
        }else{
            $result = $this->execute($sql,$dataBind);
        }

        $this->reset();
        return $result;
    }

    public function insertAll(array $data,$replace = false){

        $fields = [];
        $values = [];
        $dataBind = [];
        foreach($data as $k=>$arr){
            $valueItem = [];
            foreach($arr as $field=>$value){
                $bindField = 'd_'.$field;
                $fullBindField = $bindField.$k;
                $optionField = ":".$fullBindField;

                $valueItem[$field] = $optionField;
                $fields[$field] = '`'.$field.'`';
                $dataBind[$fullBindField] = $value;
            }
            $values[] = '( '.implode(',',array_values($valueItem)).' )';
        }
        $sql = $this->buildInsertAllSql(array_values($fields),$values,$replace);

        if($this->fetchSql){
            $result = $this->getRealSql($sql, $dataBind);
        }else{
            $result = $this->execute($sql,$dataBind);
        }

        $this->reset();
        return $result;
    }

    protected function buildInsertAllSql(array $fields,array $values, $replace = false){
        $sql = str_replace(
            [
                '%INSERT%',
                '%TABLE%',
                '%FIELD%',
                '%DATA%',
                '%COMMENT%'
            ],
            [
                $replace ? 'REPLACE' : 'INSERT',
                $this->table.' '.$this->alias,
                implode(' , ', $fields),
                implode(' , ', $values),
                $this->comment,

            ],
            $this->insertAllSql
        );
        return $sql;
    }

    public function find(){
        $this->limit = "";
        $sql = $this->buildSelectSql();

        if($this->fetchSql){
            $result = $this->getRealSql($sql, array_merge($this->whereBind,$this->havingBind));
        }else{
            $result = $this->query($sql,array_merge($this->whereBind,$this->havingBind),false,false,false);
        }

        $this->reset();
        return $result;
    }

    protected function buildDeleteSql(){
        if(!$this->where){
            throw new \Exception('不允许全表删除');
        }

        $whereStr = implode(' '.$this->whereCond.' ',$this->where);
        $orderStr = implode(',',$this->order);
        $sql = str_replace(
            [
                '%TABLE%',
                '%USING%',
                '%JOIN%',
                '%WHERE%',
                '%ORDER%',
                '%LIMIT%',
                '%LOCK%',
                '%COMMENT%'
            ],
            [
                $this->table.' '.$this->alias,
                ' ',
                implode(' ',$this->join),
                $whereStr? ' where '.$whereStr : " ",
                $orderStr? ' order by '.$orderStr:" ",
                $this->limit,
                $this->lock,
                $this->comment,

            ],
            $this->deleteSql
        );
        return $sql;
    }
    public function delete(){
        $sql = $this->buildDeleteSql();


        if($this->fetchSql){
            $result = $this->getRealSql($sql, $this->whereBind);
        }else{
            $result = $this->execute($sql,$this->whereBind);
        }

        $this->reset();
        return $result;
    }

    public function limit(int $limit,?int $number=null){
        $limitStr = $number ?  " limit ".$limit.",".$number : " limit ".$limit;
        $this->limit = $limitStr;
        return $this;
    }



    /**
     * 存储过程的输入输出参数绑定
     * @access public
     * @param array $bind 要绑定的参数列表
     * @return void
     * @throws Exception
     */
    protected function bindParam($bind)
    {
        foreach ($bind as $key => $val) {
            $param = is_numeric($key) ? $key + 1 : ':' . $key;
            if (is_array($val)) {
                array_unshift($val, $param);
                $result = call_user_func_array([$this->PDOStatement, 'bindParam'], $val);
            } else {
                $result = $this->PDOStatement->bindValue($param, $val);
            }
            if (!$result) {
                $param = array_shift($val);
                throw new \Exception(
                    "Error occurred  when binding parameters '{$param}'\r\n".
                    $this->getLastsql()."\r\n"
                );
            }
        }
    }

    /**
     * 参数绑定
     * 支持 ['name'=>'value','id'=>123] 对应命名占位符
     * 或者 ['value',123] 对应问号占位符
     * @access public
     * @param array $bind 要绑定的参数列表
     * @return void
     * @throws Exception
     */
    protected function bindValue(array $bind = [])
    {
        foreach ($bind as $key => $val) {
            // 占位符
            $param = is_numeric($key) ? $key + 1 : ':' . $key;
            if (is_array($val)) {
                if (\PDO::PARAM_INT == $val[1] && '' === $val[0]) {
                    $val[0] = 0;
                }
                $result = $this->PDOStatement->bindValue($param, $val[0], $val[1]);
            } else {
                $result = $this->PDOStatement->bindValue($param, $val);
            }
            if (!$result) {
                throw new \Exception(
                    "Error occurred  when binding parameters '{$param}'\r\n".
                    $this->getLastsql()."\r\n"
                );
            }
        }
    }

    public function query($sql, $bind = [], $master = false, $pdo = false,$fetchAll=true)
    {
        $this->initConnect($master);
        if (!$this->linkID) {
            return false;
        }

        // 记录SQL语句
        $this->queryStr = $sql;
        if ($bind) {
            $this->bind = $bind;
        }

        Db::$queryTimes++;
        try {

            // 预处理
            $this->PDOStatement = $this->linkID->prepare($sql);

            // 是否为存储过程调用
            $procedure = in_array(strtolower(substr(trim($sql), 0, 4)), ['call', 'exec']);
            // 参数绑定
            if ($procedure) {

                $this->bindParam($bind);
            } else {
                $this->bindValue($bind);
            }
            // 执行查询
            $this->PDOStatement->execute();
            // 返回结果集
            return $this->getResult($pdo, $procedure,$fetchAll);
        } catch (\PDOException $e) {
            if ($this->isBreak($e)) {
                return $this->close()->query($sql, $bind, $master, $pdo);
            }
            throw new \PDOException($e, $this->config, $this->getLastsql());
        } catch (\Throwable $e) {
            if ($this->isBreak($e)) {
                return $this->close()->query($sql, $bind, $master, $pdo);
            }
            throw $e;
        } catch (\Exception $e) {
            if ($this->isBreak($e)) {
                return $this->close()->query($sql, $bind, $master, $pdo);
            }
            throw $e;
        }
    }

    /**
     * 执行语句
     * @access public
     * @param  string        $sql sql指令
     * @param  array         $bind 参数绑定
     * @param  Query         $query 查询对象
     * @return int
     * @throws PDOException
     * @throws \Exception
     */
    public function execute($sql, $bind = [], $query = null)
    {
        $this->initConnect(true);
        if (!$this->linkID) {
            return false;
        }

        // 记录SQL语句
        $this->queryStr = $sql;
        if ($bind) {
            $this->bind = $bind;
        }

        self::$executeTimes++;


        try {
            // 预处理
            $this->PDOStatement = $this->linkID->prepare($sql);

            // 是否为存储过程调用
            $procedure = in_array(strtolower(substr(trim($sql), 0, 4)), ['call', 'exec']);
            // 参数绑定
            if ($procedure) {
                $this->bindParam($bind);
            } else {
                $this->bindValue($bind);
            }
            // 执行语句
            $this->PDOStatement->execute();

            $this->numRows = $this->PDOStatement->rowCount();
            return $this->numRows;
        } catch (\PDOException $e) {
            if ($this->isBreak($e)) {
                return $this->close()->execute($sql, $bind, $query);
            }
            throw new \PDOException($e."\r\n"."\r\n".$this->getLastsql());
        } catch (\Throwable $e) {
            if ($this->isBreak($e)) {
                return $this->close()->execute($sql, $bind, $query);
            }
            throw $e;
        } catch (\Exception $e) {
            if ($this->isBreak($e)) {
                return $this->close()->execute($sql, $bind, $query);
            }
            throw $e;
        }
    }

    public function fetchSql(bool $fetchSql=true){
        $this->fetchSql = $fetchSql;
        return $this;
    }



    /**
     * 获得数据集数组
     * @access protected
     * @param bool   $pdo 是否返回PDOStatement
     * @param bool   $procedure 是否存储过程
     * @return PDOStatement|array
     */
    protected function getResult($pdo = false, $procedure = false,$fetchAll = true)
    {
        if ($pdo) {
            // 返回PDOStatement对象处理
            return $this->PDOStatement;
        }
        if ($procedure) {
            // 存储过程返回结果
            return $this->procedure();
        }
        if($fetchAll){
            $result        = $this->PDOStatement->fetchAll($this->fetchType);
            $this->numRows = count($result);
        }else{
            $result        = $this->PDOStatement->fetch($this->fetchType);
            $this->numRows = 1;
        }

        return $result;
    }

    /**
     * 获得存储过程数据集
     * @access protected
     * @return array
     */
    protected function procedure()
    {
        $item = [];
        do {
            $result = $this->getResult();
            if ($result) {
                $item[] = $result;
            }
        } while ($this->PDOStatement->nextRowset());
        $this->numRows = count($item);
        return $item;
    }

    /**
     * 是否断线
     * @access protected
     * @param \PDOException|\Exception  $e 异常对象
     * @return bool
     */
    protected function isBreak($e)
    {
        if (!$this->config['break_reconnect']) {
            return false;
        }

        $info = [
            'server has gone away',
            'no connection to the server',
            'Lost connection',
            'is dead or not enabled',
            'Error while sending',
            'decryption failed or bad record mac',
            'server closed the connection unexpectedly',
            'SSL connection has been closed unexpectedly',
            'Error writing data to the connection',
            'Resource deadlock avoided',
            'failed with errno',
        ];

        $error = $e->getMessage();

        foreach ($info as $msg) {
            if (false !== stripos($error, $msg)) {
                return true;
            }
        }
        return false;
    }
    public function close()
    {
        $this->linkID    = null;
        $this->linkWrite = null;
        $this->linkRead  = null;
        $this->links     = [];
        // 释放查询
        $this->free();
        return $this;
    }
    /**
     * 释放查询结果
     * @access public
     */
    public function free()
    {
        $this->PDOStatement = null;
    }

    /**
     * 获取最近一次查询的sql语句
     * @access public
     * @return string
     */
    public function getLastSql()
    {
        return $this->getRealSql($this->queryStr, $this->bind);
    }

    /**
     * 根据参数绑定组装最终的SQL语句 便于调试
     * @access public
     * @param string    $sql 带参数绑定的sql语句
     * @param array     $bind 参数绑定列表
     * @return string
     */
    public function getRealSql($sql, array $bind = [])
    {
        if (is_array($sql)) {
            $sql = implode(';', $sql);
        }

        foreach ($bind as $key => $val) {
            $value = is_array($val) ? $val[0] : $val;
            $type  = is_array($val) ? $val[1] : \PDO::PARAM_STR;
            if (\PDO::PARAM_STR == $type) {
                $value = $this->quote($value);
            } elseif (\PDO::PARAM_INT == $type) {
                $value = (float) $value;
            }
            // 判断占位符
            $sql = is_numeric($key) ?
                substr_replace($sql, $value, strpos($sql, '?'), 1) :
                str_replace(
                    [':' . $key . ')', ':' . $key . ',', ':' . $key . ' ', ':' . $key . PHP_EOL],
                    [$value . ')', $value . ',', $value . ' ', $value . PHP_EOL],
                    $sql . ' ');
        }
        return rtrim($sql);
    }

    /**
     * SQL指令安全过滤
     * @access public
     * @param string $str SQL字符串
     * @param bool   $master 是否主库查询
     * @return string
     */
    public function quote($str, $master = true)
    {
        return $this->linkID ? $this->linkID->quote($str) : $str;
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        $this->reset();
        // 释放查询
        if ($this->PDOStatement) {
            $this->free();
        }
        // 关闭连接
        $this->close();
    }
}


