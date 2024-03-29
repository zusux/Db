<?php
namespace zusux;
use Exception;

class Db{
    public static $ins =[];
    protected $dsn="mysql:host=%host%;dbname=%dbname%";
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
        'break_reconnect' => true,
    ];

    // PDO连接参数
    protected $params = [
        \PDO::ATTR_CASE              => \PDO::CASE_NATURAL,
        \PDO::ATTR_ERRMODE           => \PDO::ERRMODE_EXCEPTION,
        \PDO::ATTR_ORACLE_NULLS      => \PDO::NULL_NATURAL,
        \PDO::ATTR_STRINGIFY_FETCHES => false,
        \PDO::ATTR_EMULATE_PREPARES  => false,
        \PDO::ATTR_PERSISTENT => false, //持久连接

        \PDO::ATTR_TIMEOUT => 1, // in seconds

    ];

    private $links=[];
    /** @var PDO 当前连接ID */
    protected $linkID;

    // 字段属性大小写
    protected $attrCase = \PDO::CASE_LOWER;

    private function __construct(array $config = [])
    {
        if (!empty($config)) {
            $this->config = array_merge($this->config, $config);
        }
        $this->initConnect();
    }

    public static function instance($config=[]){
        $md5 = md5(serialize($config));
        if(!isset(self::$ins[$md5])){
            self::$ins[$md5] = new static($config);
        }else{
            if(!self::$ins[$md5]){
                self::$ins[$md5] = new static($config);
            }
        }
        $ins = self::$ins[$md5];
        $linkID = $ins->getConnect();
        if(!$linkID){
            $linkID = $ins->initConnect();
        }
        return new Build($linkID);
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
                    return $this->connect($config, $linkNum,$autoConnection);
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
    private function initConnect()
    {
        $this->close();
        $this->linkID = $this->connect([],0,true);
        return $this->linkID;
    }

    public function getConnect(){
        return $this->linkID;
    }


    private function parseDsn($config){
        return str_replace(['%mysql%','%host%','%port%','%dbname%'],[$config['type'],$config['hostname'],$config['hostport'],$config['database']],"%mysql%:host=%host%;port=%port%;dbname=%dbname%");
    }

    public function close()
    {
        $this->linkID    = null;
        $this->links     = [];
        return $this;
    }


    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        // 关闭连接
        $this->close();
    }
}

class Build{
    //pdo连接
    protected $linkID ;

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


    // SQL表达式
    protected $selectSql    = 'SELECT%DISTINCT% %FIELD% FROM %TABLE%%FORCE%%JOIN%%WHERE%%GROUP%%HAVING%%UNION%%ORDER%%LIMIT%%LOCK%%COMMENT%';
    protected $insertSql    = 'INSERT INTO %TABLE% (%FIELD%) VALUES (%DATA%) %COMMENT%';
    protected $maxSql    = 'SELECT MAX(%FIELD%) as result FROM %TABLE% ';
    protected $minSql    = 'SELECT Min(%FIELD%) as result FROM %TABLE% ';
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

    // 查询结果类型
    protected $fetchType = \PDO::FETCH_ASSOC;

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


    public function __construct($linkID)
    {
        $this->linkID = $linkID;
    }


    public function table(string $table){
        $this->table = $table;
        return $this;
    }

    public function alias(string $alias){
        $this->alias = $alias;
        return $this;
    }

    public function distinct(string $distinct){
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

    /**
     * @param string $field
     * @param $value array|string
     * @param string $condition
     * @return $this
     */
    public function where(string $field, $value,string $condition='='){

        if(is_array($value)){
            $fullBindArr = [];
            foreach($value as $one){
                $bindField = 'w_'.$field;
                $time = $this->tempCount[$bindField] ?? 0;
                $this->tempCount[$bindField] = $this->tempCount[$bindField] ?? 0;
                $this->tempCount[$bindField]++;
                $fullBindField = $bindField.$time;
                $this->whereBind[$fullBindField] = $one;
                $fullBindArr[] = ':'.$fullBindField;
            }
            if($fullBindArr){
                $instr = implode(',',$fullBindArr);
                $this->where[] = implode(' ',['`'.$field.'`',$condition,'('.$instr.')']);;
            }else{
                throw new Exception('value 是数组类型不能为空值');
            }
        }else{
            $bindField = 'w_'.$field;
            $time = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField] = $this->tempCount[$bindField] ?? 0;
            $this->tempCount[$bindField]++;
            $fullBindField = $bindField.$time;

            $this->where[] = implode(' ',['`'.$field.'`',$condition,':'.$fullBindField]);
            $this->whereBind[$fullBindField] = $value;
        }


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
        $this->order[] =  implode(' ',[$field ,$order]);
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

    protected function buildMinSql($filed){
        $sql = str_replace(
            [
                '%FIELD%',
                '%TABLE%',
            ],
            [
                $filed,
                $this->table.' '.$this->alias,
            ],
            $this->minSql
        );
        return $sql;
    }
    public function min($field){
        $sql = $this->buildMinSql($field);
        if($this->fetchSql){
            $result = $this->getRealSql($sql, []);
        }else{
            $result = $this->query($sql,[]);
        }
        $this->reset();
        $return = current($result);
        return $return['result'] ?? null;
    }

    protected function buildMaxSql($filed){
        $sql = str_replace(
            [
                '%FIELD%',
                '%TABLE%',
            ],
            [
                $filed,
                $this->table.' '.$this->alias,
            ],
            $this->maxSql
        );
        return $sql;
    }
    public function max($field){
        $sql = $this->buildMaxSql($field);
        if($this->fetchSql){
            $result = $this->getRealSql($sql, []);
        }else{
            $result = $this->query($sql,[]);
        }
        $this->reset();
        $return = current($result);
        return $return['result'] ?? null;
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

    public function value($field){
        $this->limit = "";
        $this->field([$field]);
        $sql = $this->buildSelectSql();

        if($this->fetchSql){
            $result = $this->getRealSql($sql, array_merge($this->whereBind,$this->havingBind));
            return $result;
        }else{
            $result = $this->query($sql,array_merge($this->whereBind,$this->havingBind),false,false,false);
            $this->reset();
            if($result){
                return $result[$field];
            }else{
                return null;
            }
        }
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
        $limitStr = $number ?  " limit ".$number.",".$limit : " limit ".$limit;
        $this->limit = $limitStr;
        return $this;
    }


    public function query($sql, $bind = [],$fetchAll=true)
    {
        if (!$this->linkID) {
            return false;
        }
        // 记录SQL语句
        $this->queryStr = $sql;
        if ($bind) {
            $this->bind = $bind;
        }
        try {
            // 预处理
            $this->PDOStatement = $this->linkID->prepare($sql);
            // 执行查询
            $this->PDOStatement->execute($bind);
            // 返回结果集
            return $this->getResult($fetchAll);
        } catch (\PDOException $e) {
            if ($this->isBreak($e)) {
                $this->linkID = Db::$ins->initConnect();
                return $this->free()->query($sql, $bind);
            }
            throw new \PDOException($e);
        } catch (\Throwable $e) {
            if ($this->isBreak($e)) {
                $this->linkID = Db::$ins->initConnect();
                return $this->free()->query($sql, $bind);
            }
            throw $e;
        } catch (\Exception $e) {
            if ($this->isBreak($e)) {
                $this->linkID = Db::$ins->initConnect();
                return $this->free()->query($sql, $bind);
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

        if (!$this->linkID) {
            return false;
        }

        // 记录SQL语句
        $this->queryStr = $sql;
        if ($bind) {
            $this->bind = $bind;
        }

        try {
            // 预处理
            $this->PDOStatement = $this->linkID->prepare($sql);
            // 执行语句
            $this->PDOStatement->execute($bind);

            $this->numRows = $this->PDOStatement->rowCount();
            return $this->numRows;
        } catch (\PDOException $e) {
            if ($this->isBreak($e)) {
                $this->linkID = Db::$ins->initConnect();
                return $this->free()->execute($sql, $bind);
            }
            throw new \PDOException($e);
        } catch (\Throwable $e) {
            if ($this->isBreak($e)) {
                $this->linkID = Db::$ins->initConnect();
                return $this->free()->execute($sql, $bind);
            }
            throw $e;
        } catch (\Exception $e) {
            if ($this->isBreak($e)) {
                $this->linkID = Db::$ins->initConnect();
                return $this->free()->execute($sql, $bind);
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
    protected function getResult($fetchAll = true)
    {
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
     * 是否断线
     * @access protected
     * @param \PDOException|\Exception  $e 异常对象
     * @return bool
     */
    protected function isBreak($e)
    {
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

    /**
     * 释放查询结果
     * @access public
     */
    public function free()
    {
        $this->PDOStatement = null;
        return $this;
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

    public function close(){
        $this->linkID = null;
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        $this->reset();
        $this->linkID = null;
        // 释放查询
        if ($this->PDOStatement) {
            $this->free();
        }
    }
}
