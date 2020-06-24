<?php
namespace zusux;
use \Swoole\Database\PDOConfig;
use \Swoole\Database\PDOPool;
use Exception;
class Pool{

    protected $queryTimes=0;
    protected $executeTimes=0;

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

    /** @var 携程连接池 PDO操作实例 */
    protected static $pool;

    /** @var string 当前SQL指令 */
    protected $queryStr = '';
    // 返回或者影响记录数
    protected $numRows = 0;
    // 绑定参数
    protected $bind = [];

    private static $ins;


    public function __construct($config,$pool)
    {
        if($pool){
            self::$pool = $pool;
        }else{
            if(!self::$pool){
                self::$pool = new PDOPool((new PDOConfig)
                    ->withHost($config['hostname'])
                    ->withPort($config['hostport'])
                    ->withDbName($config['database'])
                    ->withCharset($config['charset'])
                    ->withUsername($config['username'])
                    ->withPassword($config['password'])
                );
            }
        }
    }

    public static function instance($config){
        if(!self::$pool){
            self::$pool = new PDOPool((new PDOConfig)
                ->withHost($config['hostname'])
                ->withPort($config['hostport'])
                ->withDbName($config['database'])
                ->withCharset($config['charset'])
                ->withUsername($config['username'])
                ->withPassword($config['password'])
            );
            self::$ins = new static($config,self::$pool);
        }else{
            self::$ins = new static($config,self::$pool);
        }
        return self::$ins;
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

    /**
     * @param string $field
     * @desc 获取值
     */
    public function value(string $field){
        $this->limit = "";
        $sql = $this->buildSelectSql();

        if($this->fetchSql){
            $result = $this->getRealSql($sql, array_merge($this->whereBind,$this->havingBind));
        }else{
            $result = $this->query($sql,array_merge($this->whereBind,$this->havingBind),false,false,false);
        }
        $this->reset();
        if(is_array($result)){
            return $result[$field] ?? null;
        }else{
            return $result;
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
        $limitStr = $number ?  " limit ".$limit.",".$number : " limit ".$limit;
        $this->limit = $limitStr;
        return $this;
    }



    public function query($sql, $bind = [])
    {
        // 记录SQL语句
        $this->queryStr = $sql;
        if ($bind) {
            $this->bind = $bind;
        }
        $this->queryTimes++;

        $db = self::$pool->get();
        $result = [];
        try {
            // 预处理
            $statement = $db->prepare($sql);
            // 执行查询
            $statement->execute($bind);
            // 返回结果集
            $result = $statement->fetchAll();
            self::$pool->put($db);
        } catch (\PDOException $e) {
            echo "[pdo query error] ".$e->getMessage().PHP_EOL;
            self::$pool->put($db);
        }
        return $result;
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
    public function execute($sql, $bind = [])
    {
        // 记录SQL语句
        $this->queryStr = $sql;
        if ($bind) {
            $this->bind = $bind;
        }

        $this->executeTimes++;

        $db = self::$pool->get();
        $this->numRows = 0;
        $result = false;
        try {
            // 预处理
            $statement = $db->prepare($sql);

            // 执行语句
            $result = $statement->execute($bind);
            //获取自增id
            $id = $db->lastInsertId();
            if($result && $id){
                $result = $id;
            }
            $this->numRows = $statement->rowCount();
            self::$pool->put($db);
        } catch (\PDOException $e) {
            echo "[pdo execute error] ".$e->getMessage().PHP_EOL;
            self::$pool->put($db);
        }
        return $result;
    }

    public function fetchSql(bool $fetchSql=true){
        $this->fetchSql = $fetchSql;
        return $this;
    }


    public function close()
    {
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
        $pdo = self::$pool->get();
        $quote = $pdo->quote($str);
        self::$pool->put($pdo);
        return $quote;
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        $this->reset();
        // 释放查询
        $this->free();
        // 关闭连接
        $this->close();
    }
}


