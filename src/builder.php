<?php
/**
 *
 * This script generates safe SQL queries.
 * 
 * Author: Osvaldo Jiang <osvaldo@publitar.com>
 *
 */
namespace choval;


class builder {


  protected static $tempHandlerFile;
  protected static $tempHandler;




  /**
   *
   * Returns the database handler instance.
   * This is a temporary SQLite3 handler used for preparing and binding values.
   *
   */
  protected static function getTempHandler() {
    if(empty(static::$tempHandler)) {
      static::$tempHandlerFile = tempnam( sys_get_temp_dir(), 'chovalBuilder' );
      static::$tempHandler = new \SQLite3(static::$tempHandlerFile);
      register_shutdown_function( array( __CLASS__, 'closeTempHandler'));
    }
    return static::$tempHandler;
  }




  /**
   *
   * Closes the database handler if any.
   * This function is registed to run on shutdown when getTempHandler is called.
   * 
   */
  public static function closeTempHandler() {
    if(static::$tempHandler) {
      if(static::$tempHandler->close()) {
        static::$tempHandler = null;
      }
      if(is_file(static::$tempHandlerFile)) {
        @unlink(static::$tempHandlerFile);
      }
    }
  }




  /**
   *
   * Checks for valid table/column names.
   * https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
   *
   * Note: 
   *   - Not compatible with UTF8 names.
   *   - TODO: Above
   *
   */
  protected static function checkIdentifier(string $id) {
    $exp = '/^[a-zA-Z0-9_\-\$]$/'; 
    if(preg_match($exp, $id)) {
      return $id;
    }
    return false;
  }




  protected $primaryKeys = [];
  protected $handler;
  protected $handlerType;
  protected $driver;

  protected $prepareFn = 'prepare';
  protected $bindFn = 'bindValue';
  protected $getSqlFn = 'getSQL';




  /**
   *
   * Constructor
   *
   */
  public function __construct($format=null) {

    /*
    $keysSet = false;
    $handlerSet = false;
    $vars = func_get_args();
    foreach($vars as $var) {
      if(is_object($var)) {
        if($handlerSet) {
          continue;
        }
        $handlerSet = $this->setHandler($var);
      } else if(is_array($var)) {
        if($keysSet) {
          continue;
        }
        $keysSet = $this->setPrimaryKeys($var);
      }
    }
    */
    
  }



  
  /**
   *
   * Returns the CREATE for SQLite sentence from a structure
   *
   * Usage:
   *   $userDdl = [
   *     'table' => 'user',
   *     "columns' => [
   *        [ 'name' => 'id', 'type' => 'INTEGER', 'key' => 'primary', 'autoincrement' => true, ],
   *        [ 'name' => 'name', 'type' => 'TEXT', ],
   *        [ 'name' => 'age', 'type' => 'INTEGER', 'null' => true, ],
   *     };
   *   ];
   *   $sql = $builder->arrayToDdl( $userDdl );
   *   // CREATE TABLE `user` ( 
   *   //   `id` INTEGER PRIMARY KEY AUTOINCREMENT,
   *   //   `name` TEXT,
   *   //   `age` INTEGER NULL
   *   // );
   *
   * Notes:
   *   - TODO: Check the structure...
   *
   */
  public function arrayToDdl(array $ddl) : string {
    $sql = "CREATE TABLE `{$ddl['table']}` ( ";
    $columns = [];
    foreach($ddl['columns'] as $col) {
      $tmp = " `{$col['name']}` {$col['type']} ";
      if(!empty($col['null'])) {
        $tmp .= " NULL ";
      }
      if(!empty($col['key'])) {
        switch($col['key']) {
          case 'primary':
          case 'PRIMARY':
          case 'Primary':
            $tmp .= ' PRIMARY KEY ';
            if(!empty($col['autoincrement'])) {
              $tmp .= ' AUTOINCREMENT';
            }
            break;
        }
      }
      $columns[] = $tmp;
    }
    $sql .= implode(' , ', $columns);
    $sql .= " ) ";
    return $sql;
  }




  /**
   *
   * Loads the DDL into the class
   *
   * Notes:
   *   - TODO: Allow passing RAW SQL, like from a MySQL dump
   *
   */
  public function loadTable(array $table) {
    $ddl = $this->arrayToDdl($table);
    $this->getHandler()->query($ddl);
    $pk = false;
    foreach($table['columns'] as $col) {
      if(!empty($col['key']) && $col['key'] == 'primary') {
        $pk = $col['name'];
        break;
      }
    }
    if($pk) {
      $this->setPrimaryKey( $table['table'], $pk);
    }
    return $this;
  }



  /** 
   *
   * Sets the database handler.
   * The following handlers are allowed:
   *   - PDO
   *   - SQLite3
   *   - mysqli
   *
   * If no handler is passed, it will fallback to a temporary SQLite3
   * connection to prepare statements.
   *
   * NOTE: DO NOT USE, UNDER REVISION
   * 
   */
  public function setHandler(object $var) : bool {
    $handlerSet = false;
    if(is_a($var, 'PDO')) {
      $this->handler = $var;
      $this->handlerType = 'pdo';
      $handlerSet = true;
    } else if(is_a($var, 'mysqli')) {
      $this->handler = $var;
      $this->handlerType = 'mysqli';
      $handlerSet = true;
    } else if(is_a($var, 'SQLite3')) {
      $this->handler = $var;
      $this->handlerType = 'sqlite3';
      $handlerSet = true;
    }
    return $handlerSet;
  }




  /**
   *
   * Gets a handler.
   *
   */
  public function getHandler() {
    if($this->handler) {
      return $this->handler;
    }
    return $this->getTempHandler();
  }




  /** 
   *
   * Sets an array of primary keys.
   *
   * Usage:
   *   $keys = [
   *     'user' => 'id',
   *     'company' => 'id',
   *   ];
   *   $builder->setPrimaryKeys( $keys );
   *
   */
  public function setPrimaryKeys(array $var) : bool {
    $keysSet = false;
    $firstKey = key($var);
    if(!is_numeric($firstKey)) {
      foreach($var as $k=>$v) {
        $this->setPrimaryKey($k, $v);
      }
      $keysSet = true;
    }
    return $keysSet;
  }




  /**
   *
   * Sets the primary key of a table.
   * 
   * Usage:
   *   $builder = new \choval\builder;
   *   $builder->setPrimaryKey('user','user_id');
   *   $sql = $builder->get('user','1');
   *   echo $sql;
   *   // SELECT * FROM user WHERE ( user_id = 1 )
   *
   */
  public function setPrimaryKey(string $table, string $pk) {
    if(static::checkIdentifier($table) && static::checkIdentifier($pk)) {
      $this->primaryKeys[$table] = $pk;
      return $this;
    }
    return false;
  }




  /**
   *
   * Gets the driver from the handlerType & handler.
   *
   * Usage:
   *   $driver = $builder->getDriver();
   *
   * Returns:
   *   - mysql
   *   - sqlite
   * 
   * TODO: Upcoming drivers: Cassandra / Sphinxsearch / SQLServer/ PostgreSQL
   *
   */
  public function getDriver() {
    if(!$this->driver) {
      if($this->handlerType) {
        switch($this->handlerType) {
          case 'mysqli':
            $this->driver = 'mysql';
            break;
          case 'sqlite3':
            $this->driver = 'sqlite';
            break;
          case 'pdo':
            $this->driver = strtolower( $this->handler->getAttribute(\PDO::ATTR_DRIVER_NAME ) );
            break;
          default:
            $this->driver = 'mysql';
            break;
        }
      } else {
        $this->driver = 'sqlite';
      }
    }
    return $this->driver;
  }




  /**
   *
   * Sets the driver
   *
   */
  public function setDriver(string $driver) {
    $this->driver = $driver;
    return $driver;
  }




  /**
   *
   * Retrieves the primary key of a table if any.
   *
   * Usage:
   *   $pk = $builder->getPrimaryKey('user');
   *
   */
  public function getPrimaryKey(string $table) {
    $table = static::checkIdentifier($table);
    if(!$table) {
      return false;
    }
    return $this->primaryKeys[$table] ?? false;
  }




  /**
   *
   * Gets the SQL required to find the primary key of a table.
   *
   * Usage:
   *   $sql = $builder->primaryKeyQuery('user');
   *   $result = $db->query($sql)->fetch(); 
   *   $builder->setPrimaryKeyQuery($result);
   *   $pk = $this->getPrimaryKey('user');
   *
   * TODO: TESTING REQUIRED
   *
   */
  public function primaryKeyQuery(string $table) : string {
    $sql = false;
    $driver = $this->getDriver();
    switch($driver) {
      case 'mysql':
        $sql = "SHOW KEYS FROM `$table` WHERE Key_name = 'PRIMARY'";
        break;
      case 'sqlite':
        $sql = "PRAGMA table_info('$table')";
        break;
      default:
        break;
    }
    return $sql;
  }




  /**
   *
   * Sets the primary key from a primaryKeyQuery.
   *
   * Usage: check primaryKeyQuery
   *
   * TODO: TESTING REQUIRED
   *
   */
  public function setPrimaryKeyQuery($data) {
    $table = $data['Table'];
    $pk = $data['Column_name'];
    return $this->setPrimaryKey($table, $pk);
  }




  /**
   *
   * Prepares an SQL, returning a statement.
   *
   * TODO: Add the name of the function to call depending on the handler,
   *       as it might not always be "prepare".
   *
   */
  public function prepare(string $sql) {
    $handler = $this->getHandler();
    return call_user_func([$handler, $this->prepareFn], $sql);
  }
  public function setPrepareFunction(string $fn) {
    $this->prepareFn = $fn;
    return $this;
  }




  /**
   *
   * Binds value to the statement, returning the bind result
   *
   * TODO: Add the name of the function to call depending on the handler,
   *       as it might not always be "bindValue".
   *
   */
  public function bind(&$statement, string $key, $value) {
    return call_user_func( [$statement, $this->bindFn], $key, $value ?? null );
  }
  public function setBindFunction(string $fn) {
    $this->bindFn = $fn;
    return $this;
  }



  /**
   *
   * Gets the SQL from the statement
   *
   */
  public function getSQL($statement) {
    $driver = $this->getDriver();
    switch($driver) {
      case 'sqlite':
      var_dump($statement);
        return call_user_func( [$statement, $this->getSqlFn], true );
        break;
    }
    return call_user_func( [$statement, $this->getSqlFn] );
  }




  /**
   *
   * Returns the SQL for retrieving a single row.
   *
   * Usage:
   *   $sql = $builder->get('user', 1);
   *   // SELECT * FROM `user` WHERE ( id = 1 )
   *
   */
  public function get(string $table, $id) : string {
    $pk = $this->getPrimaryKey( $table );
    if(!$pk) {
      throw new \Exception('PRIMARY KEY MISSING: '.$table);
    }
    $filter = [ $pk => $id ];
    return $this->findOne($table, $filter);
  }




  /**
   *
   * Returns the SQL for a Find query
   *
   * Usage:
   *   $filter = [
   *     'name' => ['LIKE'=>'%John%'],
   *     'active' => 1,
   *   ];
   *   $sql = $builder->find('user', $filter);
   *   // SELECT * FROM `user` WHERE ( name LIKE '%John%' AND active = 1 )
   *
   */
  public function find(string $table, array $filter=[]) : string {
    $sql = "SELECT * FROM `$table` ";
    $values = [];
    if(!empty($filter)) {
      $sql .= " WHERE ";
      $sql .= static::buildFilter($filter, $values);
    }
    if(empty($values)) {
      return $sql;
    }
    $st = $this->prepare( $sql );
    foreach($values as $k=>$v) {
      $this->bind( $st, $k, $v);
    }
    return $this->getSQL( $st );
  }




  /**
   *
   * Takes an SQL and adds a limit
   *
   * Usage:
   *   $sqlLimited = $builder->limit( $sqlUnlimited, 50, 50);
   *   // SELECT * FROM `user` LIMIT 50, 50
   *
   */
  public function limit(string $sql, int $int1, int $int2=null) : string {
    $limit = " LIMIT $int1";
    if($int2) {
      $limit .= ", $int2 ";
    }
    if(preg_match('/ LIMIT [0-9 ,]+ /s', $sql, $match)) {
      $sql = str_replace( $match[0], $limit, $sql );
    } else {
      $sql .= $limit;
    }
    return $sql;
  }




  /**
   *
   * Returns an SQL to find a single row.
   *
   * Usage:
   *   $filter = [
   *     'active' => 1,
   *   ];
   *   $sql = $builder->findOne('user', $filter);
   *   // SELECT * FROM `user` WHERE ( active = 1 ) LIMIT 1 
   *
   */
  public function findOne(string $table, array $filter=[]) : string {
    $sql = $this->find($table, $filter);
    return $this->limit( $sql, 1 );
  }




  /** 
   *
   * Returns an SQL to delete rows.
   * 
   * Usage:
   *   $sql = $builder->delete('user', 1);
   *   // DELETE FROM `user` WHERE ( `id` = 1 ) LIMIT 1
   *
   *   $filter = [ 'activo' => 0 ];
   *   $sql = $builder->delete('user', $filter);
   *   // DELETE FROM `user` WHERE ( `activo` = 0 )
   *
   */
  public function delete(string $table, $filter=null) : string {
    $sql = "DELETE FROM `$table` ";
    if(!empty($filter)) {
      $values = [];
      $sql .= " WHERE ";
      if(!is_array($filter)) {
        $pk = $this->getPrimaryKey($table);
        $filter = [ $pk => $filter ];
      }
      $sql .= static::buildFilter($filter, $values);
    }
    $st = $this->prepare( $sql );
    foreach($values as $k=>$v) {
      $this->bind( $st, $k, $v);
    }
    return $this->getSQL($st);
  }




  /**
   *
   * Generates an SQL for upsert or INSERT & UPDATE combo
   *
   * Usage:
   *   $data = [
   *     'id' => 1,
   *     'name' => 'John',
   *   ];
   *   $sql = $builder->save('user', $data);
   *   // mysql
   *   // INSERT 
   *
   *   // sqlite
   *   // INSERT OR IGNORE INTO `user` ( `id` , `name` ) VALUES ( 1 , 'John' ) ;
   *   // UPDATE `user` SET `name` = 'John' WHERE `id` = 1
   *
   */
  public function save(string $table, array $array) : string {
    $pk = $this->getPrimaryKey($table);
    $sqls = [];
    $sqls[] = $this->insert($table, $array);
    if(isset($array[$pk])) {
      $sqls[] = $this->update($table, $array);
    }
    return implode(' ; ', $sqls);
  }



  /**
   *
   * Generates an SQL for an insert
   *
   * Usage:
   *   $data = [
   *     'id' => 1,
   *     'name' => 'John',
   *   ];
   *   $sql = $builder->insert('user', $data, true);
   *   // INSERT IGNORE INTO `user` ( `id` , `name` ) VALUES ( 1 , 'John' )
   *
   *   $rows = [
   *     ['id'=>1, 'name'=>'John'],
   *     ['id'=>2, 'name'=>'Jane'],
   *   ];
   *   $sql = $builder->insert('user', $rows);
   *   // INSERT INTO `user` ( `id` , `name` ) VALUES ( 1 , 'John' ) , ( 2 , 'Jane' )
   * 
   */
  public function insert(string $table, array $array=[], bool $ignore=false) : string {
    $sql = "INSERT ";
    if($ignore) {
      $driver = $this->getDriver();
      if($driver == 'sqlite') {
        $sql .= "OR ";
      }
      $sql .= "IGNORE ";
    }
    $sql .= " INTO `$table` ";
    $keys = [];
    $values = [];
    $subkeys = [];
    $testKey = current($array);
    if(!is_array($testKey)) {
      $array = [$array];
    }
    foreach($array as $row) {
      foreach($row as $k=>$v) {
        $keys[] = $k;
      }
    }
    $keys = array_unique($keys);
    $batch = [];
    foreach($array as $pos=>$row) {
      $set = [];
      foreach($keys as $k) {
        if(isset($row[$k])) {
          $subk = ':'.$k.'_'.$pos;
          $values[ $subk ] = $row[$k];
          $set[] = $subk;
        } else {
          $set[] = 'NULL';
        }
      }
      $batch[] = ' ( '.implode(' , ', $set).' ) ';
    }
    $inKeys = [];
    foreach($keys as $k) {
      $inKeys[] = ' `'.$k.'` ';
    }
    $sql .= ' ( '.implode(',', $inKeys).' ) ';
    $sql .= ' VALUES '.implode(', ', $batch);
    $st = $this->prepare( $sql );
    foreach($values as $k=>$v) {
      $this->bind( $st, $k, $v);
    }
    return $this->getSQL($st);
  }




  /**
   *
   * Generates an SQL for update
   *
   * Usage:
   *   $data = [
   *     'id' => 1,
   *     'name' => 'John',
   *   ];
   *   $sql = $builder->update('user', $data);
   *   // UPDATE `user` SET `name` = 'John' WHERE ( `id` = 1 ) LIMIT 1
   *
   *   $set = [
   *     'active' => 1,
   *   ];
   *   $filter = [
   *     'active' => 0,
   *   ];
   *   $sql = $builder->update('user', $set, $filter);
   *   // UPDATE `user` SET `active` = 1 WHERE ( `active` = 0 )
   *
   */
  public function update(string $table, array $array=[], array $filter=[]) : string {
    $pk = $this->getPrimaryKey($table);
    $sql = "UPDATE `$table` SET ";
    $limit = false;
    if(isset($array[$pk])) {
      $filter = [
        $pk => $array[$pk],
      ];
      unset($array[$pk]);
      $limit = 1;
    }
    $values = [];
    $setSql = $this->buildFilter( $array, $values, ', ');
    $setSql = ' '.substr(trim($setSql,1,-1)).' ';
    $sql .= $setSql;
    if(!empty($filter)) {
      $sql .= " WHERE ";
      $sql .= $this->buildFilter( $filter, $values );
    }
    if($limit) {
      $this->limit($sql, $limit);
    }
    $st = $this->prepare( $sql );
    foreach($values as $k=>$v) {
      $this->bind( $st, $k, $v);
    }
    return $this->getSQL($st);
  }



  
  /**
   *
   * Generates a COUNT SQL.
   *
   * Usage:
   *   $builder->setPk( 'users', 'id' );
   *   $filter = [
   *     'user' => 1,
   *     'OR' => [
   *        'active' => 1,
   *        'blocked' => ['<=' => '2018-01-01'],
   *     ],
   *   ];
   *   $sql = $builder->count('users', $filter);
   *   // SELECT COUNT( `id` ) FROM `users` WHERE ( `user` = 1 AND ( `active` = 1 OR `blocked` <= '2018-01-01' ) ) 
   * 
   */
  public function count(string $table, array $filter=[]) : string {
    $pk = $this->getPrimaryKey($table);
    if($pk) {
      $pk = '`'.$pk.'`';
    } else {
      $pk = '*';
    }
    $sql = "SELECT COUNT( $pk ) FROM `$table` ";
    $values = [];
    if(!empty($filter)) {
      $sql .= " WHERE ";
      $sql .= static::buildFilter($filter, $values);
    }
    $st = $this->prepare( $sql );
    foreach($values as $k=>$v) {
      $this->bind( $st, $k, $v);
    }
    return $this->getSQL( $st );
  }




  /**
   *
   * Returns the WHERE portion of an SQL from an array.
   * The second value is used to store values to be passed to the statement.
   * Usage:
   *   $filter = [
   *     'user' => 1,
   *     'company' => 3,
   *   ];
   *   $where = static::buildFilter($filter, $values);
   *
   *   // $where holds
   *   // ( user = :_user_1 AND company :_company_1 )
   *   // $values holds
   *   // [ ':_user_1' => 1, ':_company_1' => 3 ];
   * 
   *   // DO NOT DO THIS:
   *   $sql = "SELECT * FROM users WHERE ".$where;
   *   // Because $sql holds:
   *   // SELECT * FROM `users` WHERE ( `user` = :_user_1 AND `company` :_company_1 )
   *   // It needs to be prepared as a statement and have values binded.
   *   // This is also the reason this function is protected and not public.
   *   
   */
  public static function buildFilter(array $filter, array &$values=[], string $connector='AND', int $depth=1) : string {
    $raws = [];
    foreach($filter as $k=>$v) {
      if($k=='OR' || $k=='AND') {
        $raws[] = static::buildFilter($v, $values, $k, ($depth+1));
      } else {
        if(is_array($v)) {
          if( $k == 'IN' || (is_array($v) && key($v)===0) ) {
            $raw = " `$k` IN (";
            $vars = [];
            foreach($v as $pos=>$subv) {
              $subk = ':'.$k.'_'.$depth.'_'.$pos;
              $values[$subk] = $subv;
              $vars[] = $subk;
            }
            $raw .= implode(', ', $vars);
            $raw .= ') ';
            $raws[] = $raw;
          } else if(count($v) == 1) {
            $oper = key($v);
            $subk = ':'.$k.'_'.$depth;
            switch($oper) {
              case '!=':
              case '<>':
              case '>':
              case '<':
              case '>=':
              case '<=':
                $raws[] = " `$k` $oper $subk ";
                $values[$subk] = current($v);
                break;
              case 'IS':
                $subv = $v['IS'];
                if($subv == 'NULL' || $subv == 'NOT NULL') {
                  $raws[] = " `$k` $oper $subv ";
                }
                break;
              case 'LIKE':
                $subv = $v['LIKE'];
                $raws[] = " `$k` $oper $subv ";
                break;
              default:
                $raws[] = static::buildFilter($v, $values);
                break;
            }
          } else {
            $raws[] = static::buildFilter($v, $values);
          }
        } else {
          $subk = ':'.$k.'_'.$depth;
          $raws[] = " `$k` = $subk ";
          $values[$subk] = $v;
        }
      }
    }
    return ' ( '.implode(' '.$connector.' ', $raws).' ) ';
  }



  
  /**
   *
   * Takes an SQL and replaces the params for the values order
   *
   */
  static function replaceFilterParams(string $sql, array &$values) : string {
    $finalValues = [];
    foreach($values as $k=>$v) {
      if(strpos($k, $sql)) {
        $sql = str_replace($k, '?', $sql);
        $finalValues[] = $v;
      }
    }
    $values = $finalValues;
    return $sql;
  }



  
}

