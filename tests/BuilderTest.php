<?php
declare(strict_types=1);

use PHPUnit\Framework\TestCase;

use choval\builder;
use React\EventLoop\Factory;

final class BuilderTest extends TestCase {


  private static $dbFile;
  private static $db;

  private static $builder;
  

  /**
   * Constructor
   */
  public static function setUpBeforeClass() {
    static::$dbFile = tempnam( sys_get_temp_dir(), 'test' );
    static::$db = new \SQLite3( static::$dbFile );
    static::$db->query("
      CREATE TABLE test (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        age INTEGER
      );
    ");


    $format = [
      [
        'table'   => 'test',
        'columns' => [
          [ 'name' => 'id', 'type' => 'INTEGER', 'key' => 'primary' , 'autoincrement' => true ],
          [ 'name' => 'name', 'type' => 'TEXT' ],
          [ 'name' => 'age', 'type' => 'INTEGER' ],
        ],
      ],
    ];
    static::$builder = new builder;
    foreach($format as $table) {
      static::$builder->loadTable($table);
    }
  }


  /**
   * Destructor
   */
  public static function tearDownAfterClass() {
    static::$db->close();
    unlink( static::$dbFile );
  }



  public function rowsProvider() {
    $rows = [];
    $rows[] = [
      [
      'id' => 1,
      'name' => 'John',
      'age' => 25,
      ],
    ];
    $rows[] = [
      [
      'id' => 2,
      'name' => 'Jane',
      'age' => 25,
      ],
    ];
    $rows[] = [
      [
      'id' => 3,
      'name' => 'Oscar',
      'age' => 50,
      ],
    ];
    return $rows;
  }


  /**
   * @dataProvider rowsProvider
   */
  public function testInsert($row) : void {
    var_dump( static::$builder->insert('test', $row));
    $this->assertNotFalse( static::$db->querySingle( static::$builder->insert('test', $row) ) );
  }




}
