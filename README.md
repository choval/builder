
# QueryBuilder

Generates SQL queries for MySQL and SQLite.

This library helps creating safe SQL queries to use in different plugins.

* [PHP/MySQLi](http://php.net/manual/en/mysqli.query.php)
* [friends-of-reactphp/mysql](https://github.com/friends-of-reactphp/mysql)

## Requirements

* PHP 7.4+
* libqlite 3.14+


## Installation

```sh
composer require choval/builder
```

## Usage

```php
use choval\builder;

$builder = new builder;
$db = new SQLite3('my.db');
$db->query('CREATE TABLE user ( id INT PRIMARY KEY AUTO_INCREMENT, name TEXT )');

$userId = 1;
$user = $db->querySingle( $builder->get('user', $userId) );

$user['name'] = 'John';
$db->querySingle( $builder->save('user', $user) );
```


## TODO

* `loadStructure` method to load the database structure from a JSON.
* `fields` method for selecting the fields to retrieve. Allow functions as well.
* `orderBy` method for adding an order/sort.
* `groupBy` method to add grouping.
* `having` method to filter after grouping.


