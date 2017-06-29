# Cassandra library for Phalcon


![status](https://travis-ci.org/SachaMorard/phalcon-cassandra.svg?branch=master)


## Requirements

* PHP >= 5.3
* Cassandra php driver https://github.com/datastax/php-driver
* Phalcon >= 3.0 https://github.com/phalcon/cphalcon/

Cassandra php driver (https://github.com/datastax/php-driver) 
hasn't appropriate version number. The only version number 
available is for C/C++ driver version (wich is not the version 
of the extension itself).
Because of it, version of this library follow the cassandra 
php-driver real version.

If you need to use cassandra php-driver v1.2.2, please install 
our library at 1.2.2
Follow the exact same logic for other available versions.

## Installing via Composer

Install composer in a common location or in your project:

```bash
curl -s http://getcomposer.org/installer | php
```

Create the composer.json file as follows:

```json
{
    "require-dev": {
        "sachoo/phalcon-cassandra": "1.3.0"
    }
}
```

Run the composer installer:

```bash
php composer.phar install
```


## How to use

First of all you have to add and define some conf in your phalcon config file
```php
return new \Phalcon\Config([
    'cassandra' => [
        'adapter' => 'Cassandra',
        'hosts' => [ 'localhost'],
        'keyspace' => 'lobs',
        'consistency' => 'ONE', // Infos here \cassandra\ConsistencyLevel
        'retryPolicies' => 'DefaultPolicy',
    ]
]);
```

Then, you have to declare Cassandra service in your dependency injector : 

```php
/**
 * This service returns a Cassandra database
 */
$di->setShared('dbCassandra', function () {
    $config = $this->getConfig();
    $connection = new \Phalcon\Db\Adapter\Cassandra($config->cassandra->toArray());
    return $connection;
});
```

IMPORTANT, if you are using mysql too, you have to change the name of mysql service in **dbMysql**. For exemple:
```php
$di->setShared('dbMysql', function () {
    $config = $this->getConfig();
    $params = [
        'host' => $config->mysql->host,
        'port' => isset($config->mysql->port) ? $config->mysql->port : 3306,
        'username' => $config->mysql->username,
        'password' => $config->mysql->password,
        'dbname' => $config->mysql->dbname,
        'charset' => $config->mysql->charset
    ];
    return new \Phalcon\Db\Adapter\Pdo\Mysql($params);
});
```

Then, you just have to declare the correct database service to your models in @Source annotation. For exemple:
```php
<?php
namespace Models\Cassandra;

/**
 * Class Article
 * @package Models\Cassandra
 *
 * @Source('dbCassandra', 'article')
 */
class Article extends \Phalcon\Mvc\CassandraModel
{
    /**
     * @Primary
     * @Column(type="uuid", nullable=false)
     */
    public $uuid;

    /**
     * @Column(type="varchar", nullable=false)
     */
    public $title;

    /**
     * @Column(type="varchar", nullable=false)
     */
    public $description;

    /**
     * list<uuid>
     * uuid of linked articles
     * @Column(type="list", nullable=true)
     */
    public $links;

    /**
     * @Column(type="float", nullable=true)
     */
    public $lat;

    /**
     * @Column(type="float", nullable=true)
     */
    public $lon;

    /**
     * @Column(type="int", nullable=true)
     */
    public $radius;

    /**
     * @Column(type="varchar", nullable=false)
     */
    public $status;

    /**
     * @Column(type="varchar", nullable=false)
     */
    public $type;

    /**
     * @Column(type="varchar", nullable=false)
     */
    public $language;
}
```
