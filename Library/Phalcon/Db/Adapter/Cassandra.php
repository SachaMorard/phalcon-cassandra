<?php
/*
 * This file is part of the PhalconCassandra package.
 *
 * (c) Sacha Morard <sachamorard@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Phalcon\Db\Adapter;

use Cassandra\ExecutionOptions;
use Cassandra\BatchStatement;
use Cassandra\Keyspace;
use Cassandra\Table;
use Phalcon\Events\Manager;
use Phalcon\Exception;
use Phalcon\Db\Result\Cassandra as ResultCassandra;
use Phalcon\Cassandra\DataType;

/**
 * Phalcon\Db\Adapter\Cassandra
 *
 * Phalcon\Db\Adapter\Cassandra is the Phalcon\Db that internally uses Cassandra + Thrift to connect to a database
 *
 *<code>
 *    $connection = new \Phalcon\Db\Adapter\Cassandra(array(
 *        'host' => '192.168.0.11',
 *        'username' => 'sigma',
 *        'password' => 'secret',
 *        'keyspace' => 'blog',
 *        'port' => '3306'
 *    ));
 *</code>
 */
class Cassandra extends \Phalcon\Db\Adapter implements \Phalcon\Events\EventsAwareInterface, \Phalcon\Db\AdapterInterface
{
    const DEFAULT_PORT = 9042;

    /**
     * @var Manager
     */
    protected $_eventsManager;
    /**
     * @var
     */
    protected $_cqlStatement;
    /**
     * @var
     */
    protected $_cqlVariables;
    /**
     * @var
     */
    protected $_cqlBindTypes;
    /**
     * Last affected rows
     */
    protected $_affectedRows;
    /**
     * @var bool
     */
    protected $_isUnderTransaction = false;
    /**
     * @var string
     */
    protected $_dialectType;
    /**
     * @var string
     */
    protected $_type;
    /**
     * @var array
     */
    protected $_descriptor;
    /**
     * @var int
     */
    protected $_connectionId;
    /**
     * @var array
     */
    protected $_hosts;
    /**
     * @var string
     */
    protected $_keyspace;
    /**
     * @var int
     */
    protected $_port;
    /**
     * @var int
     */
    protected $_consistency;
    /**
     * @var float
     */
    protected $_connectTimeout;
    /**
     * @var float
     */
    protected $_requestTimeout;
    /**
     * @var float
     */
    protected $_defaultTimeout;
    /**
     * @var string
     */
    protected $_username;
    /**
     * @var string
     */
    protected $_password;
    /**
     * @var array
     */
    protected $_DARRLBPolicy;
    /**
     * @var int
     */
    protected $_pageSize;
    /**
     * @var boolean
     */
    protected $_persistentSession;
    /**
     * @var string
     */
    protected $_retryPolicies;
    /**
     * @var boolean
     */
    protected $_roundRobinLoadBalancingPolicy;
    /**
     * @var boolean
     */
    protected $_tokenAwareRouting;
    protected $_latencyAwareRouting;
    protected $_protocolVersion;
    protected $_IOThreads;
    protected $_connectionsPerHost;
    protected $_TCPNodelay;
    protected $_TCPKeepalive;
    /**
     * @var \Cassandra\Session
     */
    protected $_session;
    /**
     * @var \Cassandra\ExecutionOptions
     */
    protected $_options;
    /**
     * @var \Cassandra\BatchStatement
     */
    protected $_batch;
    /**
     * @var \Cassandra\Cluster
     */
    protected $_cluster;
    /**
     * @var \Phalcon\Db\Dialect\Cassandra
     */
    protected $_dialect;
    /**
     * @var bool
     */
    protected $_async = false;
    /**
     * @var bool
     */
    protected $_ttl = false;

    /**
     * Constructor for \Phalcon\Db\Adapter\Cassandra
     *
     * @param array $descriptor
     */
    public function __construct(array $descriptor)
    {
        $this->getDialect();
        $this->_buildCluster($descriptor);
        $this->connect();
    }

    /**
     * @param array $descriptor
     */
    protected function _buildCluster(array $descriptor)
    {
        $this->_dialectType = 'cassandra';
        $this->_type = 'cassandra';
        $this->_descriptor = $descriptor;
        $this->_connectionId = 0;
        $this->_hosts = $descriptor['hosts'];
        $this->_keyspace = isset($descriptor['keyspace']) ? $descriptor['keyspace'] : null;
        $this->_port = isset($descriptor['port']) ? $descriptor['port'] : self::DEFAULT_PORT;
        $this->_consistency = isset($descriptor['consistency']) ? 'CONSISTENCY_' . $descriptor['consistency'] : null;
        $this->_connectTimeout = isset($descriptor['connectTimeout']) ? $descriptor['connectTimeout'] : null;
        $this->_requestTimeout = isset($descriptor['requestTimeout']) ? $descriptor['requestTimeout'] : null;
        $this->_defaultTimeout = isset($descriptor['defaultTimeout']) ? $descriptor['defaultTimeout'] : null;
        $this->_username = isset($descriptor['username']) ? $descriptor['username'] : null;
        $this->_password = isset($descriptor['password']) ? $descriptor['password'] : null;
        $this->_DARRLBPolicy = isset($descriptor['datacenterAwareRoundRobinLoadBalancingPolicy']) ? $descriptor['datacenterAwareRoundRobinLoadBalancingPolicy'] : null;
        $this->_pageSize = isset($descriptor['pageSize']) ? $descriptor['pageSize'] : null;
        $this->_persistentSession = isset($descriptor['persistentSession']) ? $descriptor['persistentSession'] : null;
        $this->_roundRobinLoadBalancingPolicy = isset($descriptor['roundRobinLoadBalancingPolicy']) ? $descriptor['roundRobinLoadBalancingPolicy'] : null;
        $this->_tokenAwareRouting = isset($descriptor['tokenAwareRouting']) ? $descriptor['tokenAwareRouting'] : null;

        $this->_latencyAwareRouting = isset($descriptor['latencyAwareRouting']) ? $descriptor['latencyAwareRouting'] : null;
        $this->_protocolVersion = isset($descriptor['protocolVersion']) ? $descriptor['protocolVersion'] : null;
        $this->_IOThreads = isset($descriptor['IOThreads']) ? $descriptor['IOThreads'] : null;
        $this->_connectionsPerHost = isset($descriptor['connectionsPerHost']) ? $descriptor['connectionsPerHost'] : null;
        $this->_TCPNodelay = isset($descriptor['TCPNodelay']) ? $descriptor['TCPNodelay'] : null;
        $this->_TCPKeepalive = isset($descriptor['TCPKeepalive']) ? $descriptor['TCPKeepalive'] : null;
        $this->_retryPolicies = isset($descriptor['retryPolicies']) ? $descriptor['retryPolicies'] : null;

        $builder = \Cassandra::cluster();

        call_user_func_array([$builder, 'withContactPoints'], $this->_hosts);

        $builder->withPort($this->_port);

        if ($this->_consistency) {
            $consistency = constant('\Cassandra::'.$this->_consistency);
            $builder->withDefaultConsistency($consistency);
        }

        if ($this->_latencyAwareRouting !== null) {
            $builder->withLatencyAwareRouting($this->_latencyAwareRouting);
        }
        if ($this->_protocolVersion !== null) {
            $builder->withProtocolVersion($this->_protocolVersion);
        }
        if ($this->_IOThreads !== null) {
            $builder->withIOThreads($this->_IOThreads);
        }
        if ($this->_connectionsPerHost !== null) {
            $builder->withConnectionsPerHost($this->_connectionsPerHost['core'], $this->_connectionsPerHost['max']);
        }
        if ($this->_TCPNodelay !== null) {
            $builder->withTCPNodelay($this->_TCPNodelay);
        }
        if ($this->_TCPKeepalive !== null) {
            $builder->withTCPKeepalive($this->_TCPKeepalive);
        }
        if ($this->_connectTimeout !== null) {
            $builder->withConnectTimeout($this->_connectTimeout);
        }
        if ($this->_requestTimeout !== null) {
            $builder->withRequestTimeout($this->_requestTimeout);
        }
        if ($this->_defaultTimeout !== null) {
            $builder->withDefaultTimeout($this->_defaultTimeout);
        }
        if ($this->_username !== null) {
            $builder->withCredentials($this->_username, $this->_password);
        }
        if ($this->_DARRLBPolicy !== null) {
            $builder->withDatacenterAwareRoundRobinLoadBalancingPolicy($this->_DARRLBPolicy['localDatacenter'], $this->_DARRLBPolicy['hostPerRemoteDatacenter'], $this->_DARRLBPolicy['useRemoteDatacenterForLocalConsistencies']);
        }
        if ($this->_pageSize !== null) {
            $builder->withDefaultPageSize($this->_pageSize);
        }
        if ($this->_persistentSession !== null) {
            $builder->withPersistentSessions($this->_persistentSession);
        }
        if ($this->_roundRobinLoadBalancingPolicy !== null && $this->_roundRobinLoadBalancingPolicy === true) {
            $builder->withRoundRobinLoadBalancingPolicy();
        }
        if ($this->_tokenAwareRouting !== null) {
            $builder->withTokenAwareRouting($this->_tokenAwareRouting);
        }
        if ($this->_retryPolicies !== null) {
            $retryPolicies = null;
            if ($this->_retryPolicies === 'DowngradingConsistency') {
                $retryPolicies = new \Cassandra\RetryPolicy\DowngradingConsistency();
            } elseif ($this->_retryPolicies === 'DefaultPolicy') {
                $retryPolicies = new \Cassandra\RetryPolicy\DefaultPolicy();
            } elseif ($this->_retryPolicies === 'Fallthrough') {
                $retryPolicies = new \Cassandra\RetryPolicy\Fallthrough();
            } elseif ($this->_retryPolicies === 'Logging') {
                $retryPolicies = new \Cassandra\RetryPolicy\Logging();
            }

            if ($retryPolicies !== null) {
                $builder->withRetryPolicy($retryPolicies);
            }
        }

        $this->_cluster = $builder->build();
        return;
    }

    /**
     * This method is automatically called in Phalcon\Db\Adapter\Cassandra constructor.
     * Call it when you need to restore a database connection
     *
     *<code>
     * //Make a connection
     * $connection = new \Phalcon\Db\Adapter\Cassandra(array(
     *        'host' => '192.168.0.11',
     *        'username' => 'sigma',
     *        'password' => 'secret',
     *        'keyspace' => 'blog',
     *        'port' => '3306'
     *    ));
     *
     * //Reconnect
     * $connection->connect();
     * </code>
     * @param null $descriptor
     * @return \Cassandra\Session
     */
    public function connect(array $descriptor = null)
    {
        if ($descriptor) {
            $this->_buildCluster($descriptor);
        }
        $this->_session = $this->_cluster->connect($this->_keyspace);

        return $this->_session;
    }

    /**
     * Closes the active connection returning success. \Phalcon automatically closes and destroys
     * active connections when the request ends
     *
     * @return boolean
     */
    public function close()
    {
        $this->_session->close();
    }

    /**
     * Automatic close
     */
    public function __destruct()
    {
        $this->_session->close();
        $this->close();
    }

    /**
     * @return $this
     */
    public function nextWillBeAsync()
    {
        $this->_async = true;
        return $this;
    }

    /**
     * @param $ttl
     * @return $this
     */
    public function ttl($ttl)
    {
        $this->_ttl = (int)$ttl;
        return $this;
    }

    /**
     * Sends CQL3 statements to the database server returning the success state.
     * Use this method only when the CQL statement sent to the server is returning rows
     *
     *<code>
     *    //Querying data
     *    $resultset = $connection->query("SELECT * FROM robots WHERE type='mechanical'");
     *    $resultset = $connection->query("SELECT * FROM robots WHERE type=?", array("mechanical"));
     *</code>
     *
     * @param string $cqlStatement
     * @param null $bindParams
     * @param null $bindTypes
     * @return ResultCassandra
     * @throws Exception
     */
    public function query($cqlStatement, $bindParams = null, $bindTypes = null)
    {
        $this->_cqlStatement = $cqlStatement;
        if ($this->_ttl) {
            $this->_cqlStatement .= ' USING TTL ' . $this->_ttl;
            $this->_ttl = false;
        }
        $this->_cqlBindTypes = $bindTypes;
        $this->_cqlVariables = $bindParams;

        if ($bindParams === null) {
            /**
             * Execute the beforeQuery event if a EventsManager is available
             */
            if ($this->_eventsManager !== null) {
                /** @var  \Phalcon\Events\Manager $eventsManager */
                $eventsManager = $this->_eventsManager;
                if ($eventsManager->fire("db:beforeQuery", $this, $bindParams) === false) {
                    return false;
                }
            }
        }

        if (is_array($bindParams)) {
            if ($this->isUnderTransaction()) {
                $this->addTransaction($cqlStatement, $bindParams);
                return true;
            } else {
                $statement = $this->prepare($cqlStatement);
                $result = $this->executePrepared($statement, $bindParams);
                return $result;
            }

        } else {
            if ($this->isUnderTransaction()) {
                $this->addTransaction($cqlStatement);
                return true;
            } else {
                $newStatement = new \Cassandra\SimpleStatement($cqlStatement);
                if ($this->_async) {
                    $this->_async = false;
                    $result = $this->_session->executeAsync($newStatement);
                } else {
                    $result = $this->_session->execute($newStatement);
                }

            }
        }

        /**
         * Execute the afterQuery event if a EventsManager is available
         */
        if ($bindParams === null) {
            if ($this->_eventsManager !== null) {
                /** @var  \Phalcon\Events\Manager $eventsManager */
                $this->_eventsManager->fire("db:afterQuery", $this, $bindParams);
            }
        }
        if (stripos($cqlStatement, 'SELECT') === 0) {
            return new ResultCassandra($this, $result, $cqlStatement, $bindParams);
        }
        return true;

    }

    /**
     * Sends CQL statements to the database server returning the success state.
     * Use this method only when the CQL statement sent to the server doesn't return any row
     *
     *<code>
     *    //Inserting data
     *    $success = $connection->execute("INSERT INTO robots VALUES (1, 'Astro Boy')");
     *    $success = $connection->execute("INSERT INTO robots VALUES (?, ?)", array(1, 'Astro Boy'));
     *</code>
     *
     * @param  string cqlStatement
     * @param  array bindParams
     * @param  array bindTypes
     * @return boolean
     */
    public function execute($cqlStatement, $bindParams = null, $bindTypes = null)
    {
        /**
         * Initialize affectedRows to -1 because Cassandra can't return affected rows
         */
        $this->_affectedRows = -1;
        return $this->query($cqlStatement, $bindParams, $bindTypes);
    }

    /**
     * Returns a Cassandra prepared statement to be executed with 'executePrepared'
     *
     *<code>
     * $statement = $db->prepare('SELECT * FROM robots WHERE name = :name');
     * $result = $connection->executePrepared($statement, array('name' => 'Voltron'));
     *</code>
     *
     * @param string CqlStatement
     * @return \Cassandra\PreparedStatement
     */
    public function prepare($cqlStatement)
    {
        $this->_cqlStatement = $cqlStatement;
        if ($this->_ttl) {
            $this->_cqlStatement .= ' USING TTL ' . $this->_ttl;
            $this->_ttl = false;
        }
        return $this->_session->prepare($cqlStatement);
    }

    /**
     * @param \Cassandra\PreparedStatement $preparedStatement
     * @param array $placeholders
     * @return array|bool|ResultCassandra
     * @throws \Exception
     */
    public function executePrepared(\Cassandra\PreparedStatement $preparedStatement, array $placeholders)
    {
        if ($this->isUnderTransaction()) {
            return $this->addTransaction($this->_cqlStatement, $placeholders);
        }

        $this->_cqlVariables = $placeholders;
        /**
         * Execute the beforeQuery event if a EventsManager is available
         */
        if ($this->_eventsManager !== null) {
            /** @var  \Phalcon\Events\Manager $eventsManager */
            $eventsManager = $this->_eventsManager;
            if ($eventsManager->fire("db:beforeQuery", $this, $placeholders) === false) {
                return false;
            }
        }
        $options = new ExecutionOptions(['arguments' => $placeholders]);
        if ($this->_async) {
            $this->_async = false;
            $result = $this->_session->executeAsync($preparedStatement, $options);
        } else {
            $result = $this->_session->execute($preparedStatement, $options);
        }
        /**
         * Execute the afterQuery event if a EventsManager is available
         */
        if ($this->_eventsManager !== null) {
            /** @var  \Phalcon\Events\Manager $eventsManager */
            $this->_eventsManager->fire("db:afterQuery", $this, $placeholders);
        }

        if (stripos($this->_cqlStatement, 'SELECT') === 0) {
            return new ResultCassandra($this, $result, $this->_cqlStatement, $placeholders);
        }
        return true;
    }

    /**
     * @param array|mixed|string $table
     * @param array $values
     * @param null $fields
     * @param null $dataTypes
     * @return bool
     * @throws Exception
     */
    public function insert($table, array $values, $fields = null, $dataTypes = null)
    {
        if (empty($values)) {
            throw new Exception("Unable to insert into " . $table . " without data");
        }

        foreach ($fields as $position => $dbColumnName) {
            $values[$position] = DataType::pack($dataTypes[$position], $values[$position]);
        }

        $placeholders = [];
        $insertValues = [];
        $bindDataTypes = [];
        foreach ($values as $position => $value) {
            $placeholders[] = "?";
            $insertValues[] = $value;
            if (is_array($dataTypes)) {
                if (!isset($dataTypes[$position])) {
                    throw new Exception("Incomplete number of bind types");
                }
                $bindDataTypes[] = $dataTypes[$position];
            }

        }

        $joinedValues = implode(', ', $placeholders);
        if (is_array($fields)) {
            $insertSql = "INSERT INTO " . $table . " (" . implode(", ", $fields) . ") VALUES (" . $joinedValues . ")";
        } else {
            $insertSql = "INSERT INTO " . $table . " VALUES (" . $joinedValues . ")";
        }

        if ($this->_ttl) {
            $insertSql .= ' USING TTL ' . $this->_ttl;
            $this->_ttl = false;
        }
        return $this->execute($insertSql, $insertValues);
    }

    /**
     * Returns an array of the records in the result
     * This method is affected by the active fetch flag set using \Phalcon\Db\Result\Pdo::setFetchMode
     *
     *<code>
     *    $result = $connection->fetchOne("SELECT * FROM robots ORDER BY name");
     *</code>
     *
     * @return array
     */
    public function fetchOne($cqlQuery, $fetchMode = \PDO::FETCH_BOTH, $bindParams = null, $bindTypes = null)
    {
        $result = $this->query($cqlQuery, $bindParams, $bindTypes);
        if ($result instanceof \Phalcon\Db\Result\Cassandra) {
            $result->setFetchMode($fetchMode);
            return $result->fetch();
        }

        return $result;
    }

    /**
     * Escapes a column/table/schema name
     *
     *<code>
     *    $escapedTable = $connection->escapeIdentifier('robots');
     *    $escapedTable = $connection->escapeIdentifier(array('store', 'robots'));
     *</code>
     *
     * @param string identifier
     * @return string
     */
    public function escapeIdentifier($identifier)
    {
        if (is_array($identifier)) {
            return '"' . $identifier[0] . '"."' . $identifier[1] . '"';
        }
        return '"' . $identifier . '"';
    }

    /**
     * Escapes a value to avoid SQL injections according to the active charset in the connection
     *
     *<code>
     *    $escapedStr = $connection->escapeString('some dangerous value');
     *</code>
     *
     * @param string str
     * @return string
     */
    public function escapeString($str)
    {
        $str = str_replace("\\'", "'", $str);
        return "'" . str_replace("'", "''", $str) . "'";

    }

    /**
     * Converts bound parameters such as :name: or ?1 into PDO bind params ?
     * @param $cql
     * @param null $params
     * @return array
     * @throws Exception
     */
    public function convertBoundParams($cql, $params = null)
    {
        $placeHolders = [];
        $bindPattern = "/\\?([0-9]+)|:([a-zA-Z0-9_]+):/";
        $matches = null;
        $setOrder = 2;

        if (preg_match_all($bindPattern, $cql, $matches, $setOrder)) {
            foreach ($matches as $placeMatch) {
                $value = isset($params[$placeMatch[1]]) ? $params[$placeMatch[1]] : false;
                if (!$value) {
                    if (isset($placeMatch[2])) {
                        $value = isset($params[$placeMatch[2]]) ? $params[$placeMatch[2]] : false;
                        if (!$value) {
                            throw new Exception("Matched parameter wasn't found in parameters list");
                        }
                    } else {
                        throw new Exception("Matched parameter wasn't found in parameters list");
                    }
                }
                $placeHolders[] = $value;
            }

            $boundCql = preg_replace($bindPattern, "?", $cql);
        } else {
            $boundCql = $cql;
        }

        return [
            "cql" => $boundCql,
            "params" => $placeHolders
        ];
    }

    /**
     * Starts a transaction in the connection
     *
     * @param bool|int $type
     * @return bool
     */
    public function begin($type = \Cassandra::BATCH_UNLOGGED)
    {
        $this->_isUnderTransaction = true;
        $this->_batch = new BatchStatement($type);
        return true;
    }

    /**
     * Commits the active transaction in the connection
     *
     * @param boolean nesting
     * @return boolean
     */
    public function commit($nesting = null, $counterBatch = false)
    {
        if ($this->_batch !== null) {
            $this->_isUnderTransaction = false;
            if ($this->_async) {
                $this->_async = false;
                return $this->_session->executeAsync($this->_batch);
            }
            return $this->_session->execute($this->_batch);
        }
        return;
    }

    /**
     * @param $cql
     * @param null $bindParams
     * @return array
     * @throws Exception
     */
    public function addTransaction($cql, $bindParams = null)
    {
        if ($this->_batch === null) {
            $this->begin();
        }

        $stm = $this->prepare($cql);
        $this->_batch->add($stm, $bindParams);
        return true;
    }

    /**
     * @param string $tableName
     * @param null $schemaName
     * @return array|bool
     */
    public function describeColumns($tableName, $schemaName = null)
    {
        $session = $this->getClient();
        $schema = $session->schema();
        if ($schemaName === null) {
            $schemaName = $this->_keyspace;
        }
        $keyspace = $schema->keyspace($schemaName);
        if ($keyspace === false) {
            return false;
        }
        $table = $keyspace->table($tableName);
        $keys = $table->primaryKey();

        if ($table === false) {
            return false;
        }
        $primaryKey = [];
        foreach ($keys as $column) {
            $primaryKey[$column->name()] = true;
        }

        $columns = array();
        $dataType = new DataType();
        foreach ($table->columns() as $name => $column) {
            $cassandraType = $column->type()->name();
            $type = $dataType->getPhalconTypeFor($cassandraType);
            $bind = $dataType->getPhalconBindFor($cassandraType);
            $isnumeric = $type === \Phalcon\Db\Column::TYPE_INTEGER || $type === \Phalcon\Db\Column::TYPE_FLOAT || $type === \Phalcon\Db\Column::TYPE_DECIMAL ? true : false;
            $columns[] = new \Phalcon\Db\Column($name, array(
                'type' => $type,
                'bindType' => $bind,
                'isNumeric' => $isnumeric,
                'primary' => isset($primaryKey[$name]),
            ));
        }

        if (empty($columns)) {
            return false;
        }
        return $columns;
    }

    /**
     * Returns the current transaction nesting level
     *
     * @return int
     */
    public function getTransactionLevel()
    {
        if ($this->isUnderTransaction()) {
            return 1;
        }
        return 0;
    }

    /**
     * Checks whether the connection is under a transaction
     *
     *<code>
     *    $connection->begin();
     *    var_dump($connection->isUnderTransaction()); //true
     *</code>
     *
     * @return boolean
     */
    public function isUnderTransaction()
    {
        return $this->_isUnderTransaction;
    }

    /**
     * @return \Cassandra\Session
     */
    public function getInternalHandler()
    {
        return $this->_session;
    }

    /**
     * @param string $tableName
     * @param null $schemaName
     * @return bool
     * @throws Exception
     */
    public function tableExists($tableName, $schemaName = null)
    {
        $session = $this->getClient();
        $schema = $session->schema();
        if ($schemaName === null) {
            $schemaName = $this->_keyspace;
        }


        $keyspace = $schema->keyspace($schemaName);
        if ($keyspace === false) {
            return false;
        }
        $tables = $keyspace->tables();

        if (!isset($tables[$tableName])) {
            return false;
        }
        return true;
    }

    /**
     * @param null $schemaName
     * @return array
     * @throws Exception
     */
    public function listTables($schemaName = null)
    {
        $session = $this->getClient();
        $schema = $session->schema();
        if ($schemaName === null) {
            $schemaName = $this->_keyspace;
        }

        $keyspace = $schema->keyspace($schemaName);
        if ($keyspace === false) {
            throw new Exception("Unknown database '$schemaName'");
        }
        $result = $keyspace->tables();

        $tables = [];
        foreach ($result as $tableName => $columnFamily) {
            $tables[] = $tableName;
        }
        return $tables;
    }

    /**
     * @param $tableName
     * @param null $schemaName
     * @return array
     * @throws Exception
     */
    public function describeIndexes($tableName, $schemaName = null)
    {
        $session = $this->getClient();
        $schema = $session->schema();
        if ($schemaName === null) {
            $schemaName = $this->_keyspace;
        }
        /** @var Keyspace $keyspace */
        $keyspace = $schema->keyspace($schemaName);
        if ($keyspace === false) {
            return false;
        }
        /** @var Table $table */
        $table = $keyspace->table($tableName);
        $keys = $table->primaryKey();

        $primary = [];
        foreach ($keys as $column) {
            $primary[] = $column->name();
        }

        $indexes = [];
        $indexes['PRIMARY'] = new \Phalcon\Db\Index('PRIMARY', $primary);


        foreach ($table->indexes() as $index) {
            if ($index->name() !== null) {
                $indexes[$index->name()] = new \Phalcon\Db\Index($index->name(), array($index->target()));
            }
        }

        return $indexes;
    }

    /**
     * @param mixed|string $tableName
     * @param null $schemaName
     * @param mixed $definition
     * @return bool
     */
    public function createTable($tableName, $schemaName = null, array $definition)
    {
        $result = $this->execute($this->getDialect()->createTable($tableName, $schemaName, $definition));
        return $result;
    }

    /**
     * Active CQL statement in the object
     *
     * @return string
     */
    public function getCQLStatement()
    {
        return $this->_cqlStatement;
    }

    /**
     * Active CQL statement in the object without replace bound parameters
     *
     * @return string
     */
    public function getRealCQLStatement()
    {
        return $this->_cqlStatement;
    }

    /**
     * Active CQL statement in the object
     *
     * @return array
     */
    public function getCQLBindTypes()
    {
        return $this->_cqlBindTypes;
    }

    /**
     * Active CQL variables
     *
     * @return array
     */
    public function getCqlVariables()
    {
        return $this->_cqlVariables;
    }

    /**
     * @return \Cassandra\Session
     */
    public function getClient()
    {
        return $this->_session;
    }

    /**
     * @return \Cassandra\Session
     */
    public function getSession()
    {
        return $this->_session;
    }

    /**
     * @return string
     */
    public function getCurrentKeyspace()
    {
        return $this->_keyspace;
    }

    /**
     * Check whether the database system requires a sequence to produce auto-numeric values
     *
     * @return boolean
     */
    public function supportSequences()
    {
        return false;
    }

    /**
     * Check whether the database system requires an explicit value for identity columns
     *
     * @return boolean
     */
    public function useExplicitIdValue()
    {
        return true;
    }

    /**
     * @return bool
     */
    public function isNestedTransactionsWithSavepoints()
    {
        return false;
    }

    /**
     * @return \Phalcon\Db\Dialect\Cassandra
     */
    public function getDialect()
    {
        if ($this->_dialect === null) {
            $this->setDialect(new \Phalcon\Db\Dialect\Cassandra());
        }
        return $this->_dialect;
    }

    /**
     * @param \Phalcon\Db\DialectInterface $dialectInterface
     */
    public function setDialect(\Phalcon\Db\DialectInterface $dialectInterface)
    {
        $this->_dialect = $dialectInterface;
    }

    public function affectedRows()
    {
        throw new Exception('Method unable');
    }

    public function lastInsertId($sequenceName = null)
    {
        throw new Exception('Method unable');
    }

    public function rollback($nesting = null)
    {
        throw new Exception('Method unable');
    }

    public function setNestedTransactionsWithSavepoints($nestedTransactionsWithSavepoints)
    {
        throw new Exception('Method unable');
    }

    public function createSavepoint($name)
    {
        throw new Exception('Method unable');
    }

    public function releaseSavepoint($name)
    {
        throw new Exception('Method unable');
    }

    public function rollbackSavepoint($name)
    {
        throw new Exception('Method unable');
    }

    public function describeReferences($table, $schema = null)
    {
        throw new Exception('Method unable');
    }

    public function getSQLStatement()
    {
        return $this->getCQLStatement();
    }

    public function getRealSQLStatement()
    {
        return $this->getRealCQLStatement();
    }

    public function getSQLBindTypes()
    {
        return $this->getCQLBindTypes();
    }

    public function listViews($schemaName = null)
    {
        /** @var \Cassandra\Session $session */
        $session = $this->getClient();
        $schema = $session->schema();
        if ($schemaName === null) {
            $schemaName = $this->_keyspace;
        }

        /** @var \Cassandra\Keyspace $keyspace */
        $keyspace = $schema->keyspace($schemaName);
        if ($keyspace === false) {
            throw new Exception("Unknown database '$schemaName'");
        }
        $result = $keyspace->materializedViews();

        $tables = [];
        foreach ($result as $tableName => $columnFamily) {
            $tables[] = $tableName;
        }
        return $tables;
    }

    public function dropForeignKey($tableName, $schemaName, $referenceName)
    {
    }

    public function addForeignKey($tableName, $schemaName, \Phalcon\Db\ReferenceInterface $reference)
    {
        throw new Exception('Method unable');
    }

    public function dropPrimaryKey($tableName, $schemaName)
    {
        throw new Exception('Method unable');
    }

    public function dropView($viewName, $schemaName = null, $ifExists = null)
    {
        throw new Exception('Method unable');
    }

    public function createView($viewName, array $definition, $schemaName = null)
    {
        throw new Exception('Method unable');
    }

    public function sharedLock($sqlQuery)
    {
        throw new Exception('Method unable');
    }

    public function forUpdate($sqlQuery)
    {
        throw new Exception('Method unable');
    }

    public function viewExists($viewName, $schemaName = null)
    {
        throw new Exception('Method unable');
    }

    public function getDefaultIdValue()
    {
        throw new Exception('Method unable');
    }

    public function getNestedTransactionSavepointName()
    {
        throw new Exception('Method unable');
    }
}

