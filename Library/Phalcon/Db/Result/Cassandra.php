<?php
/*
 * This file is part of the PhalconCassandra package.
 *
 * (c) Sacha Morard <sachamorard@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Phalcon\Db\Result;

use Phalcon\Cassandra\DataType;

/**
 * Phalcon\Db\Result\Cassandra
 *
 * Encapsulates the resultset internals
 *
 * <code>
 *    $result = $connection->query("SELECT * FROM robots ORDER BY name");
 *    $result->setFetchMode(PDO::FETCH_NUM);
 *    while ($robot = $result->fetchArray()) {
 *        print_r($robot);
 *    }
 * </code>
 */
class Cassandra
{
    /**
     * @var \Phalcon\Db\Adapter\Cassandra
     */
    protected $_connection;

    /**
     * @var \Cassandra\Rows
     */
    protected $_cassandraResult;

    /**
     * @var \Cassandra\FutureRows
     */
    protected $_futureRows;

    /**
     * @var \Cassandra\Session
     */
    protected $_session;

    protected $_fetchMode = \PDO::FETCH_OBJ;

    protected $_cqlStatement;

    protected $_bindParams;

    protected $_bindTypes;

    protected $_rowCount = false;

    protected $_rowNum = 0;

    protected $_fetchAll = null;

    /**
     * @param \Phalcon\Db\Adapter\Cassandra $connection
     * @param \Cassandra\Rows $result
     * @param null $cqlStatement
     * @param null $bindParams
     * @param null $bindTypes
     */
    public function __construct(\Phalcon\Db\Adapter\Cassandra $connection, $result, $cqlStatement = null, $bindParams = null, $bindTypes = null)
    {
        $this->_connection = $connection;
        $this->_session = $connection->getSession();

        if($result instanceof \Cassandra\FutureRows){
            $this->_futureRows = $result;
        }else{
            $this->_cassandraResult = $result;
        }


        if ($cqlStatement !== null) {
            $this->_cqlStatement = $cqlStatement;
        }

        if ($bindParams !== null) {
            $this->_bindParams = $bindParams;
        }

        if ($bindTypes !== null) {
            $this->_bindTypes = $bindTypes;
        }
    }

    /**
     * Allows to execute the statement again. Some database systems don't support scrollable cursors,
     * So, as cursors are forward only, we need to execute the cursor again to fetch rows from the begining
     *
     * @return boolean
     */
    public function execute()
    {
        return $this->_connection->execute($this->_cqlStatement, $this->_bindParams, $this->_bindTypes);
    }

    /**
     * Gets number of rows returned by a resulset
     *
     *<code>
     *    $result = $connection->query("SELECT * FROM robots ORDER BY name");
     *    echo 'There are ', $result->numRows(), ' rows in the resulset';
     *</code>
     *
     * @return int
     */
    public function numRows()
    {
        $rowCount = $this->_rowCount;
        if ($rowCount === false) {
            if($this->_cassandraResult === null && $this->_futureRows !== null){
                $this->_cassandraResult = $this->_futureRows->get();
            }
            if($this->_cassandraResult !== null){
                $rowCount = $this->_cassandraResult->count();
            }
        }
        $this->_rowCount = $rowCount;

        return $rowCount;
    }

    /**
     * Returns an array of arrays containing all the records in the result
     * This method is affected by the active fetch flag set using \Phalcon\Db\Result\Pdo::setFetchMode
     *
     *<code>
     *    $result = $connection->query("SELECT * FROM robots ORDER BY name");
     *    $robots = $result->fetchAll();
     *</code>
     *
     * @return array
     */
    public function fetchAll()
    {
        if ($this->numRows() !== false) {
            /**
             * If the sql_statement starts with SELECT COUNT(*) we don't make the count
             */
            if(stripos($this->_cqlStatement, 'SELECT count') !== 0) {
                if(stripos($this->_cqlStatement, 'SELECT') === 0) {
                    return $this->_fetchResults();
                }

            } else {
                /**
                 * We have to return the value of count
                 */
                if($this->_cassandraResult === null && $this->_futureRows !== null){
                    $this->_cassandraResult = $this->_futureRows->get();
                }
                return $this->_cassandraResult->first()['count']->value();
            }
        }
        return false;
    }

    /**
     * Fetches an array/object of strings that corresponds to the fetched row, or FALSE if there are no more rows.
     * This method is affected by the active fetch flag set using \Phalcon\Db\Result\Pdo::setFetchMode
     *
     *<code>
     *    $result = $connection->query("SELECT * FROM robots ORDER BY name");
     *    $result->setFetchMode(PDO::FETCH_OBJ);
     *    while ($robot = $result->fetch()) {
     *        echo robot->name;
     *    }
     *</code>
     *
     * @return mixed
     */
    public function fetch()
    {
        $result = $this->_fetchResults($this->_rowNum);
        if($this->_fetchResults($this->_rowNum) !== false){
            $this->_rowNum++;
            return $result;
        }
        return false;
    }

    /**
     * Returns an array of strings that corresponds to the fetched row, or FALSE if there are no more rows.
     * This method is affected by the active fetch flag set using \Phalcon\Db\Result\Pdo::setFetchMode
     *
     *<code>
     *    $result = $connection->query("SELECT * FROM robots ORDER BY name");
     *    $result->setFetchMode(PDO::FETCH_NUM);
     *    while ($robot = result->fetchArray()) {
     *        print_r($robot);
     *    }
     *</code>
     *
     * @return mixed
     */
    public function fetchArray()
    {
        $result = $this->_fetchResults($this->_rowNum);
        if($this->_fetchResults($this->_rowNum) !== false){
            $this->_rowNum++;
            return $result;
        }
        return false;
    }

    /**
     * Moves internal resulset cursor to another position letting us to fetch a certain row
     *
     *<code>
     *    $result = $connection->query("SELECT * FROM robots ORDER BY name");
     *    $result->dataSeek(2); // Move to third row on result
     *    $row = $result->fetch(); // Fetch third row
     *</code>
     *
     * @param long number
     */
    public function dataSeek($number)
    {
        $this->_rowNum = $number;
        return;
    }

    /**
     * Changes the fetching mode affecting \Phalcon\Db\Result\Cassandra::fetch()
     *
     *<code>
     *    //Return array with integer indexes
     *    $result->setFetchMode(PDO::FETCH_NUM);
     *
     *    //Return associative array without integer indexes
     *    $result->setFetchMode(PDO::FETCH_ASSOC);
     *
     *    //Return associative array together with integer indexes
     *    $result->setFetchMode(PDO::FETCH_BOTH);
     *
     *    //Return an object
     *    $result->setFetchMode(PDO::FETCH_OBJ);
     *</code>
     *
     * @param int fetchMode
     */
    public function setFetchMode($fetchMode)
    {
        $this->_fetchAll = null;
        switch ($fetchMode) {
            case \PDO::FETCH_NUM :
                $this->_fetchMode = \PDO::FETCH_NUM;
                break;
            case \PDO::FETCH_ASSOC :
                $this->_fetchMode = \PDO::FETCH_ASSOC;
                break;
            case \PDO::FETCH_BOTH :
                $this->_fetchMode = \PDO::FETCH_BOTH;
                break;
            case \PDO::FETCH_OBJ :
                $this->_fetchMode = \PDO::FETCH_OBJ;
                break;
        }
    }

    /**
     * Gets the internal \Cassandra\Rows result object
     *
     * @return \Cassandra\Rows
     */
    public function getInternalResult()
    {
        return $this->_cassandraResult;
    }

    /**
     * @return array
     */
    protected function _fetchResults($rowNum = null)
    {
        if($this->_cassandraResult === null && $this->_futureRows !== null){
            $this->_cassandraResult = $this->_futureRows->get();
        }

        if($rowNum === null){
            $results = [];
            if ($this->_fetchAll !== null) {
                return $this->_fetchAll;
            }

            while (true) {
                foreach ($this->_cassandraResult as $row) {
                    $values = [];
                    $namesValues = [];
                    $resultObject = new \stdClass();
                    foreach ($row as $columnName => $columnValue) {
                        $value = DataType::unpack($columnValue);
                        if (($this->_fetchMode === \PDO::FETCH_NUM) || ($this->_fetchMode === \PDO::FETCH_BOTH)) {
                            $values[] = $value;
                        }
                        if (($this->_fetchMode === \PDO::FETCH_ASSOC) || ($this->_fetchMode === \PDO::FETCH_BOTH)) {
                            $namesValues[$columnName] = $value;
                        }
                        if ($this->_fetchMode === \PDO::FETCH_OBJ) {
                            $resultObject->{$columnName} = $value;
                        }
                    }
                    switch ($this->_fetchMode) {
                        case \PDO::FETCH_NUM :
                            $results[] = $values;
                            break;
                        case \PDO::FETCH_ASSOC :
                            $results[] = $namesValues;
                            break;
                        case \PDO::FETCH_BOTH :
                            $results[] = array_merge($values, $namesValues);
                            break;
                        case \PDO::FETCH_OBJ :
                            $results[] = $resultObject;
                            break;
                    }
                }
                if ($this->_cassandraResult->isLastPage()) {
                    break;
                }
                $this->_cassandraResult->nextPage();
            }
            $this->_fetchAll = $results;
            return $results;
        }

        if($this->_cassandraResult->offsetExists($rowNum)){
            $values = [];
            $namesValues = [];
            $resultObject = new \stdClass();
            foreach ($this->_cassandraResult->offsetGet($rowNum) as $columnName => $columnValue) {
                $value = DataType::unpack($columnValue);
                if (($this->_fetchMode === \PDO::FETCH_NUM) || ($this->_fetchMode === \PDO::FETCH_BOTH)) {
                    $values[] = $value;
                }
                if (($this->_fetchMode === \PDO::FETCH_ASSOC) || ($this->_fetchMode === \PDO::FETCH_BOTH)) {
                    $namesValues[$columnName] = $value;
                }
                if ($this->_fetchMode === \PDO::FETCH_OBJ) {
                    $resultObject->{$columnName} = $value;
                }
            }

            switch ($this->_fetchMode) {
                case \PDO::FETCH_NUM :
                    $result = $values;
                    break;
                case \PDO::FETCH_ASSOC :
                    $this->_fetchMode = \PDO::FETCH_ASSOC;
                    $result = $namesValues;
                    break;
                case \PDO::FETCH_BOTH :
                    $result = array_merge($values, $namesValues);
                    break;
                case \PDO::FETCH_OBJ :
                    $result = $resultObject;
                    break;
            }
            return $result;
        }
        return false;


    }

}
