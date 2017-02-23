<?php
/*
 * This file is part of the PhalconCassandra package.
 *
 * (c) Sacha Morard <sachamorard@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Phalcon\Db\Dialect;

use Phalcon\Cassandra\DataType;
use Phalcon\Db\Column;
use Phalcon\Db\Exception;

/**
 * Phalcon\Db\Dialect\Cassandra
 *
 * Generates database specific SQL for the Cassandra RBDM
 */
class Cassandra extends \Phalcon\Db\Dialect implements \Phalcon\Db\DialectInterface
{

    protected $_escapeChar = "'";

    protected $_dataType;

    protected $_type = 'cassandra';

    protected $_dialectType = 'cassandra';

    protected $_currentKeyspace;

    /**
     * Constructor
     */
    public function __construct()
    {
        $this->_dataType = new DataType();
    }

    /**
     * Gets the column name in Cassandra
     *
     * @param \Phalcon\Db\ColumnInterface column
     * @return string
     */
    public function getColumnDefinition(\Phalcon\Db\ColumnInterface $column)
    {
        return $this->_dataType->getCassandraTypeFor($column->getType());
    }

    /**
     * Generates CQL to add a column to a table
     *
     * @param $tableName
     * @param $schemaName
     * @param \Phalcon\Db\ColumnInterface $column
     * @return string
     */
    public function addColumn($tableName, $schemaName = null, \Phalcon\Db\ColumnInterface $column)
    {
        if (!$schemaName) {
            $cql = 'ALTER TABLE ' . $tableName . ' ADD ';
        } else {
            $cql = 'ALTER TABLE ' . $schemaName . '.' . $tableName . ' ADD ';
        }
        $cql .= $column->getName() . ' ' . $this->getColumnDefinition($column);
        return $cql;
    }


    /**
     * Generates CQL to modify a column in a table
     *
     * @param $tableName
     * @param $schemaName
     * @param \Phalcon\Db\ColumnInterface $column
     * @return string
     */
    public function modifyColumn($tableName, $schemaName = null, \Phalcon\Db\ColumnInterface $column, \Phalcon\Db\ColumnInterface $currentColumn = null)
    {
        if (!$schemaName) {
            $cql = 'ALTER TABLE ' . $tableName . ' ALTER ';
        } else {
            $cql = 'ALTER TABLE ' . $schemaName . '.' . $tableName . ' ALTER ';
        }
        $cql .= $column->getName() . ' TYPE ' . $this->getColumnDefinition($column);
        return $cql;
    }


    /**
     * Generates CQL to delete a column from a table
     *
     * @param $tableName
     * @param $schemaName
     * @param $columnName
     * @return    string
     */
    public function dropColumn($tableName, $schemaName = null, $columnName)
    {
        if (!$schemaName) {
            $cql = 'ALTER TABLE ' . $tableName . ' DROP ';
        } else {
            $cql = 'ALTER TABLE ' . $schemaName . '.' . $tableName . ' DROP ';
        }
        $cql .= $columnName;
        return $cql;
    }


    /**
     * Generates CQL to add an index to a table
     *
     * @param $tableName
     * @param $schemaName
     * @param \Phalcon\Db\IndexInterface $index
     * @return string
     * @throws Exception
     */
    public function addIndex($tableName, $schemaName = null, \Phalcon\Db\IndexInterface $index)
    {
        if (count($index->getColumns()) > 1) {
            throw new Exception('Multiple index are not allowed in Cassandra');
        }
        $name = '';
        if ($index->getName()) {
            $name = $index->getName() . ' ';
        }

        if (!$schemaName) {
            $cql = 'CREATE INDEX IF NOT EXISTS ' . $name . 'ON ' . $tableName . ' (';
        } else {
            $cql = 'CREATE INDEX IF NOT EXISTS ' . $name . 'ON ' . $schemaName . '.' . $tableName . ' (';
        }
        if(is_object($index->getColumns()[0])){
            $cql .= $index->getColumns()[0]->getName() . ')';
        }else{
            $cql .= $index->getColumns()[0] . ')';
        }

        return $cql;
    }

    /**
     * Generates SQL to delete an index from a table
     *
     * @param mixed $tableName
     * @param null $schemaName
     * @param mixed $indexName
     * @return string
     */
    public function dropIndex($tableName, $schemaName = null, $indexName)
    {
        if (!$schemaName) {
            $cql = 'DROP INDEX ';
        } else {
            $cql = 'DROP INDEX ' . $schemaName . '.';
        }
        $cql .= $indexName;
        return $cql;
    }


    /**
     * Adding Primary Key on existing or new column is not allowed.
     *
     * @param mixed $tableName
     * @param null $schemaName
     * @param \Phalcon\Db\IndexInterface $index
     * @throws Exception
     * @return Exception
     */
    public function addPrimaryKey($tableName, $schemaName = null, \Phalcon\Db\IndexInterface $index)
    {
        throw new Exception('Adding Primary Key on existing or new column is not allowed.');
    }


    /**
     * Dropping Primary Key is not allowed.
     *
     * @param    mixed $tableName
     * @param    null $schemaName
     * @throws Exception
     * @return Exception
     */
    public function dropPrimaryKey($tableName, $schemaName = null)
    {
        throw new Exception($tableName.' Dropping Primary Key is not allowed.');
    }



    /**
     * Cassandra is not a relational Database. You can't add foreign key
     *
     * @param mixed $tableName
     * @param null $schemaName
     * @param \Phalcon\Db\ReferenceInterface $reference
     * @throws Exception
     * @return Exception
     */
    public function addForeignKey($tableName, $schemaName = null, \Phalcon\Db\ReferenceInterface $reference)
    {
        throw new Exception('Cassandra is not a relational Database. You can\'t use foreign key');
    }


    /**
     * Cassandra is not a relational Database. You can't use foreign key
     *
     * @param mixed $tableName
     * @param null $schemaName
     * @param mixed $referenceName
     * @throws Exception
     * @return Exception
     */
    public function dropForeignKey($tableName, $schemaName = null, $referenceName)
    {
        throw new Exception('Cassandra is not a relational Database. You can\'t use foreign key');
    }




    /**
     * Generates CQL to create a table in Cassandra
     *
     * @param $tableName
     * @param null $schemaName
     * @param array $definition
     * @return string
     * @throws Exception
     */
    public function createTable($tableName, $schemaName = null, array $definition)
    {
        if (!isset($definition['columns']) || count($definition['columns']) === 0) {
            throw new Exception('The table must contain at least one column');
        }

        if (!$schemaName) {
            $cql = 'CREATE TABLE IF NOT EXISTS ' . $tableName . ' (';
        } else {
            $cql = 'CREATE TABLE IF NOT EXISTS ' . $schemaName . '.' . $tableName . ' (';
        }

        $primary = array();
        foreach ($definition['columns'] as $column) {
            /** @var $column Column */
            $cql .= $column->getName() . ' ' . $this->getColumnDefinition($column) . ', ';
            if ($column->isPrimary()) {
                $primary[] = $column->getName();
            }
        }
        $cql .= 'PRIMARY KEY (' . implode(', ', $primary) . '))';

        return $cql;
    }


    /**
     * Generates CQL to drop a table
     *
     * @param  string tableName
     * @param  string schemaName
     * @param  boolean ifExists
     * @return string
     */
    public function dropTable($tableName, $schemaName = null, $ifExists = null)
    {
        if ($ifExists) {
            $ifExists = 'IF EXISTS ';
        } else {
            $ifExists = '';
        }
        if (!$schemaName) {
            $cql = 'DROP TABLE ' . $ifExists . $tableName;
        } else {
            $cql = 'DROP TABLE ' . $ifExists . $schemaName . '.' . $tableName;
        }
        return $cql;
    }


    /**
     * View not exists in Cassandra
     *
     * @param string viewName
     * @param array definition
     * @param string schemaName
     * @throws Exception
     * @return Exception
     */
    public function createView($viewName, array $definition = null, $schemaName = null)
    {
        throw new Exception('View not exists in Cassandra');
    }


    /**
     * View not exists in Cassandra
     *
     * @param string viewName
     * @param string schemaName
     * @param boolean ifExists
     * @throws Exception
     * @return Exception
     */
    public function dropView($viewName, $schemaName = null, $ifExists = null)
    {
        throw new Exception('View not exists in Cassandra');
    }


    /**
     * @param string $tableName
     * @param null $schemaName
     * @throws Exception
     * @return Exception
     */
    public function tableExists($tableName, $schemaName = null)
    {
        throw new Exception('No CQL to test if table exists in cassandra, use driver for that');
    }


    /**
     * View not exists in Cassandra
     *
     * @param string viewName
     * @param string schemaName
     * @throws Exception
     * @return Exception
     */
    public function viewExists($viewName, $schemaName = null)
    {
        throw new Exception('View not exists in Cassandra');
    }


    /**
     * @param string $tableName
     * @param null $schemaName
     * @throws Exception
     * @return Exception
     */
    public function describeColumns($tableName, $schemaName = null)
    {
        throw new Exception('No CQL to describe columns, use driver for that');
    }

    /**
     * @return mixed
     */
    public function getCurrentKeyspace()
    {
        if($this->_currentKeyspace === null){
            $di = \Phalcon\Di::getDefault();
            $config = $di->get('config');
            $this->_currentKeyspace = $config->cassandra->keyspace;
        }
        return $this->_currentKeyspace;
    }

    /**
     * @param null $schemaName
     * @throws Exception
     * @return Exception
     */
    public function listTables($schemaName = null)
    {
        throw new Exception('No CQL to list cassandra tables, use driver for that');
    }


    /**
     * View not exists in Cassandra
     *
     * @param string schemaName
     * @throws Exception
     */
    public function listViews($schemaName = null)
    {
        throw new Exception('View not exists in Cassandra');
    }


    /**
     * @param string $tableName
     * @param null $schemaName
     * @throws Exception
     * @return Exception
     */
    public function describeIndexes($tableName, $schemaName = null)
    {
        throw new Exception('No CQL to describe indexes if table exists in cassandra, use driver for that');
    }

    /**
     * @param mixed $tableName
     * @param null $schemaName
     * @throws Exception
     * @return Exception
     */
    public function describeReferences($tableName, $schemaName = null)
    {
        throw new Exception('Cassandra is not a relational Database. There is no references beetween tables');
    }

    /**
     * There is no significant table options with cassandra
     *
     * @param $table
     * @param null $schema
     * @throws Exception
     * @return Exception
     */
    public function tableOptions($table, $schema = null)
    {
        throw new Exception('There is no significant table options with cassandra');
    }

    /**
     * Method createSavePoint unable
     *
     * @param $name
     * @throws Exception
     * @return Exception
     */
    public function createSavePoint($name)
    {
        throw new Exception('Method createSavePoint unable');
    }

    /**
     * @param $cqlQuery
     * @throws Exception
     * @return Exception
     */
    public function forUpdate($cqlQuery)
    {
        throw new Exception('Method forUpdate unable');
    }

    /**
     * @param $cqlQuery
     * @throws Exception
     * @return Exception
     */
    public function sharedLock($cqlQuery)
    {
        throw new Exception('Method sharedLock unable');
    }

    /**
     * @param array $definition
     * @throws Exception
     * @return Exception
     */
    public function select(array $definition)
    {
        throw new Exception('Method select unable');
    }

    /**
     * @return bool
     */
    public function supportsSavepoints()
    {
        return false;
    }

}

