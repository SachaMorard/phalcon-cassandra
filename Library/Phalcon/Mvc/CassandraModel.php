<?php
/*
 * This file is part of the PhalconCassandra package.
 *
 * (c) Sacha Morard <sachamorard@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Phalcon\Mvc;

use Phalcon\Mvc\Model\Exception;
use Phalcon\Mvc\Model\Resultset\Simple as SimpleResult;
use Phalcon\Cassandra\DataType;
use PhalconStart\Models\TestTableC;

class CassandraModel extends \Phalcon\Mvc\Model
{
    protected static $_async = false;

    protected static $_ttl = false;

    /**
     * @return $this
     */
    public function nextWillBeAsync()
    {
        self::$_async = true;
        return $this;
    }

    /**
     * @return $this
     */
    public function ttl($ttl)
    {
        self::$_ttl = (int) $ttl;
        return $this;
    }

    /**
     * Allows to query the first record that match the specified conditions
     *
     * <code>
     *
     * //What's the first robot in robots table?
     * $robot = Robots::findFirst();
     * echo "The robot name is ", $robot->name;
     *
     * //What's the first mechanical robot in robots table?
     * $robot = Robots::findFirst("type='mechanical'");
     * echo "The first mechanical robot name is ", $robot->name;
     *
     * //Get first virtual robot ordered by name
     * $robot = Robots::findFirst(array("type='virtual'", "order" => "name"));
     * echo "The first virtual robot name is ", $robot->name;
     *
     * </code>
     *
     * @param null $parameters
     * @param null $bindParams
     * @return bool
     */
    public static function findFirst($parameters = null, $bindParams = null)
    {
        /** @var CModel $model */
        $model = get_called_class();
        $instance = new $model;
        $cassandra = $instance->getDI()->get('dbCassandra');
        if (self::$_async) {
            self::$_async = false;
            $cassandra->nextWillBeAsync();
        }

        $cqlQuery = self::buildSelectQuery($instance, $parameters);

        $result = $cassandra->fetchOne($cqlQuery, \PDO::FETCH_ASSOC, $bindParams);
        if (!$result) {
            return false;
        }
        /** @var \Phalcon\Mvc\Model $instance */


        $instance->setDirtyState(self::DIRTY_STATE_PERSISTENT);
        $instance->assign($result, $instance->getModelsMetaData()->getColumnMap($instance));
        if(method_exists($instance, 'afterFetch')) {
            $instance->afterFetch();
        }
        return $instance;
    }


    /**
     * Allows to query a set of records that match the specified conditions
     *
     * <code>
     *
     * //How many robots are there?
     * $robots = Robots::find();
     * echo "There are ", count($robots), "\n";
     *
     * //How many mechanical robots are there?
     * $robots = Robots::find("type='mechanical'");
     * $robots = Robots::find("type=?", array('mechanical'));
     * echo "There are ", count($robots), "\n";
     *
     * //Get and print virtual robots ordered by name
     * $robots = Robots::find(array("type='virtual'", "order" => "name"));
     * foreach ($robots as $robot) {
     *       echo $robot->name, "\n";
     * }
     *
     * //Get first 100 virtual robots ordered by name
     * $robots = Robots::find(array("type='virtual'", "order" => "name DESC", "limit" => 100));
     * foreach ($robots as $robot) {
     *       echo $robot->name, "\n";
     * }
     * </code>
     *
     * @param null $parameters
     * @param null $bindParams
     * @return bool|SimpleResult
     */
    public static function find($parameters = null, $bindParams = null)
    {
        $model = get_called_class();
        /** @var TestTableC $instance */
        $instance = new $model;
        $ouais = $instance->getReadConnectionService();
        $cassandra = $instance->getDI()->get('dbCassandra');
        if (self::$_async) {
            self::$_async = false;
            $cassandra->nextWillBeAsync();
        }

        $cqlQuery = self::buildSelectQuery($instance, $parameters);
        $results = $cassandra->query($cqlQuery, $bindParams);
        if (!$results) {
            return false;
        }

        $columnMap = $instance->getModelsMetaData()->getColumnMap($instance);
        $resultSet = new SimpleResult($columnMap, $instance, $results);

        return $resultSet;
    }

    /**
     * @param \Phalcon\Mvc\ModelInterface $model
     * @param null $parameters
     * @return string
     */
    public static function buildSelectQuery(\Phalcon\Mvc\ModelInterface $model, $parameters = null)
    {
        $schema = $model->getSchema();
        $source = $model->getSource();
        $attributes = $model->getModelsMetaData()->getAttributes($model);
        if ($schema) {
            $table = $schema . '.' . $source;
        } else {
            $table = $source;
        }

        $where = '';
        $order = '';
        $limit = '';
        $select = implode(',', $attributes);

        if ($parameters && !is_array($parameters)) {
            $where = ' WHERE ' . $parameters;
        }

        if ($parameters && is_array($parameters)) {
            $where = isset($parameters[0]) ? ' WHERE ' . $parameters[0] : '';
            $order = isset($parameters['order']) ? ' ORDER BY  ' . $parameters['order'] : '';
            $limit = isset($parameters['limit']) ? ' LIMIT ' . $parameters['limit'] : '';
            $select = isset($parameters['select']) ? $parameters['select'] : $select;
        }

        $cqlQuery = 'SELECT ' . $select . ' FROM ' . $table . $where . $order . $limit;

        return $cqlQuery;
    }

    /**
     * Checks if the current record already exists or not
     *
     * @param \Phalcon\Mvc\Model\MetadataInterface metaData
     * @param \Phalcon\Db\AdapterInterface connection
     * @param string|array table
     * @return boolean
     */
    protected function _exists(\Phalcon\Mvc\Model\MetaDataInterface $metaData, \Phalcon\Db\AdapterInterface $connection, $table = null)
    {
        $primaryKeys = $metaData->getPrimaryKeyAttributes($this);
        $bindDataTypes = $metaData->getBindTypes($this);

        $numberPrimary = count($primaryKeys);
        if ($numberPrimary === 0) {
            return false;
        }

        if ($this->{$primaryKeys[0]} === null) {
            return false;
        }

        $uniqueKey = $this->_uniqueKey;
        if ($uniqueKey === null) {
            $this->_uniqueKey = $primaryKeys[0] . ' = ?';
            $this->_uniqueParams = [$this->{$primaryKeys[0]}];
            $this->_uniqueTypes = [$bindDataTypes[$primaryKeys[0]]];
        }

        if ($this->_dirtyState === 0) {
            return true;
        }

        $schema = $this->getSchema();
        $source = $this->getSource();
        if ($schema) {
            $table = $schema . '.' . $source;
        } else {
            $table = $source;
        }

        if (!$this->_uniqueParams) {
            return false;
        }

        /** @var \Phalcon\Db\Result\Cassandra $result */
        $result = $connection->query('SELECT COUNT(*) FROM ' . $table . ' WHERE ' . $this->_uniqueKey, $this->_uniqueParams);

        $this->_dirtyState = self::DIRTY_STATE_PERSISTENT;
        if ($result->fetchAll() !== 0) {
            return true;
        }
        return false;
    }

    /**
     * Force INSERT (without existence testing)
     *
     * @throws \Phalcon\Mvc\Model\ValidationFailed
     */
    public function insert()
    {
        $metadata = $this->getModelsMetaData();

        /**
         * Query the identity field
         */
        $identityField = $metadata->getIdentityField($this);
        $exists = false;

        /**
         * _preSave() makes all the validations
         */
        if (!$this->_preSave($metadata, $exists, $identityField)) {
            throw new \Phalcon\Mvc\Model\ValidationFailed($this, $this->getMessages());
        }

        $writeConnection = $this->getWriteConnection();
        if (self::$_async) {
            self::$_async = false;
            $writeConnection->nextWillBeAsync();
        }

        if (self::$_ttl) {
            $writeConnection->ttl(self::$_ttl);
            self::$_ttl = false;
        }

        $schema = $this->getSchema();
        $source = $this->getSource();

        if ($schema) {
            $table = [$schema, $source];
        } else {
            $table = $source;
        }

        $success = $this->_doLowInsert($metadata, $writeConnection, $table, $identityField);

        if ($this->getDi()->get('config')->application->debug === true) {
            time_nanosleep(0, 1000000);
        }

        /**
         * Change the dirty state to persistent
         */
        if ($success) {
            $this->_dirtyState = self::DIRTY_STATE_PERSISTENT;
        }
    }

    public function update($data = null, $whiteList = null)
    {
        if (self::$_async) {
            self::$_async = false;
            $this->getWriteConnection()->nextWillBeAsync();
        }
        if (self::$_ttl) {
            $this->getWriteConnection()->ttl(self::$_ttl);
            self::$_ttl = false;
        }
        return parent::update($data, $whiteList);
    }

    public function delete()
    {
        $metadata = $this->getModelsMetaData();
        $dataTypes = $metadata->getDataTypes($this);
        $columnMap = $metadata->getColumnMap($this);
        foreach ($columnMap as $dbColumnName => $modelColumnName) {
            $this->{$modelColumnName} = DataType::pack($dataTypes[$dbColumnName], $this->{$modelColumnName});
        }

        return parent::delete();
    }

    protected function _preSave(\Phalcon\Mvc\Model\MetadataInterface $metaData, $exists, $identityField)
    {
        $dataTypes = $metaData->getDataTypes($this);
        $columnMap = $metaData->getColumnMap($this);
        foreach ($columnMap as $dbColumnName => $modelColumnName) {
            $this->{$modelColumnName} = DataType::pack($dataTypes[$dbColumnName], $this->{$modelColumnName});
        }
        return parent::_preSave($metaData, $exists, $identityField);
    }


    // Methods not enabled

    protected function _preSaveRelatedRecords(\Phalcon\Db\AdapterInterface $connection, $related)
    {
        throw new Exception('Method not enabled');
    }

    protected function _postSaveRelatedRecords(\Phalcon\Db\AdapterInterface $connection, $related)
    {
        throw new Exception('Method not enabled');
    }

    public function getOperationMade()
    {
        throw new Exception('Method not enabled');
    }

    protected function hasOne($fields, $referenceModel, $referencedFields, $options = null)
    {
        throw new Exception('Method not enabled');
    }

    protected function belongsTo($fields, $referenceModel, $referencedFields, $options = null)
    {
        throw new Exception('Method not enabled');
    }

    protected function hasMany($fields, $referenceModel, $referencedFields, $options = null)
    {
        throw new Exception('Method not enabled');
    }

    protected function hasManyToMany($fields, $intermediateModel, $intermediateFields, $intermediateReferencedFields, $referenceModel, $referencedFields, $options = null)
    {
        throw new Exception('Method not enabled');
    }

    public function getRelated($alias, $arguments = null)
    {
        throw new Exception('Method not enabled');
    }

    protected function _getRelatedRecords($modelName, $method, $arguments)
    {
        throw new Exception('Method not enabled');
    }

    public static function query(\Phalcon\DiInterface $dependencyInjector = null)
    {
        throw new Exception('Method not enabled');
    }

    protected static function _groupResult($functionName, $alias, $parameters)
    {
        throw new Exception('Method not enabled');
    }

    public static function count($parameters = null)
    {
        throw new Exception('Method not enabled');
    }

    public static function sum($parameters = null)
    {
        throw new Exception('Method not enabled');
    }

    public static function maximum($parameters = null)
    {
        throw new Exception('Method not enabled');
    }

    public static function minimum($parameters = null)
    {
        throw new Exception('Method not enabled');
    }

    public static function average($parameters = null)
    {
        throw new Exception('Method not enabled');
    }

}
