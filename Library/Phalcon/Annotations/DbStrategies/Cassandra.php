<?php
/*
 * This file is part of the PhalconCassandra package.
 *
 * (c) Sacha Morard <sachamorard@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Phalcon\Annotations\DbStrategies;

use Phalcon\Annotations\ModelStrategy;
use Phalcon\Mvc\Model\MetaData;
use Phalcon\Cassandra\DataType;

class Cassandra
{

    /**
     * @return array
     */
    public static function getMetaData(\Phalcon\Annotations\Reflection $reflection, $properties,array $indexes)
    {
        $attributes = array();
        $dataTypes = array();
        $nullables = array();
        $dataTypesBind = array();
        $numericTypes = array();
        $primaryKeys = array();
        $nonPrimaryKeys = array();

        foreach ($properties as $name => $collection) {
            if ($collection->has('Column')) {
                $arguments = $collection->get('Column')->getArguments();

                /**
                 * Get the column's name
                 */
                if (isset($arguments['column'])) {
                    $columnName = $arguments['column'];
                } else {
                    $columnName = $name;
                }

                /**
                 * Check for the 'type' parameter in the 'Column' annotation
                 */
                if (isset($arguments['type'])) {
                    switch ($arguments['type']) {
                        case 'int':
                            $dataTypes[$columnName] = DataType::TYPE_INTEGER;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            $numericTypes[$columnName] = true;
                            break;
                        case 'timestamp':
                            $dataTypes[$columnName] = DataType::TYPE_DATETIME;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'datetime':
                            $dataTypes[$columnName] = DataType::TYPE_DATETIME;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'varchar':
                            $dataTypes[$columnName] = DataType::TYPE_TEXT;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'float':
                            $dataTypes[$columnName] = DataType::TYPE_FLOAT;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            $numericTypes[$columnName] = true;
                            break;
                        case 'decimal':
                            $dataTypes[$columnName] = DataType::TYPE_DECIMAL;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            $numericTypes[$columnName] = true;
                            break;
                        case 'boolean':
                            $dataTypes[$columnName] = DataType::TYPE_BOOLEAN;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'uuid':
                            $dataTypes[$columnName] = DataType::TYPE_UUID;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'timeuuid':
                            $dataTypes[$columnName] = DataType::TYPE_TIMEUUID;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'double':
                            $dataTypes[$columnName] = DataType::TYPE_DOUBLE;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            $numericTypes[$columnName] = true;
                            break;
                        case 'blob':
                            $dataTypes[$columnName] = DataType::TYPE_BLOB;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'bigint':
                            $dataTypes[$columnName] = DataType::TYPE_BIGINT;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            $numericTypes[$columnName] = true;
                            break;
                        case 'varint':
                            $dataTypes[$columnName] = DataType::TYPE_VARINT;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            $numericTypes[$columnName] = true;
                            break;
                        case 'set':
                            $dataTypes[$columnName] = DataType::TYPE_SET;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'list':
                            $dataTypes[$columnName] = DataType::TYPE_LIST;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'map':
                            $dataTypes[$columnName] = DataType::TYPE_MAP;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'inet':
                            $dataTypes[$columnName] = DataType::TYPE_INET;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            break;
                        case 'counter':
                            $dataTypes[$columnName] = DataType::TYPE_COUNTER;
                            $dataTypesBind[$columnName] = $dataTypes[$columnName];
                            $numericTypes[$columnName] = true;
                            break;
                    }
                } else {
                    $dataTypes[$columnName] = DataType::TYPE_VARCHAR;
                    $dataTypesBind[$columnName] = $dataTypes[$columnName];
                }

                /**
                 * Check for the 'nullable' parameter in the 'Column' annotation
                 */
                if (!$collection->has('Primary')) {
                    if (isset($arguments['nullable'])) {
                        if (!$arguments['nullable']) {
                            $nullables[] = $columnName;
                        }
                    }
                } else {
                    $nullables[] = $columnName;
                }

                $attributes[] = $columnName;
                /**
                 * Check if the attribute is marked as primary
                 */
                if ($collection->has('Primary')) {
                    $primaryKeys[] = $columnName;
                } else {
                    $nonPrimaryKeys[] = $columnName;
                }
            }
        }
        return array(
            //Every column in the mapped table
            MetaData::MODELS_ATTRIBUTES => $attributes,
            //Every column part of the primary key
            MetaData::MODELS_PRIMARY_KEY => $primaryKeys,
            //Every column that isn't part of the primary key
            MetaData::MODELS_NON_PRIMARY_KEY => $nonPrimaryKeys,
            //Every column that doesn't allows null values
            MetaData::MODELS_NOT_NULL => $nullables,
            //Every column and their data types
            MetaData::MODELS_DATA_TYPES => $dataTypes,
            //The columns that have numeric data types
            MetaData::MODELS_DATA_TYPES_NUMERIC => $numericTypes,
            //The identity column, use boolean false if the model doesn't have
            //an identity column
            MetaData::MODELS_IDENTITY_COLUMN => false,
            //How every column must be bound/casted
            MetaData::MODELS_DATA_TYPES_BIND => $dataTypesBind,
            //Fields that must be ignored from INSERT SQL statements
            MetaData::MODELS_AUTOMATIC_DEFAULT_INSERT => array(),
            //Fields that must be ignored from UPDATE SQL statements
            MetaData::MODELS_AUTOMATIC_DEFAULT_UPDATE => array(),
            // Default values
            MetaData::MODELS_DEFAULT_VALUES => array(),
            //Size of fields
            ModelStrategy::METADATA_SIZES_OF_FIELDS => array(),
            //Table indexes
            ModelStrategy::METADATA_TABLE_INDEXES => $indexes
        );
    }
}