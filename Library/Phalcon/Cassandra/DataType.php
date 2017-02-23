<?php
/*
 * This file is part of the PhalconCassandra package.
 *
 * (c) Sacha Morard <sachamorard@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Phalcon\Cassandra;

/**
 * Class DataType
 * @package cassandra\Schema
 */
class DataType
{

    const TYPE_INTEGER = \Phalcon\Db\Column::TYPE_INTEGER;   // 0
    const TYPE_DATETIME = \Phalcon\Db\Column::TYPE_DATETIME; // 4
    const TYPE_TEXT = \Phalcon\Db\Column::TYPE_TEXT;         // 6
    const TYPE_FLOAT = \Phalcon\Db\Column::TYPE_FLOAT;       // 7
    const TYPE_BOOLEAN = \Phalcon\Db\Column::TYPE_BOOLEAN;   // 8
    const TYPE_UUID = 9;
    const TYPE_TIMEUUID = 10;
    const TYPE_DOUBLE = 12;
    const TYPE_BLOB = 13;
    const TYPE_BIGINT = 14;
    const TYPE_VARINT = 15;
    const TYPE_SET = 16;
    const TYPE_LIST = 17;
    const TYPE_MAP = 18;
    const TYPE_INET = 19;
    const TYPE_COUNTER = 20;
    const TYPE_DECIMAL = 21;

    /**
     * @var array
     */
    protected $_phalcon_type_map = array(
        'int' => self::TYPE_INTEGER,  // 0 => int
        'timestamp' => self::TYPE_DATETIME, // 4 => timestamp
        'varchar' => self::TYPE_TEXT,     // 6 => varchar
        'text' => self::TYPE_TEXT,     // 6 => text
        'float' => self::TYPE_FLOAT,    // 7 => float
        'boolean' => self::TYPE_BOOLEAN,  // 8 => boolean
        'uuid' => self::TYPE_UUID,     //uuid
        'timeuuid' => self::TYPE_TIMEUUID, //timeuuid
        'double' => self::TYPE_DOUBLE,   //double
        'blob' => self::TYPE_BLOB,     //blob
        'bigint' => self::TYPE_BIGINT,   //bigint
        'varint' => self::TYPE_VARINT,   //varint
        'set' => self::TYPE_SET,      //set
        'list' => self::TYPE_LIST,     //list
        'map' => self::TYPE_MAP,      //map
        'inet' => self::TYPE_INET,     //inet
        'counter' => self::TYPE_COUNTER,  //counter
        'decimal' => self::TYPE_DECIMAL,  //decimal
    );

    /**
     * @var array
     */
    protected $_cassandra_type_map = array(
        self::TYPE_INTEGER => 'int',
        self::TYPE_DATETIME => 'timestamp',
        self::TYPE_TEXT => 'varchar',
        self::TYPE_FLOAT => 'float',
        self::TYPE_BOOLEAN => 'boolean',
        self::TYPE_UUID => 'uuid',
        self::TYPE_TIMEUUID => 'timeuuid',
        self::TYPE_DOUBLE => 'double',
        self::TYPE_BLOB => 'blob',
        self::TYPE_BIGINT => 'bigint',
        self::TYPE_VARINT => 'varint',
        self::TYPE_SET => 'set',
        self::TYPE_LIST => 'list',
        self::TYPE_MAP => 'map',
        self::TYPE_INET => 'inet',
        self::TYPE_COUNTER => 'counter',
        self::TYPE_DECIMAL => 'decimal'
    );

    /**
     * @var array
     */
    protected $_phalcon_bind_map = array(
        'int' => \Phalcon\Db\Column::BIND_PARAM_INT,     // 0 => int
        'timestamp' => \Phalcon\Db\Column::BIND_SKIP,          // 4 => timestamp
        'varchar' => \Phalcon\Db\Column::BIND_PARAM_STR,     // 6 => varchar
        'text' => \Phalcon\Db\Column::BIND_PARAM_STR,     // 6 => varchar
        'float' => \Phalcon\Db\Column::BIND_PARAM_DECIMAL, // 7 => float
        'boolean' => \Phalcon\Db\Column::BIND_PARAM_BOOL,    // 8 => boolean
        'uuid' => \Phalcon\Db\Column::BIND_SKIP,          //uuid
        'timeuuid' => \Phalcon\Db\Column::BIND_SKIP,          //timeuuid
        'double' => \Phalcon\Db\Column::BIND_PARAM_DECIMAL, //double
        'blob' => \Phalcon\Db\Column::BIND_SKIP,          //blob
        'bigint' => \Phalcon\Db\Column::BIND_PARAM_INT,     //bigint
        'varint' => \Phalcon\Db\Column::BIND_PARAM_INT,     //varint
        'set' => \Phalcon\Db\Column::BIND_SKIP,          //set
        'list' => \Phalcon\Db\Column::BIND_SKIP,          //list
        'map' => \Phalcon\Db\Column::BIND_SKIP,          //map
        'inet' => \Phalcon\Db\Column::BIND_PARAM_STR,     //inet
        'counter' => \Phalcon\Db\Column::BIND_PARAM_INT,     //counter
        'decimal' => \Phalcon\Db\Column::BIND_PARAM_DECIMAL, //decimal
    );



    /**
     * Return de correct Phalcon data type corresponding to Cassandra type
     * exemple UTF8Type => \Phalcon\Db\Column::TYPE_CHAR
     *
     * @param $typestr
     * @return mixed
     */
    public function getPhalconTypeFor($typestr)
    {
        if (strpos($typestr, 'set') !== false) {
            return $this->_phalcon_type_map['set'];
        } elseif (strpos($typestr, 'list') !== false) {
            return $this->_phalcon_type_map['list'];
        } elseif (strpos($typestr, 'map') !== false) {
            return $this->_phalcon_type_map['map'];
        }

        return $this->_phalcon_type_map[$typestr];
    }


    /**
     * Return de correct Phalcon bind type corresponding to Cassandra type
     * exemple UTF8Type => \Phalcon\Db\Column::BIND_PARAM_STR
     *
     * @param $typestr
     * @return mixed
     */
    public function getPhalconBindFor($typestr)
    {
        if (strpos($typestr, 'set') !== false) {
            return $this->_phalcon_bind_map['set'];
        } elseif (strpos($typestr, 'list') !== false) {
            return $this->_phalcon_bind_map['list'];
        } elseif (strpos($typestr, 'map') !== false) {
            return $this->_phalcon_bind_map['map'];
        }
        return $this->_phalcon_bind_map[$typestr];
    }


    /**
     * Return de correct Cassandra type corresponding to Phalcon data type
     *
     * @param $typeint
     * @return mixed
     */
    public function getCassandraTypeFor($typeint)
    {
        return $this->_cassandra_type_map[$typeint];
    }

    /**
     * @param $value
     * @return float|int|string
     */
    public static function unpack($value)
    {
        if (is_object($value)) {
            if ($value instanceof \Cassandra\Bigint) {
                return $value->toInt();
            }
            if ($value instanceof \Cassandra\Timestamp) {
                if ($value->time() < 34369738) {
                    return null;
                }
                return $value->toDateTime();
            }
            if ($value instanceof \Cassandra\Varint) {
                return $value->toInt();
            }
            if ($value instanceof \Cassandra\Float) {
                return $value->toDouble();
            }
            if ($value instanceof \Cassandra\Decimal) {
                return $value->toDouble();
            }
            if ($value instanceof \Cassandra\Blob) {
                return $value->bytes();
            }
            if ($value instanceof \Cassandra\Inet) {
                return $value->address();
            }
        }
        return $value;
    }

    /**
     * @param $type
     * @param $value
     * @return \Cassandra\Bigint
     */
    public static function pack($type, $value)
    {
        if ($value === null) {
            return null;
        }
        if ($type === self::TYPE_BIGINT) {
            if ($value instanceof \Cassandra\Bigint) {
                return $value;
            }
            return new \Cassandra\Bigint((int) $value);
        }
        if ($type === self::TYPE_DATETIME) {
            if ($value instanceof \DateTime) {
                return new \Cassandra\Timestamp($value->getTimestamp());
            }
            if ($value instanceof \Cassandra\Timestamp) {
                return $value;
            }
            if($value !== ''){
                $timeZone = new \DateTimeZone('UTC');
                $date = \DateTime::createFromFormat('Y-m-d H:i:s', $value, $timeZone);
                return new \Cassandra\Timestamp($date->getTimestamp());
            }
        }
        if ($type === self::TYPE_VARINT) {
            if ($value instanceof \Cassandra\Varint) {
                return $value;
            }
            return new \Cassandra\Varint((int) $value);
        }
        if ($type === self::TYPE_FLOAT) {
            return new \Cassandra\Float((float) $value);

        }
        if ($type === self::TYPE_DECIMAL) {
            return new \Cassandra\Decimal((float) $value);
        }
        if ($type === self::TYPE_BLOB) {
            return new \Cassandra\Blob($value);
        }
        if ($type === self::TYPE_INET) {
            return new \Cassandra\Inet($value);
        }
        if ($type === self::TYPE_BOOLEAN) {
            return (bool) $value;
        }
        return $value;
    }
}
