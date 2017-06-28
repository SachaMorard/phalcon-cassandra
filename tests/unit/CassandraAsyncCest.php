<?php

require dirname(__DIR__) . '/_bootstrap.php';


class CassandraAsyncCest
{
    public function connection(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();

        $I->assertTrue($cassandra instanceof \Phalcon\Db\Adapter\Cassandra, 'Correct instance of Cassandra Adapter');
        $I->assertTrue($cassandra->getClient() instanceof \Cassandra\Session, 'Correct instance of Cassandra Client');
        $I->assertTrue($cassandra->getInternalHandler() instanceof \Cassandra\Session, 'Correct instance of Cassandra Adapter');
        $I->assertTrue($cassandra->getDialect() instanceof \Phalcon\Db\Dialect\Cassandra, 'Correct instance of Cassandra Dialect');
        $I->assertEquals('cassandra', $cassandra->getDialectType(), 'Correct Cassandra Dialect Type');
        $I->assertEquals('cassandra', $cassandra->getType(), 'Correct Cassandra Type');
        $I->assertFalse($cassandra->supportSequences(), 'Cassandra does\'t support sequences');
        $I->assertFalse($cassandra->isNestedTransactionsWithSavepoints(), 'Correct Cassandra Dialect Type');
        $I->assertTrue(is_array($cassandra->getDescriptor()));
        $I->assertTrue($cassandra->useExplicitIdValue());
        $I->assertContains('Cassandra', $cassandra->getDescriptor());

        /** @var \Phalcon\Config $config */
        $config = $I->getConfig();
        $I->assertEquals($config->cassandra->keyspace, $cassandra->getCurrentKeyspace(), 'Correct Cassandra Dialect Type');

        $client = $cassandra->connect();
        $I->assertTrue($client instanceof \Cassandra\Session);

        $I->assertEquals(0, $cassandra->getConnectionId());

        $I->assertEquals(0, $cassandra->getTransactionLevel());
        $I->assertFalse($cassandra->isUnderTransaction());


        $cassandra->begin();
        $I->assertEquals(1, $cassandra->getTransactionLevel());
        $I->assertTrue($cassandra->isUnderTransaction());
        $cassandra->commit();

        $cassandra->close();

        try {
            $cassandra->listTables();
        } catch (Exception $e) {
            $I->assertTrue(true, 'Connection close properly');
        }

        $cassandra->connect($config->cassandra->toArray());
        $tables = $cassandra->listTables();
        $I->assertTrue(is_array($tables));

        $cassandra->setDialect(new \Phalcon\Db\Dialect\MySQL());
        $I->assertTrue($cassandra->getDialect() instanceof \Phalcon\Db\Dialect\MySQL);
        $cassandra->setDialect(new \Phalcon\Db\Dialect\Cassandra());

        $eventManager = new \Phalcon\Events\Manager();
        $eventManager->name = 'toto';
        $cassandra->setEventsManager($eventManager);
        $I->assertEquals('toto', $cassandra->getEventsManager()->name);

        $cassandra->close();
    }
    public function createTable(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();
        $return = $cassandra->nextWillBeAsync()->execute('CREATE TABLE IF NOT EXISTS test (id bigint, fname varchar, lname varchar, PRIMARY KEY (id, fname))');
        $I->assertEquals(true, $return);

        /** @var \Phalcon\Config $config */
        $config = $I->getConfig();
        $column1 = new \Phalcon\Db\Column('id', array('primary' => true, 'type' => \Phalcon\Cassandra\DataType::TYPE_BIGINT));
        $column2 = new \Phalcon\Db\Column('bool', array('type' => \Phalcon\Cassandra\DataType::TYPE_BOOLEAN));
        $return = $cassandra->nextWillBeAsync()->createTable('test2', $config->cassandra->keyspace, array('columns' => array($column1, $column2)));
        $I->assertEquals(true, $return);
        sleep(1);
    }

    public function tableExists(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();
        sleep(3);
        $I->assertEquals(true, $cassandra->nextWillBeAsync()->tableExists('test'));
        $I->assertEquals(true, $cassandra->nextWillBeAsync()->tableExists('test2'));
        $I->assertEquals(true, $cassandra->nextWillBeAsync()->tableExists('peers', 'system'));
        $I->assertEquals(false, $cassandra->nextWillBeAsync()->tableExists('ffhdjkuyfsgdzy'));
        $I->assertEquals(false, $cassandra->nextWillBeAsync()->tableExists('test', 'klfdsmlofjdfs'));
    }

    public function tableManipulation(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();

        //1
        try {
            $cassandra->nextWillBeAsync()->query("SELECT * FROM test where lname = 'bonjour'");
        } catch (\Cassandra\Exception $e) {
            $I->assertTrue(true, 'Can\'t add where clause on not indexed column');
        }

        //2
        $index = new \Phalcon\Db\Index('test_lname_idx', array(new \Phalcon\Db\Column('lname', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT))));
        $cassandra->nextWillBeAsync()->addIndex('test', null, $index);
        sleep(3);
        $I->assertFalse($cassandra->nextWillBeAsync()->fetchOne("SELECT * FROM test where lname = 'bonjour'"));

        //3
        $columns = $cassandra->nextWillBeAsync()->describeColumns('test');
        $I->assertTrue(is_array($columns));
        //4
        $I->assertTrue($columns[0] instanceof \Phalcon\Db\Column);
        //5
        $I->assertContains('fname', $columns[1]->getName());
        //6
        $I->assertEquals('varchar', $cassandra->nextWillBeAsync()->getColumnDefinition($columns[1]));
        //7
        $indexes = $cassandra->nextWillBeAsync()->describeIndexes('test');
        $I->assertTrue(is_array($indexes));
        //8
        $I->assertTrue($indexes['PRIMARY'] instanceof \Phalcon\Db\Index);
        //9
        $I->assertTrue($indexes['test_lname_idx'] instanceof \Phalcon\Db\Index);
        //10
        $I->assertContains('test_lname_idx', $indexes['test_lname_idx']->getName());

        //11
        /** @var \Phalcon\Config $config */
        $config = $I->getConfig();

        //13
        $newcolumn = new \Phalcon\Db\Column('aaa', array('type' => \Phalcon\Cassandra\DataType::TYPE_UUID));
        $cassandra->nextWillBeAsync()->addColumn('test', null, $newcolumn);
        sleep(1);
        $columns = $cassandra->nextWillBeAsync()->describeColumns('test');
        $I->assertEquals('aaa', $columns[2]->getName());
        //14
        $I->assertEquals(\Phalcon\Cassandra\DataType::TYPE_UUID, $columns[2]->getType());

        //15
        $newcolumn = new \Phalcon\Db\Column('fname', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT));
        $cassandra->nextWillBeAsync()->modifyColumn('test', null, $newcolumn);
        time_nanosleep(0, 100000000);
        $columns = $cassandra->nextWillBeAsync()->describeColumns('test');
        $I->assertEquals('fname', $columns[1]->getName());
        //16
        $I->assertEquals(\Phalcon\Cassandra\DataType::TYPE_TEXT, $columns[1]->getType());

        //17
        $cassandra->nextWillBeAsync()->dropColumn('test', null, 'aaa');
        sleep(1);
        $columns = $cassandra->nextWillBeAsync()->describeColumns('test');
        $I->assertNotEquals('aaa', $columns[2]->getName());

        //18
        $cassandra->nextWillBeAsync()->dropIndex('test', null, 'test_lname_idx');
        sleep(1);
        $indexes = $cassandra->nextWillBeAsync()->describeIndexes('test');
        $I->assertTrue(is_array($indexes));
        //19
        $I->assertTrue($indexes['PRIMARY'] instanceof \Phalcon\Db\Index);
        //20
        $I->assertFalse(isset($indexes['test_lname_idx']));

        //21
        $result = $cassandra->nextWillBeAsync()->execute("CREATE TABLE IF NOT EXISTS typetabletest ("
            . "bigint bigint, "
            . "boolean boolean, "
            . "decimal decimal, "
            . "double double, "
            . "float float, "
            . "int int, "
            . "list list<text>, "
            . "map map<int,text>, "
            . "aset set<text>, "
            . "text text, "
            . "timestamp timestamp, "
            . "uuid uuid, "
            . "timeuuid timeuuid, "
            . "varchar varchar, "
            . "varint varint, "
            . "id bigint, "
            . "PRIMARY KEY (id)"
            . ")");
        $I->assertTrue($result);

        //22
        $result = $cassandra->nextWillBeAsync()->execute("CREATE TABLE IF NOT EXISTS totaldeclickstest (shortlink varchar, counter counter, PRIMARY KEY (shortlink))");
        $I->assertTrue($result);

        //23
        $result = $cassandra->nextWillBeAsync()->execute("INSERT INTO typetabletest (bigint,boolean,decimal,double,float,int,list,map,aset,text,timestamp,uuid,timeuuid,varchar,varint,id) "
            . "VALUES ("
            . "1,"
            . "true,"
            . "12.5,"
            . "7E-10,"
            . "12.55,"
            . "12,"
            . "['ok', 'ok'],"
            . "{ 1:'toto', 2:'tata' },"
            . "{'f@baggins.com', 'baggins@gmail.com'},"
            . "'ok léclate',"
            . "'2012-01-01 00:00:00',"
            . "58321180-9b22-11e4-9c2e-f3b5fa9fa832,"
            . "58321b00-9b22-11e4-bf7f-7f7f7f7f7f7f,"
            . "'ok léclate',"
            . "1236,"
            . "6)");
        $I->assertTrue($result);

        //24
        $result = $cassandra->nextWillBeAsync()->execute("UPDATE totaldeclickstest SET counter = counter + 1 WHERE shortlink='lkjf6354d'");
        $I->assertTrue($result);
        sleep(1);

        //25
        $uuid = new \Cassandra\Uuid();
        $timeuuid = new \Cassandra\Timeuuid();
        $date = new \Cassandra\Timestamp();
        $bigint = new \Cassandra\Bigint(3003241563200265401);
        $decimal = new \Cassandra\Decimal(1254568.123);
        $float = new \Cassandra\Float(1254568.123546);
        $varint = new \Cassandra\Varint(1254568);
        $id = new \Cassandra\Bigint(20);
        $map = new \Cassandra\Map(\Cassandra::TYPE_INT, \Cassandra::TYPE_TEXT);
        $map->set(1, 'rivendell');
        $map->set(2, 'maximus');
        $map->set(3, 'spacial');
        $map->set(4, 'twitter');
        $set = new \Cassandra\Set(\Cassandra::TYPE_TEXT);
        $set->add('f@baggins.com');
        $set->add('baggins@gmail.com');
        $set->add('sacha@morard.com');
        $set->add('sachoo@toto.com');
        $list = new \Cassandra\Collection(\Cassandra\Type::text());
        $list->add('rivendell');
        $list->add('rohan');
        $list->add('jesus');
        $result = $cassandra->nextWillBeAsync()->execute("INSERT INTO typetabletest ("
            . "bigint,"
            . "boolean,"
            . "decimal,"
            . "float,"
            . "int,"
            . "list,"
            . "map,"
            . "aset,"
            . "text,"
            . "timestamp,"
            . "uuid,"
            . "timeuuid,"
            . "varchar,"
            . "varint,"
            . "id"
            . ") VALUES ("
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?,"
            . "?)",
            array(
                $bigint,
                false,
                $decimal,
                $float,
                4000000,
                $list,
                $map,
                $set,
                "ok l'éclate totalze",
                $date,
                $uuid,
                $timeuuid,
                "ok l'éclat dfsf e",
                $varint,
                $id
            ));
        $I->assertTrue($result);

        //26
        $result = $cassandra->nextWillBeAsync()->execute('UPDATE totaldeclickstest SET counter = counter + 1 WHERE shortlink= ?', array('lkjf6354d'));
        $I->assertTrue($result);
        sleep(1);
    }

    public function listTables(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();
        $list = $cassandra->nextWillBeAsync()->listTables();
        $I->assertNotEmpty($list);
        $I->assertContains('test', $list);
        $I->assertContains('test2', $list);
        sleep(1);
    }

    public function methods(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();

        $cassandra->nextWillBeAsync()->begin();
        $transactions = $cassandra->nextWillBeAsync()->addTransaction("INSERT INTO test (id, fname, lname) VALUES (?,?,?)", [new \Cassandra\Bigint(1), 'sacha', 'morard']);
        //1
        $I->assertTrue($transactions);
        //2
        $cassandra->nextWillBeAsync()->commit();

        sleep(1);
        //3
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test');
        $I->assertEquals('sacha', $result['fname']);
        //4
        $I->assertTrue($cassandra->nextWillBeAsync()->execute("INSERT INTO test (id, fname, lname) VALUES (?,?,?)", [new \Cassandra\Bigint(2),'toto', 'tata']));

        $result = $cassandra->nextWillBeAsync()->query('SELECT * FROM test');
        //5
        $I->assertTrue($result instanceof \Phalcon\Db\Result\Cassandra);
        $fetchArray = $result->fetchArray();
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        //6
        $I->assertTrue($fetchAll[0] instanceof stdClass);
        //7
        $I->assertTrue($fetch instanceof stdClass);

        $result->getInternalResult();
        $result->dataSeek(2);
        $result->execute();
        sleep(1);
        $result = $cassandra->nextWillBeAsync()->query("SELECT * FROM test");
        $result->setFetchMode(\PDO::FETCH_ASSOC);
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        //8
        $I->assertEquals(3, count($fetchAll[0]));
        //9
        $I->assertEquals(3, count($fetch));

        $result = $cassandra->nextWillBeAsync()->query("SELECT * FROM test");
        $result->setFetchMode(\PDO::FETCH_BOTH);
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        //10
        $I->assertEquals(6, count($fetchAll[0]));
        //11
        $I->assertEquals(6, count($fetch));

        $result = $cassandra->nextWillBeAsync()->query("SELECT * FROM test");
        $result->setFetchMode(\PDO::FETCH_NUM);
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        //12
        $I->assertEquals(3, count($fetchAll[0]));
        //13
        $I->assertEquals(3, count($fetch));

        $result = $cassandra->nextWillBeAsync()->query("SELECT * FROM test");
        $result->setFetchMode(\PDO::FETCH_OBJ);
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        //14
        $I->assertTrue(is_object($fetchAll[0]));
        //15
        $I->assertTrue(is_object($fetch));


        //16
        $I->assertTrue($cassandra->nextWillBeAsync()->execute("INSERT INTO test (id, fname, lname) VALUES (?, ?, ?)", array(new \Cassandra\Bigint(3), 'toto', 'morard')));

        //17
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 3');
        $I->assertEquals('toto', $result['fname']);


        //18
        $I->assertTrue($cassandra->nextWillBeAsync()->execute("INSERT INTO test (id, fname, lname) VALUES (:id, :fname, :lname)", array('fname' => 'bonjour', 'id' => new \Cassandra\Bigint(4),'lname' => 'morard')));

        //19
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 4');
        $I->assertEquals('bonjour', $result['fname']);

        //20
        $stm = $cassandra->nextWillBeAsync()->prepare("INSERT INTO test (id, fname, lname) VALUES (?, ?, ?)");
        $I->assertTrue($stm instanceof \Cassandra\PreparedStatement);

        //21
        $result = $cassandra->nextWillBeAsync()->executePrepared($stm, array(new \Cassandra\Bigint(5), 'toto', 'morard'));
        $I->assertTrue($result);

        //22
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 5');
        $I->assertEquals('toto', $result['fname']);

        //23
        $stm = $cassandra->nextWillBeAsync()->prepare("INSERT INTO test (id, fname, lname) VALUES (:id, :fname, :lname)");
        $I->assertTrue($stm instanceof \Cassandra\PreparedStatement);

        //24
        $result = $cassandra->nextWillBeAsync()->executePrepared($stm, array('fname' => 'bilou', 'id' => new \Cassandra\Bigint(6),'lname' => 'morard'));
        $I->assertTrue($result);

        //25
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 6');
        $I->assertEquals('bilou', $result['fname']);

        //26
        $result = $cassandra->nextWillBeAsync()->convertBoundParams("INSERT INTO test (id, fname, lname) VALUES (?0, ?1, ?2)", array(5, 'toto', 'morard'));
        $I->assertEquals('INSERT INTO test (id, fname, lname) VALUES (?, ?, ?)', $result['cql']);
        //27
        $I->assertEquals(5, $result['params'][0]);
        //28
        $I->assertEquals('toto', $result['params'][1]);

        //29
        $result = $cassandra->nextWillBeAsync()->convertBoundParams("INSERT INTO test (id, fname, lname) VALUES (:id:, :fname:, :lname:)", array('fname' => 'tata','lname' => 'morard', 'id' => 44));
        $I->assertEquals('INSERT INTO test (id, fname, lname) VALUES (?, ?, ?)', $result['cql']);
        //30
        $I->assertEquals(44, $result['params'][0]);
        //31
        $I->assertEquals('tata', $result['params'][1]);

        //32
        $result = $cassandra->nextWillBeAsync()->limit("SELECT column_name FROM system.schema_columns", 5);
        $I->assertEquals('SELECT column_name FROM system.schema_columns LIMIT 5', $result);

        //33
        $I->assertNull($cassandra->nextWillBeAsync()->getCQLBindTypes());

        //34
        $I->assertNull($cassandra->nextWillBeAsync()->getCqlVariables());

        //35
        $I->assertEquals("'some dan''gerous value'", $cassandra->nextWillBeAsync()->escapeString("some dan'gerous value"));

        //36
        $I->assertEquals('"bonjour"', $cassandra->nextWillBeAsync()->escapeIdentifier("bonjour"));

        //37
        $I->assertEquals("'bonjour'", $cassandra->nextWillBeAsync()->getColumnList(array('bonjour')));

        //38
        $I->assertTrue($cassandra->nextWillBeAsync()->delete('test', 'id=1'));
        //39
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertFalse($result);

        $cassandra->nextWillBeAsync()->delete('test', 'id=1');

        //40
        $I->assertTrue($cassandra->nextWillBeAsync()->insert('test', array(new \Cassandra\Bigint(1), 'roubin', 'momo'), array('id', 'fname', 'lname')));
        //41
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('roubin', $result['fname']);

        $cassandra->nextWillBeAsync()->delete('test', 'id=1');

        //42
        $I->assertTrue($cassandra->nextWillBeAsync()->insertAsDict('test', array('id' => new \Cassandra\Bigint(1),  'fname' => 'sacha',  'lname' => 'morard')));
        //43
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('sacha', $result['fname']);

        //44
        $I->assertTrue($cassandra->nextWillBeAsync()->update('test', array('lname'), array('kjlmklmk'), array('conditions' => "id = ? AND fname = ?", 'bind' => array(new \Cassandra\Bigint(1), 'sacha'))));
        //45
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('kjlmklmk', $result['lname']);

        //46
        $I->assertTrue($cassandra->nextWillBeAsync()->updateAsDict('test', array('lname' => 'monsieur'), "id = 1 AND fname = 'sacha'"));
        //47
        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('monsieur', $result['lname']);
        sleep(1);
    }

    public function results(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();

        $result = $cassandra->nextWillBeAsync()->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('sacha', $result['fname']);
    }

    public function dropTable(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();
        $return = $cassandra->execute('DROP TABLE IF EXISTS test');

        $return = $cassandra->dropTable('test2', null, true);

        $return = $cassandra->tableExists('test');

        $return = $cassandra->tableExists('test2');

        $return = $cassandra->dropTable('totaldeclickstest', null, true);

        $return = $cassandra->dropTable('typetabletest', null, true);
    }
}
























