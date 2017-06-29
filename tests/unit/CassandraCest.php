<?php

require dirname(__DIR__) . '/_bootstrap.php';


class CassandraCest
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
        $return = $cassandra->execute('CREATE TABLE IF NOT EXISTS test (id bigint, fname varchar, lname varchar, PRIMARY KEY (id, fname))');
        $I->assertEquals(true, $return);

        /** @var \Phalcon\Config $config */
        $config = $I->getConfig();
        $column1 = new \Phalcon\Db\Column('id', array('primary' => true, 'type' => \Phalcon\Cassandra\DataType::TYPE_BIGINT));
        $column2 = new \Phalcon\Db\Column('bool', array('type' => \Phalcon\Cassandra\DataType::TYPE_BOOLEAN));
        $return = $cassandra->createTable('test2', $config->cassandra->keyspace, array('columns' => array($column1, $column2)));
        $I->assertEquals(true, $return);
        sleep(1);

    }

    public function tableExists(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();
        $I->assertEquals(true, $cassandra->tableExists('test'));
        $I->assertEquals(true, $cassandra->tableExists('test2'));
        $I->assertEquals(true, $cassandra->tableExists('peers', 'system'));
        $I->assertEquals(false, $cassandra->tableExists('ffhdjkuyfsgdzy'));
        $I->assertEquals(false, $cassandra->tableExists('test', 'klfdsmlofjdfs'));
    }

    public function tableManipulation(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();

        try {
            $cassandra->query("SELECT * FROM test where lname = 'bonjour'");
        } catch (\Cassandra\Exception $e) {
            $I->assertTrue(true, 'Can\'t add where clause on not indexed column');
        }

        $index = new \Phalcon\Db\Index('test_lname_idx', array(new \Phalcon\Db\Column('lname', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT))));
        $cassandra->addIndex('test', null, $index);
        sleep(1);
        $I->assertFalse($cassandra->fetchOne("SELECT * FROM test where lname = 'bonjour'"));

        $columns = $cassandra->describeColumns('test');
        $I->assertTrue(is_array($columns));
        
        $I->assertTrue($columns[0] instanceof \Phalcon\Db\Column);
        
        $I->assertContains('fname', $columns[1]->getName());
        
        $I->assertEquals('varchar', $cassandra->getColumnDefinition($columns[1]));
        
        $indexes = $cassandra->describeIndexes('test');
        $I->assertTrue(is_array($indexes));
        
        $I->assertTrue($indexes['PRIMARY'] instanceof \Phalcon\Db\Index);
        
        $I->assertTrue($indexes['test_lname_idx'] instanceof \Phalcon\Db\Index);
        
        $I->assertContains('test_lname_idx', $indexes['test_lname_idx']->getName());

        
        /** @var \Phalcon\Config $config */
        $config = $I->getConfig();

        
        $newcolumn = new \Phalcon\Db\Column('aaa', array('type' => \Phalcon\Cassandra\DataType::TYPE_UUID));
        $cassandra->addColumn('test', null, $newcolumn);

        sleep(1);
        $columns = $cassandra->describeColumns('test');
        $I->assertEquals('aaa', $columns[2]->getName());
        
        $I->assertEquals(\Phalcon\Cassandra\DataType::TYPE_UUID, $columns[2]->getType());


        
        $cassandra->dropColumn('test', null, 'aaa');

        sleep(1);
        $columns = $cassandra->describeColumns('test');
        $I->assertNotEquals('aaa', $columns[2]->getName());

        
        $cassandra->dropIndex('test', null, 'test_lname_idx');

        sleep(1);
        $indexes = $cassandra->describeIndexes('test');
        $I->assertTrue(is_array($indexes));
        
        $I->assertTrue($indexes['PRIMARY'] instanceof \Phalcon\Db\Index);
        
        $I->assertFalse(isset($indexes['test_lname_idx']));

        
        $result = $cassandra->execute("CREATE TABLE IF NOT EXISTS typetabletest ("
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

        
        $result = $cassandra->execute("CREATE TABLE IF NOT EXISTS totaldeclickstest (shortlink varchar, counter counter, PRIMARY KEY (shortlink))");
        $I->assertTrue($result);

        
        $result = $cassandra->execute("INSERT INTO typetabletest (bigint,boolean,decimal,double,float,int,list,map,aset,text,timestamp,uuid,timeuuid,varchar,varint,id) "
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

        
        $result = $cassandra->execute("UPDATE totaldeclickstest SET counter = counter + 1 WHERE shortlink='lkjf6354d'");
        $I->assertTrue($result);


        
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
        $result = $cassandra->execute("INSERT INTO typetabletest ("
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

        
        $result = $cassandra->execute('UPDATE totaldeclickstest SET counter = counter + 1 WHERE shortlink= ?', array('lkjf6354d'));
        $I->assertTrue($result);
        sleep(1);
    }

    public function listTables(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();
        $list = $cassandra->listTables();
        $I->assertNotEmpty($list);
        $I->assertContains('test', $list);
        $I->assertContains('test2', $list);

    }

    public function methods(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();

        $cassandra->begin();
        $transactions = $cassandra->addTransaction("INSERT INTO test (id, fname, lname) VALUES (?,?,?)", [new \Cassandra\Bigint(1), 'sacha', 'morard']);
        
        $I->assertTrue($transactions);
        
        $cassandra->commit();

        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test');
        $I->assertEquals('sacha', $result['fname']);
        
        $I->assertTrue($cassandra->execute("INSERT INTO test (id, fname, lname) VALUES (?,?,?)", [new \Cassandra\Bigint(2),'toto', 'tata']));
        sleep(1);
        $result = $cassandra->query('SELECT * FROM test');
        
        $I->assertTrue($result instanceof \Phalcon\Db\Result\Cassandra);
        $fetchArray = $result->fetchArray();
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        
        $I->assertTrue($fetchAll[0] instanceof stdClass);
        
        $I->assertTrue($fetch instanceof stdClass);

        $result->getInternalResult();
        $result->dataSeek(2);
        $result->execute();

        $result = $cassandra->query("SELECT * FROM test");
        $result->setFetchMode(\PDO::FETCH_ASSOC);
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        
        $I->assertEquals(3, count($fetchAll[0]));
        
        $I->assertEquals(3, count($fetch));

        $result = $cassandra->query("SELECT * FROM test");
        $result->setFetchMode(\PDO::FETCH_BOTH);
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        
        $I->assertEquals(6, count($fetchAll[0]));
        
        $I->assertEquals(6, count($fetch));

        $result = $cassandra->query("SELECT * FROM test");
        $result->setFetchMode(\PDO::FETCH_NUM);
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        
        $I->assertEquals(3, count($fetchAll[0]));
        
        $I->assertEquals(3, count($fetch));

        $result = $cassandra->query("SELECT * FROM test");
        $result->setFetchMode(\PDO::FETCH_OBJ);
        $fetchAll = $result->fetchAll();
        $fetch = $result->fetch();
        
        $I->assertTrue(is_object($fetchAll[0]));
        
        $I->assertTrue(is_object($fetch));

        $I->assertTrue($cassandra->execute("INSERT INTO test (id, fname, lname) VALUES (?, ?, ?)", array(new \Cassandra\Bigint(3), 'toto', 'morard')));

        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 3');
        $I->assertEquals('toto', $result['fname']);

        $I->assertTrue($cassandra->execute("INSERT INTO test (id, fname, lname) VALUES (:id, :fname, :lname)", array('fname' => 'bonjour', 'id' => new \Cassandra\Bigint(4),'lname' => 'morard')));

        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 4');
        $I->assertEquals('bonjour', $result['fname']);

        $stm = $cassandra->prepare("INSERT INTO test (id, fname, lname) VALUES (?, ?, ?)");
        $I->assertTrue($stm instanceof \Cassandra\PreparedStatement);

        $result = $cassandra->executePrepared($stm, array(new \Cassandra\Bigint(5), 'toto', 'morard'));
        $I->assertTrue($result);

        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 5');
        $I->assertEquals('toto', $result['fname']);

        $stm = $cassandra->prepare("INSERT INTO test (id, fname, lname) VALUES (:id, :fname, :lname)");
        $I->assertTrue($stm instanceof \Cassandra\PreparedStatement);

        $result = $cassandra->executePrepared($stm, array('fname' => 'bilou', 'id' => new \Cassandra\Bigint(6),'lname' => 'morard'));
        $I->assertTrue($result);

        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 6');
        $I->assertEquals('bilou', $result['fname']);

        $result = $cassandra->convertBoundParams("INSERT INTO test (id, fname, lname) VALUES (?0, ?1, ?2)", array(5, 'toto', 'morard'));
        $I->assertEquals('INSERT INTO test (id, fname, lname) VALUES (?, ?, ?)', $result['cql']);
        
        $I->assertEquals(5, $result['params'][0]);
        
        $I->assertEquals('toto', $result['params'][1]);

        $result = $cassandra->convertBoundParams("INSERT INTO test (id, fname, lname) VALUES (:id:, :fname:, :lname:)", array('fname' => 'tata','lname' => 'morard', 'id' => 44));
        $I->assertEquals('INSERT INTO test (id, fname, lname) VALUES (?, ?, ?)', $result['cql']);
        
        $I->assertEquals(44, $result['params'][0]);
        
        $I->assertEquals('tata', $result['params'][1]);

        sleep(1);
        $result = $cassandra->limit("SELECT column_name FROM system.schema_columns", 5);
        $I->assertEquals('SELECT column_name FROM system.schema_columns LIMIT 5', $result);

        $I->assertNull($cassandra->getCQLBindTypes());

        $I->assertNull($cassandra->getCqlVariables());

        $I->assertEquals("'some dan''gerous value'", $cassandra->escapeString("some dan'gerous value"));

        $I->assertEquals('"bonjour"', $cassandra->escapeIdentifier("bonjour"));

        $I->assertEquals("'bonjour'", $cassandra->getColumnList(array('bonjour')));

        $I->assertTrue($cassandra->delete('test', 'id=1'));
        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertFalse($result);

        $cassandra->delete('test', 'id=1');

        $I->assertTrue($cassandra->insert('test', array(new \Cassandra\Bigint(1), 'roubin', 'momo'), array('id', 'fname', 'lname')));
        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('roubin', $result['fname']);

        $cassandra->delete('test', 'id=1');

        $I->assertTrue($cassandra->insertAsDict('test', array('id' => new \Cassandra\Bigint(1),  'fname' => 'sacha',  'lname' => 'morard')));
        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('sacha', $result['fname']);

        $I->assertTrue($cassandra->update('test', array('lname'), array('kjlmklmk'), array('conditions' => "id = ? AND fname = ?", 'bind' => array(new \Cassandra\Bigint(1), 'sacha'))));
        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('kjlmklmk', $result['lname']);

        $I->assertTrue($cassandra->updateAsDict('test', array('lname' => 'monsieur'), "id = 1 AND fname = 'sacha'"));
        sleep(1);
        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('monsieur', $result['lname']);

    }

    public function results(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();

        $result = $cassandra->fetchOne('SELECT * FROM test WHERE id = 1');
        $I->assertEquals('sacha', $result['fname']);
    }

    public function dialect(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();
        /** @var \Phalcon\Db\Dialect\Cassandra $dialect */
        $dialect = $cassandra->getDialect();

        $column1 = new \Phalcon\Db\Column('id', array('primary' => true, 'type' => \Phalcon\Cassandra\DataType::TYPE_BIGINT));
        $column2 = new \Phalcon\Db\Column('bool', array('type' => \Phalcon\Cassandra\DataType::TYPE_BOOLEAN));
        $I->assertTrue($dialect instanceof \Phalcon\Db\Dialect\Cassandra);

        $I->assertEquals('ALTER TABLE test ADD bool boolean', $dialect->addColumn('test', null, $column2));

        $index = new \Phalcon\Db\Index('test_lname_idx', array(new \Phalcon\Db\Column('lname', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT))));
        $I->assertEquals('CREATE INDEX IF NOT EXISTS test_lname_idx ON test (lname)', $dialect->addIndex('test', null, $index));

        /** @var \Phalcon\Config $config */
        
        $config = $I->getConfig();
        $column1 = new \Phalcon\Db\Column('id', array('primary' => true, 'type' => \Phalcon\Cassandra\DataType::TYPE_BIGINT));
        $column2 = new \Phalcon\Db\Column('bool', array('type' => \Phalcon\Cassandra\DataType::TYPE_BOOLEAN));
        $return = $dialect->createTable('test2', $config->cassandra->keyspace, array('columns' => array($column1, $column2)));
        $I->assertEquals('CREATE TABLE IF NOT EXISTS testphalcon.test2 (id bigint, bool boolean, PRIMARY KEY (id))', $return);

        $return = $dialect->dropColumn('test2', null, 'toto');
        $I->assertEquals("ALTER TABLE test2 DROP toto", $return);

        $return = $dialect->dropIndex('test2', null, 'toto');
        $I->assertEquals("DROP INDEX toto", $return);

        $return = $dialect->dropTable('test2');
        $I->assertEquals("DROP TABLE test2", $return);

        $return = $dialect->getColumnDefinition(new \Phalcon\Db\Column('name', array('type' => \Phalcon\Cassandra\DataType::TYPE_BOOLEAN)));
        $I->assertEquals('boolean', $return);

        $return = $dialect->getColumnDefinition(new \Phalcon\Db\Column('name', array('type' => \Phalcon\Cassandra\DataType::TYPE_DATETIME)));
        $I->assertEquals('timestamp', $return);

        $return = $dialect->getColumnList(array('test2'));
        $I->assertEquals("'test2'", $return);

        $result = $dialect->limit("SELECT column_name FROM system.schema_columns", 5);
        $I->assertEquals('SELECT column_name FROM system.schema_columns LIMIT 5', $result);

        $newcolumn = new \Phalcon\Db\Column('fname', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT));
        $return = $dialect->modifyColumn('test', null, $newcolumn);
        $I->assertEquals("ALTER TABLE test ALTER fname TYPE varchar", $return);

        $return = $dialect->supportsSavepoints();
        $I->assertEquals(false, $return);

        $I->assertEquals('ALTER TABLE testphalcon.test ADD bool boolean', $dialect->addColumn('test', 'testphalcon', $column2));

        $newcolumn = new \Phalcon\Db\Column('fname', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT));
        $return = $dialect->modifyColumn('test', 'testphalcon', $newcolumn);
        $I->assertEquals("ALTER TABLE testphalcon.test ALTER fname TYPE varchar", $return);

        $return = $dialect->dropColumn('test2', 'testphalcon', 'toto');
        $I->assertEquals("ALTER TABLE testphalcon.test2 DROP toto", $return);

        $index = new \Phalcon\Db\Index('test_lname_idx', array(new \Phalcon\Db\Column('lname', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT))));
        $I->assertEquals('CREATE INDEX IF NOT EXISTS test_lname_idx ON testphalcon.test (lname)', $dialect->addIndex('test', 'testphalcon', $index));

        $return = $dialect->dropIndex('test2', 'testphalcon', 'toto');
        $I->assertEquals("DROP INDEX testphalcon.toto", $return);

        $config = $I->getConfig();
        $column1 = new \Phalcon\Db\Column('id', array('primary' => true, 'type' => \Phalcon\Cassandra\DataType::TYPE_BIGINT));
        $column2 = new \Phalcon\Db\Column('bool', array('type' => \Phalcon\Cassandra\DataType::TYPE_BOOLEAN));
        $return = $dialect->createTable('test2', null, array('columns' => array($column1, $column2)));
        $I->assertEquals('CREATE TABLE IF NOT EXISTS test2 (id bigint, bool boolean, PRIMARY KEY (id))', $return);

        try{
            $dialect->createTable('test2', null, array($column1, $column2));
        }catch (Exception $e){
            $I->assertTrue(true, $e->getMessage());
        }

        try{
            $index = new \Phalcon\Db\Index('test_lname_idx', array(new \Phalcon\Db\Column('lname', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT)), new \Phalcon\Db\Column('toto', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT))));
            $dialect->addIndex('test', 'testphalcon', $index);
        }catch (Exception $e){
            $I->assertTrue(true, $e->getMessage());
        }

        $return = $dialect->dropTable('test2', 'toto');
        $I->assertEquals("DROP TABLE toto.test2", $return);

    }

    public function dropTable(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();
        $return = $cassandra->execute('DROP TABLE IF EXISTS test');
        $I->assertEquals(true, $return);

        $return = $cassandra->dropTable('test2', null, true);
        $I->assertEquals(true, $return);

        $return = $cassandra->tableExists('test');
        $I->assertEquals(false, $return);

        $return = $cassandra->tableExists('test2');
        $I->assertEquals(false, $return);

        $return = $cassandra->dropTable('totaldeclickstest', null, true);
        $I->assertEquals(true, $return);

        $return = $cassandra->dropTable('typetabletest', null, true);
        $I->assertEquals(true, $return);
    }

    public function unableMethods(\UnitTester $I)
    {
        /** @var \Phalcon\Db\Adapter\Cassandra $cassandra */
        $cassandra = $I->getCassandra();
        /** @var \Phalcon\Db\Dialect\Cassandra $dialect */
        $dialect = $cassandra->getDialect();

        try {
            $cassandra->affectedRows();
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $reference = new \Phalcon\Db\Reference("field_fk", array(
                'referencedSchema' => "invoicing",
                'referencedTable' => "products",
                'columns' => array("product_type", "product_code"),
                'referencedColumns' => array("type", "code")
            ));
            $cassandra->addForeignKey('tablename', 'schema', $reference);
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->createSavepoint('name');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->createView('view', array());
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->describeReferences('test');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->dropView('view');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->dropForeignKey('key', null, 'refname');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->dropPrimaryKey('primary', null);
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->forUpdate('QUERY');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->getRealSQLStatement();
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->lastInsertId();
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }


        try {
            $cassandra->rollback();
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->releaseSavepoint('name');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->rollbackSavepoint('name');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->viewExists('name');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->getDefaultIdValue();
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->getNestedTransactionSavepointName();
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->tableOptions('test');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $cassandra->setNestedTransactionsWithSavepoints(true);
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }


        /**
         * Dialect
         */

        try {
            $reference = new \Phalcon\Db\Reference("field_fk", array(
                'referencedSchema' => "invoicing",
                'referencedTable' => "products",
                'columns' => array("product_type", "product_code"),
                'referencedColumns' => array("type", "code")
            ));
            $dialect->addForeignKey('tablename', 'schema', $reference);
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $index = new \Phalcon\Db\Index('test_lname_idx', array(new \Phalcon\Db\Column('lname', array('type' => \Phalcon\Cassandra\DataType::TYPE_TEXT))));
            $dialect->addPrimaryKey('tablename', 'schema', $index);
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->createSavepoint('name');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->describeReferences('test');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->dropForeignKey('key', null, 'refname');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->dropPrimaryKey('primary', null);
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->forUpdate('QUERY');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->releaseSavepoint('name');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->rollbackSavepoint('name');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->sharedLock('query');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->tableOptions('name');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->select(array());
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->createView('view', array());
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->dropView('view');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

        try {
            $dialect->viewExists('name');
        } catch (Exception $e) {
            $I->assertTrue(true, $e->getMessage());
        }

    }
}
























